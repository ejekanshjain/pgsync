package main

import (
	"context"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/joho/godotenv"
	"github.com/meilisearch/meilisearch-go"
	"github.com/robfig/cron/v3"
)

var (
	PG_CONNECTION_STRING string
	MEILISEARCH_HOST     string
	MEILISEARCH_KEY      string
	SCHEMA_PATH          string
)

// Constants
// const PG_CONNECTION_STRING = "postgresql://postgres:postgres@192.168.2.123:5432/test"

// UPDATE "Products" SET "updatedAt" = now() WHERE id IN (SELECT id FROM "Products" LIMIT 10000);

// const MEILISEARCH_HOST = "http://localhost:7700"

// const MEILISEARCH_KEY = "masterKey"

const WATCH_CHANNEL = "pgsync_watchers"

const MESSAGE_SEPARATOR = "__:)__"

const MESSAGE_LENGTH_LIMIT = "2000"

const BATCH_SIZE = 1000

const CREATE_TRIGGER_FUNCTION_QUERY = `CREATE OR REPLACE FUNCTION pgsync_notify_trigger() RETURNS trigger AS $$
DECLARE
	payload JSONB;
  notification_text TEXT;
	page_count INT;
	hash TEXT;
BEGIN
  IF TG_OP = 'INSERT' THEN
      payload = JSONB_BUILD_OBJECT(
        'table', TG_TABLE_NAME,
				'action', 'insert',
				'new_values', NEW,
        'id', NEW.id
      );
  END IF;

  IF TG_OP = 'UPDATE' THEN
      payload = JSONB_BUILD_OBJECT(
        'table', TG_TABLE_NAME,
				'action', 'update',
				'new_values', NEW,
        'id', NEW.id
      );
  END IF;

  IF TG_OP = 'DELETE' THEN
      payload = JSONB_BUILD_OBJECT(
        'table', TG_TABLE_NAME,
				'action', 'delete',
        'id', OLD.id
      );
  END IF;

  notification_text = payload::text;
	page_count = (CHAR_LENGTH(notification_text) / ` + MESSAGE_LENGTH_LIMIT + `) + 1;
	hash = MD5(notification_text);

	FOR cur_page IN 1..page_count LOOP
    PERFORM PG_NOTIFY('` + WATCH_CHANNEL + `',
      hash || '` + MESSAGE_SEPARATOR + `' || cur_page || '` + MESSAGE_SEPARATOR + `' || page_count || '` + MESSAGE_SEPARATOR + `' ||
      SUBSTR(notification_text, ((cur_page - 1) * ` + MESSAGE_LENGTH_LIMIT + `) + 1, ` + MESSAGE_LENGTH_LIMIT + `)
    );
  END LOOP;

  RETURN NULL;
END;
$$ LANGUAGE plpgsql;`

type SchemaObj = struct {
	Table       string                   `json:"table"`
	Destination string                   `json:"destination"`
	Columns     []string                 `json:"columns"`
	Relations   []map[string]interface{} `json:"relations"`
}

type MessageHashData = struct {
	CurrPage  int
	PageCount int
	Msg       string
}

type PayloadData = struct {
	Table     string         `json:"table"`
	Action    string         `json:"action"`
	ID        string         `json:"id"`
	NewValues map[string]any `json:"new_values"`
}

type ChangeSetData = struct {
	ID        string
	Table     string
	Action    string
	NewValues map[string]any
}

func main() {

	//env
	if err := godotenv.Load(); err != nil {
		log.Fatal("Error loading .env file")
	}
	PG_CONNECTION_STRING = os.Getenv("PG_CONNECTION_STRING")
	MEILISEARCH_HOST = os.Getenv("MEILISEARCH_HOST")
	MEILISEARCH_KEY = os.Getenv("MEILISEARCH_KEY")
	SCHEMA_PATH = os.Getenv("SCHEMA_PATH")

	// Create Context
	Ctx := context.Background()

	// Parse SCHEMA_PATH
	TablesColumnsMap := map[string][]string{}
	TablesDestinationsMap := map[string]string{}
	TablesRelations := make(map[string][]map[string]interface{})
	jsonSchemaData, err := os.ReadFile(SCHEMA_PATH)
	if err != nil {
		log.Println("Error reading SCHEMA_PATH")
		panic(err)
	}
	var schema []SchemaObj
	err = json.Unmarshal(jsonSchemaData, &schema)
	if err != nil {
		log.Println("Error parsing SCHEMA_PATH")
		panic(err)
	}
	log.Println("SCHEMA_PATH parsed")
	for _, s := range schema {
		if TablesColumnsMap[s.Table] != nil {
			log.Println("Duplicate table:", s.Table)
			panic("Duplicate table")
		}
		TablesColumnsMap[s.Table] = s.Columns
		TablesDestinationsMap[s.Table] = s.Destination
		TablesRelations[s.Table] = s.Relations
	}

	// Create PG Pool & Connection
	pgPool, err := pgxpool.New(context.Background(), PG_CONNECTION_STRING)
	if err != nil {
		log.Println("Error connecting to postgres")
		panic(err)
	}
	defer pgPool.Close()
	pgConn, err := pgPool.Acquire(Ctx)
	if err != nil {
		log.Println("Failed to acquire connection from pool")
		panic(err)
	}
	defer pgConn.Release()
	log.Println("Connected to postgres")

	// Setup Triggers & Watchers
	_, err = pgConn.Exec(Ctx, CREATE_TRIGGER_FUNCTION_QUERY)
	if err != nil {
		log.Println("Failed to run create trigger function query")
		panic(err)
	}
	for _, s := range schema {
		_, err = pgConn.Exec(Ctx, getSetupTriggerOnTableQuery(s.Table))
		if err != nil {
			log.Println("Failed to setup trigger on table:", s.Table)
			panic(err)
		}
	}
	log.Println("Setting up triggers and watchers done")

	// Create Meilisearch Client
	meilisearchConfig := meilisearch.ClientConfig{
		Host:   MEILISEARCH_HOST,
		APIKey: MEILISEARCH_KEY,
	}
	meilisearchClient := meilisearch.NewClient(meilisearchConfig)

	// Listen to PG Channel
	_, err = pgConn.Exec(Ctx, "LISTEN "+WATCH_CHANNEL)
	if err != nil {
		log.Println("Failed to Listen in channel:", WATCH_CHANNEL)
		panic(err)
	}
	log.Println("Listening \"" + WATCH_CHANNEL + "\" channel")

	MessageHashMap := make(map[string][]MessageHashData)
	ChangeSet := make(map[string]map[string]ChangeSetData)
	NotificationChannel := make(chan struct{})

	go func() {

		for {
			select {
			case <-NotificationChannel:
				{
					// Stop the goroutine when the notificationChannel is closed
					return
				}
			default:
				{
					notification, err := pgConn.Conn().WaitForNotification(Ctx)
					if err != nil {
						log.Println(err)
						continue
					}

					if notification.Channel != WATCH_CHANNEL {
						continue
					}

					splitPayload := strings.Split(notification.Payload, MESSAGE_SEPARATOR)
					md5Hash := splitPayload[0]
					currPage, err := strconv.Atoi((splitPayload[1]))
					if err != nil {
						log.Println("Error parsing currPage in notification payload:", splitPayload[1])
						continue
					}
					pageCount, err := strconv.Atoi(splitPayload[2])
					if err != nil {
						log.Println("Error parsing pageCount in notification payload:", splitPayload[2])
						continue
					}
					msgBody := splitPayload[3]

					if MessageHashMap[md5Hash] == nil {
						MessageHashMap[md5Hash] = make([]MessageHashData, pageCount)
					}
					MessageHashMap[md5Hash][currPage-1] = MessageHashData{
						CurrPage:  currPage,
						PageCount: pageCount,
						Msg:       msgBody,
					}

					allPayloadsReceived := true
					fullPayload := ""
					for _, p := range MessageHashMap[md5Hash] {
						if p.Msg == "" {
							allPayloadsReceived = false
							break
						}
						fullPayload += p.Msg
					}

					if !allPayloadsReceived {
						continue
					}

					delete(MessageHashMap, md5Hash)

					// log.Println("payload:", fullPayload)

					var payload PayloadData
					err = json.Unmarshal([]byte(fullPayload), &payload)
					if err != nil {
						log.Println("Error parsing payload:", fullPayload)
						continue
					}

					if TablesColumnsMap[payload.Table] == nil {
						log.Println("Unknown table:", payload.Table)
						continue
					}

					// log.Println(payload)

					timestamp := getTimestamp()
					key := "pgsync:" + timestamp
					key2 := payload.Table + ":" + payload.ID
					// log.Println(key, key2)

					ref := ChangeSet[key]
					if ref == nil {
						ChangeSet[key] = make(map[string]ChangeSetData)
					}

					switch payload.Action {
					case "insert":
						{
							// log.Println("insert")
							ChangeSet[key][key2] = ChangeSetData{
								ID:        payload.ID,
								Table:     payload.Table,
								Action:    payload.Action,
								NewValues: payload.NewValues,
							}
						}
					case "update":
						{
							// log.Println("update")
							ref := ChangeSet[key][key2]
							if ref.Action == "insert" {
								ChangeSet[key][key2] = ChangeSetData{
									ID:        payload.ID,
									Table:     payload.Table,
									Action:    ref.Action,
									NewValues: payload.NewValues,
								}
								// log.Println("update insert", payload.NewValues)
							} else {
								ChangeSet[key][key2] = ChangeSetData{
									ID:        payload.ID,
									Table:     payload.Table,
									Action:    payload.Action,
									NewValues: payload.NewValues,
								}
							}
						}
					case "delete":
						{
							// log.Println("delete")
							ref := ChangeSet[key][key2]
							if ref.Action == "insert" {
								delete(ChangeSet[key], key2)
							} else {
								ChangeSet[key][key2] = ChangeSetData{
									ID:     payload.ID,
									Table:  payload.Table,
									Action: payload.Action,
								}
							}
						}
					default:
						{
							log.Println("Unknown action:", payload.Action)
						}
					}

					// log.Println("Data synchronized!")

					// jsonBytes, _ := json.MarshalIndent(ChangeSet, "", "  ")
					// _ = os.WriteFile("ChangeSet.json", jsonBytes, 0644)
				}
			}
		}
	}()

	// Create a new cron job runner
	cj := cron.New()

	// Schedule the cron job to run at schedule
	_, err = cj.AddFunc("* * * * *", func() {
		timestamp := time.Now().UTC().Truncate(time.Minute).Format(time.RFC3339)
		key := "pgsync:" + timestamp
		log.Println("Running cron job at", key)
		go func() {
			StartTime := time.Now()
			data := ChangeSet[key]
			if data == nil {
				return
			}
			if len(data) == 0 {
				delete(ChangeSet, key)
				return
			}

			delete(ChangeSet, key)

			log.Println("Processing data", len(data))

			allBatches := make([]map[string]ChangeSetData, 0)
			batchData := make(map[string]ChangeSetData)
			tempCount := 0
			for p, d := range data {
				if tempCount > BATCH_SIZE-1 {
					allBatches = append(allBatches, batchData)
					batchData = make(map[string]ChangeSetData)
					tempCount = 0
				}
				batchData[p] = d
				tempCount++
			}
			if len(batchData) > 0 {
				allBatches = append(allBatches, batchData)
			}
			var wg sync.WaitGroup
			for _, b := range allBatches {
				wg.Add(1)
				go func(batch2 map[string]ChangeSetData) {
					defer wg.Done()
					pgConn2, err := pgPool.Acquire(Ctx)
					if err != nil {
						log.Println("Failed to acquire connection from pool in cron job")
						return
					}
					defer pgConn2.Release()

					FinalDataToInsert := make(map[string][]map[string]any)
					FinalDataToUpdate := make(map[string][]map[string]any)
					FinalDataToDelete := make(map[string][]string)

					for _, d := range batch2 {
						columns := TablesColumnsMap[d.Table]
						if columns == nil {
							continue
						}
						if len(columns) == 0 {
							continue
						}

						switch d.Action {
						case "insert":
							{
								toInsert := make(map[string]any)
								toInsert["id"] = d.ID
								for _, c := range columns {
									if c == "id" {
										continue
									}
									toInsert[c] = d.NewValues[c]
								}
								r := TablesRelations[d.Table]
								processRelationsRecursively(Ctx, r, toInsert, pgConn2)
								FinalDataToInsert[d.Table] = append(FinalDataToInsert[d.Table], toInsert)
							}
						case "update":
							{
								toUpdate := make(map[string]any)
								toUpdate["id"] = d.ID
								for _, c := range columns {
									if c == "id" {
										continue
									}
									toUpdate[c] = d.NewValues[c]
								}
								r := TablesRelations[d.Table]
								processRelationsRecursively(Ctx, r, toUpdate, pgConn2)
								FinalDataToUpdate[d.Table] = append(FinalDataToUpdate[d.Table], toUpdate)
							}
						case "delete":
							{
								FinalDataToDelete[d.Table] = append(FinalDataToDelete[d.Table], d.ID)
							}
						default:
							{
								log.Println("Unknown action:", d.Action)
							}
						}
					}

					for table, data := range FinalDataToInsert {
						resp, err := meilisearchClient.Index(table).AddDocuments(data, "id")
						if err != nil {
							log.Println(err)
						} else {
							log.Println("inserted in meiliesearch", resp)
						}
					}
					for table, data := range FinalDataToUpdate {
						resp, err := meilisearchClient.Index(table).UpdateDocuments(data, "id")
						if err != nil {
							log.Println(err)
						} else {
							log.Println("Updated in meiliesearch", resp)
						}
					}
					for table, ids := range FinalDataToDelete {
						resp, err := meilisearchClient.Index(table).DeleteDocuments(ids)
						if err != nil {
							log.Println(err)
						} else {
							log.Println("deleted in meiliesearch", resp)
						}
					}

				}(b)
			}
			wg.Wait()

			log.Println("Data processed in", time.Since(StartTime))
		}()
	})
	if err != nil {
		log.Println("Error scheduling cron job")
		panic(err)
	}

	// Start the cron scheduler
	cj.Start()

	// Let the goroutine and cron run forever.
	select {}
}

func processRelationsRecursively(Ctx context.Context, r []map[string]interface{}, toInsert map[string]any, pgConn *pgxpool.Conn) {
	for _, s := range r {
		if s == nil {
			continue
		}

		rColumns := s["columns"].([]interface{})
		rTable := s["table"].(string)
		rRelation := s["relation"].(string)
		rRelationKey := s["relationKey"].(string)
		rRelations, ok := s["relations"].([]interface{})
		var rRelations2 []map[string]interface{}

		if ok {
			for _, rel := range rRelations {
				rRelations2 = make([]map[string]interface{}, len(rRelations))
				temp, ok2 := rel.(map[string]interface{})
				if ok2 {
					rRelations2 = append(rRelations2, temp)
				}
			}
		}

		switch rRelation {
		case "one-to-one":
			if toInsert[rRelationKey] == nil {
				continue
			}
			query := "SELECT "
			for i, c := range rColumns {
				if i != 0 {
					query += ", "
				}
				rc := c.(string)
				query += "\"" + rc + "\""
			}
			query += " FROM \"" + rTable + "\" WHERE id = '" + toInsert[rRelationKey].(string) + "'"
			rows, err := pgConn.Query(Ctx, query)
			if err != nil {
				log.Println(err)
				continue
			}
			exists := rows.Next()
			if !exists {
				continue
			}

			cols := rows.FieldDescriptions()
			vals, err := rows.Values()
			rows.Close()
			if err != nil {
				log.Println(err)
				continue
			}

			result := make(map[string]interface{}, len(cols))

			for i, key := range cols {
				maybeUUID, ok := vals[i].([16]uint8)
				if ok {
					result[string(key.Name)] = parseUUID(maybeUUID)
				} else {
					result[string(key.Name)] = vals[i]
				}
			}

			if rRelations2 != nil {
				processRelationsRecursively(Ctx, rRelations2, result, pgConn)
			}

			toInsert[rTable] = result

		case "one-to-many":
			query := "SELECT"
			for i, c := range rColumns {
				if i != 0 {
					query += ", "
				}
				rc := c.(string)
				query += "\"" + rc + "\""
			}
			query += " FROM \"" + rTable + "\" WHERE \"" + rRelationKey + "\" = '" + toInsert["id"].(string) + "'"
			rows, err := pgConn.Query(Ctx, query)
			if err != nil {
				log.Println(err)
				continue
			}

			var results []map[string]any
			for rows.Next() {
				cols := rows.FieldDescriptions()
				vals, _ := rows.Values()
				result := make(map[string]any, len(cols))

				for i, key := range cols {
					maybeUUID, ok := vals[i].([16]uint8)
					if ok {
						result[string(key.Name)] = parseUUID(maybeUUID)
					} else {
						result[string(key.Name)] = vals[i]
					}
				}
				results = append(results, result)
			}
			rows.Close()

			if rRelations2 != nil {
				for _, res2 := range results {
					processRelationsRecursively(Ctx, rRelations2, res2, pgConn)
				}
			}

			toInsert[rTable] = results
		}
	}
}

func parseUUID(bytesSlice [16]uint8) string {
	byteSlice := make([]byte, 16)
	for i, b := range bytesSlice {
		byteSlice[i] = byte(b)
	}

	hexString := hex.EncodeToString(byteSlice)

	uuidString := fmt.Sprintf("%s-%s-%s-%s-%s", hexString[:8], hexString[8:12], hexString[12:16], hexString[16:20], hexString[20:])

	return uuidString
}

func getSetupTriggerOnTableQuery(table string) (query string) {
	query = `CREATE OR REPLACE TRIGGER watched_pgsync_notify_trigger AFTER INSERT OR UPDATE OR DELETE ON "` + table + `"
FOR EACH ROW EXECUTE PROCEDURE pgsync_notify_trigger();`
	return
}

func getTimestamp() (timestamp string) {
	now := time.Now().UTC().Truncate(time.Minute)
	// minutes := now.Minute()
	// mod := minutes % 2
	// toIncrement := 2 - mod

	now = now.Add(time.Duration(1) * time.Minute)

	timestamp = now.Format(time.RFC3339)
	return
}
