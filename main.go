package main

import (
	"context"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"log"
	"math"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/meilisearch/meilisearch-go"
	"github.com/robfig/cron/v3"
)

// Constants
const PG_CONNECTION_STRING = "postgresql://postgres:postgres@192.168.2.123:5432/test"

// UPDATE "Products" SET "updatedAt" = now() WHERE id IN (SELECT id FROM "Products" LIMIT 10000);

const MEILISEARCH_HOST = "http://localhost:7700"

const MEILISEARCH_KEY = "masterKey"

const WATCH_CHANNEL = "pgsync_watchers"

const MESSAGE_SEPARATOR = "__:)__"

const MESSAGE_LENGTH_LIMIT = "2000"

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
	// Create Context
	Ctx := context.Background()

	// Parse schema.json
	TablesColumnsMap := map[string][]string{}
	TablesDestinationsMap := map[string]string{}
	TablesRelations := make(map[string][]map[string]interface{})
	jsonSchemaData, err := os.ReadFile("schema.json")
	if err != nil {
		log.Println("Error reading schema.json")
		panic(err)
	}
	var schema []SchemaObj
	err = json.Unmarshal(jsonSchemaData, &schema)
	if err != nil {
		log.Println("Error parsing schema.json")
		panic(err)
	}
	log.Println("schema.json parsed")
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

	// Schedule the cron job to run every 10 minutes
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

			pgConn2, err := pgPool.Acquire(Ctx)
			if err != nil {
				log.Println("Failed to acquire connection from pool in cron job")
				return
			}
			defer pgConn2.Release()
			FinalData := make(map[string]map[string][]interface{})
			count := 1
			for p, d := range data {
				fmt.Println(p, len(data), count)
				count++
				columns := TablesColumnsMap[d.Table]
				if columns == nil {
					continue
				}
				if len(columns) == 0 {
					continue
				}
				if _, ok := FinalData[d.Table]; !ok {
					FinalData[d.Table] = make(map[string][]interface{})
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

						if _, ok := FinalData[d.Table]["insert"]; !ok {
							FinalData[d.Table]["insert"] = make([]interface{}, 0)
						}
						FinalData[d.Table]["insert"] = append(FinalData[d.Table]["insert"], toInsert)
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

						if _, ok := FinalData[d.Table]["update"]; !ok {
							FinalData[d.Table]["update"] = make([]interface{}, 0)
						}
						FinalData[d.Table]["update"] = append(FinalData[d.Table]["update"], toUpdate)
					}
				case "delete":
					{
						if _, ok := FinalData[d.Table]["delete"]; !ok {
							FinalData[d.Table]["delete"] = make([]interface{}, 0)
						}
						FinalData[d.Table]["delete"] = append(FinalData[d.Table]["delete"], d.ID)
					}
				default:
					{
						log.Println("Unknown action:", d.Action)
					}
				}
			}

			for table, actionsMap := range FinalData {
				var toInsert []interface{}
				var toUpdate []interface{}
				var toDelete = make([]string, 0)

				for actionType, actions := range actionsMap {
					switch actionType {
					case "insert":
						toInsert = actions
					case "update":
						toUpdate = actions
					case "delete":
						for _, someDoc := range actions {
							docID, ok := someDoc.(string)
							if !ok {
								log.Println("failed typeassert")
								continue
							}
							toDelete = append(toDelete, docID)
						}
					default:
						log.Println("Unknown action type:", actionType)
					}
				}

				if len(toInsert) > 0 {
					var batchSize = 1000
					var totalBatches = int(math.Ceil(float64(len(toInsert)) / float64(batchSize)))
					for batchNumber := 0; batchNumber < totalBatches; batchNumber++ {
						start := batchNumber * batchSize
						end := start + batchSize
						if end > len(toInsert) {
							end = len(toInsert)
						}
						batch := toInsert[start:end]
						resp, err := meilisearchClient.Index(table).AddDocuments(batch, "id")
						if err != nil {
							log.Println("Error inserting in meilisearch")
							log.Println(err)
						} else {
							log.Println("inserted in meilisearch:", resp)
						}
					}
				}

				if len(toUpdate) > 0 {
					var batchSize = 1000
					var totalBatches = int(math.Ceil(float64(len(toUpdate)) / float64(batchSize)))
					for batchNumber := 0; batchNumber < totalBatches; batchNumber++ {
						start := batchNumber * batchSize
						end := start + batchSize
						if end > len(toUpdate) {
							end = len(toUpdate)
						}
						batch := toUpdate[start:end]
						resp, err := meilisearchClient.Index(table).AddDocuments(batch, "id")
						if err != nil {
							log.Println("Error updating in meilisearch")
							log.Println(err)
						} else {
							log.Println("updated in meilisearch:", resp)
						}
					}
				}
				if len(toDelete) > 0 {
					var batchSize = 1000
					var totalBatches = int(math.Ceil(float64(len(toDelete)) / float64(batchSize)))
					for batchNumber := 0; batchNumber < totalBatches; batchNumber++ {
						start := batchNumber * batchSize
						end := start + batchSize
						if end > len(toDelete) {
							end = len(toDelete)
						}
						batch := toDelete[start:end]
						resp, err := meilisearchClient.Index(table).DeleteDocuments(batch)
						if err != nil {
							log.Println("Error deleting in meilisearch")
							log.Println(err)
						} else {
							log.Println("deleted in meilisearch:", resp)
						}
					}
				}
			}

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
