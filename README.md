# pgsync

pgsync is a Go service that synchronizes selected PostgreSQL records into Meilisearch.

## How it works

- Reads a JSON schema describing source tables, Meilisearch destinations, selected columns, and relations
- Installs PostgreSQL triggers for INSERT, UPDATE, and DELETE events
- Publishes change payloads through LISTEN/NOTIFY
- Splits large PostgreSQL notifications into chunks and reassembles them safely
- Batches and processes change sets with bounded worker concurrency
- Hydrates configured relations before updating search documents
- Supports an initial or scheduled synchronization path

The included schema.json demonstrates brands, categories, products, and nested product-category relations.

## Stack

- Go
- PostgreSQL with pgx
- Meilisearch
- robfig/cron
- Docker and Docker Compose

## Configuration

Set these environment variables:

- PG_CONNECTION_STRING
- MEILISEARCH_HOST
- MEILISEARCH_KEY
- SCHEMA_PATH

## Local development

    go mod download
    go run .

Or build and run the container:

    docker build -t pgsync .
    docker run --env-file .env pgsync

## Status

This is a working synchronization prototype. Production deployment should add durable retry/dead-letter handling, observability, schema migrations, and operational recovery procedures.
