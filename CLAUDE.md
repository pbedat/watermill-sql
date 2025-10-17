# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

This is the SQL (PostgreSQL/MySQL) Pub/Sub implementation for [Watermill](https://watermill.io/), a Go library for working with message streams. It provides SQL-based publishers and subscribers that use database tables to store and consume messages.

**Key documentation:**
- Main Watermill docs: https://watermill.io/
- Getting started: https://watermill.io/docs/getting-started/
- All Pub/Sub implementations: https://watermill.io/pubsubs/

## Development Commands

### Running Tests

```bash
# Start required databases (MySQL & PostgreSQL)
make up

# Run all tests (requires databases running, 10-minute timeout)
make test

# Quick tests (excludes long-running tests)
make test_short

# Run tests with race detection
make test_race

# Verbose test output
make test_v

# Reconnection tests (requires specific tag)
make test_reconnect

# Wait for databases to be ready
make wait
```

### Building and Formatting

```bash
# Build all packages
make build

# Format code (requires goimports)
make fmt

# Update Watermill dependency
make update_watermill
```

### Database Access

```bash
# Connect to MySQL CLI
make mycli

# Connect to PostgreSQL CLI
make pgcli
```

## Architecture

### Core Components

The architecture follows an adapter pattern to support different databases and schemas:

1. **Publisher** ([pkg/sql/publisher.go](pkg/sql/publisher.go))
   - Inserts messages as rows into SQL tables
   - Supports both `*sql.DB` and `*sql.Tx` as database handles
   - Can auto-initialize schema per topic
   - Thread-safe with graceful shutdown via `Close()`

2. **Subscriber** ([pkg/sql/subscriber.go](pkg/sql/subscriber.go))
   - Polls database tables for new messages using SELECT queries
   - Supports consumer groups for load distribution
   - Handles message acking/nacking with configurable deadlines
   - Uses transactions with configurable isolation levels
   - Implements backoff strategies for errors and empty results

3. **SchemaAdapter** ([pkg/sql/schema_adapter.go](pkg/sql/schema_adapter.go))
   - Defines database schema and generates SQL queries
   - Implementations:
     - `DefaultPostgreSQLSchema` ([pkg/sql/schema_adapter_postgresql.go](pkg/sql/schema_adapter_postgresql.go))
     - `DefaultMySQLSchema` ([pkg/sql/schema_adapter_mysql.go](pkg/sql/schema_adapter_mysql.go))
     - `DefaultPostgreSQLQueueSchema` ([pkg/sql/queue_schema_adapter_postgresql.go](pkg/sql/queue_schema_adapter_postgresql.go)) - For queue-like semantics
   - Key methods: `InsertQuery()`, `SelectQuery()`, `UnmarshalMessage()`, `SchemaInitializingQueries()`

4. **OffsetsAdapter** ([pkg/sql/offsets_adapter.go](pkg/sql/offsets_adapter.go))
   - Manages message offsets and consumer acknowledgments
   - Implementations:
     - `DefaultPostgreSQLOffsetsAdapter` ([pkg/sql/offsets_adapter_postgresql.go](pkg/sql/offsets_adapter_postgresql.go))
     - `DefaultMySQLOffsetsAdapter` ([pkg/sql/offsets_adapter_mysql.go](pkg/sql/offsets_adapter_mysql.go))
     - `DefaultPostgreSQLQueueOffsetsAdapter` ([pkg/sql/queue_offsets_adapter_postgresql.go](pkg/sql/queue_offsets_adapter_postgresql.go))
   - Key methods: `AckMessageQuery()`, `NextOffsetQuery()`, `ConsumedMessageQuery()`

5. **Database Adapters**
   - `BeginnerFromStdSQL()` - Wraps standard library `database/sql` ([pkg/sql/adapters_std_sql.go](pkg/sql/adapters_std_sql.go))
   - `BeginnerFromPgx()` - Wraps pgx native driver ([pkg/sql/adapters_pgx.go](pkg/sql/adapters_pgx.go))
   - These provide a unified interface (`Beginner`, `Tx`, `ContextExecutor`) for both standard library and pgx drivers

### Additional Features

- **BackoffManager** ([pkg/sql/backoff_manager.go](pkg/sql/backoff_manager.go)) - Configurable backoff strategy for polling and error handling, with special handling for deadlocks
- **DelayedRequeuer** ([pkg/sql/delayed_requeuer.go](pkg/sql/delayed_requeuer.go)) - Requeue messages with delays using PostgreSQL's delayed publishing

### Message Flow

**Publishing:**
1. User calls `Publisher.Publish(topic, messages...)`
2. Publisher optionally initializes schema if `AutoInitializeSchema` is enabled
3. SchemaAdapter generates INSERT query with message data (UUID, payload, metadata)
4. Query is executed on the database

**Subscribing:**
1. User calls `Subscriber.Subscribe(ctx, topic)` which returns a channel
2. Subscriber runs BeforeSubscribingQueries (e.g., consumer group setup)
3. Background goroutine polls database in a loop:
   - Begin transaction with configured isolation level
   - SchemaAdapter generates SELECT query for next unread messages
   - OffsetsAdapter determines which offset to start from
   - Messages are unmarshaled and sent to output channel
   - Wait for ack/nack with optional deadline
   - On ack: OffsetsAdapter generates ACK query to mark messages as consumed
   - On nack: message is resent after `ResendInterval`
   - BackoffManager determines sleep time based on errors or empty results
   - Commit or rollback transaction

### Database Schema

The schema adapters create tables per topic:
- **Messages table**: Stores message data (offset, UUID, payload, metadata, timestamps)
- **Offsets table**: Tracks consumer group progress and locks

## Testing

Tests are located in [pkg/sql/pubsub_test.go](pkg/sql/pubsub_test.go). Key patterns:

- Tests run against real MySQL and PostgreSQL databases (started via `make up`)
- Environment variables `WATERMILL_TEST_MYSQL_HOST` and `WATERMILL_TEST_POSTGRES_HOST` can override database hosts
- Tests verify Watermill's standard pub/sub contract using the `tests` package
- Both standard library (`database/sql`) and pgx drivers are tested

### Running a Single Test

```bash
# Run specific test
go test -v -run TestPublishSubscribe_MySQL ./pkg/sql/

# Run with short flag to skip long-running tests
go test -short -v -run TestPublishSubscribe_PostgreSQL ./pkg/sql/

# Run specific test with timeout
go test -timeout=10m -v -run TestExactlyOnceDelivery ./pkg/sql/
```

## Important Implementation Notes

- **Transaction Handling**: Publishers can work with both connection pools (`*sql.DB`) and transactions (`*sql.Tx`). Using `AutoInitializeSchema` with transactions is forbidden to avoid implicit commits.
- **Isolation Levels**: Subscribers use isolation levels defined by `SchemaAdapter.SubscribeIsolationLevel()` - typically `ReadCommitted` for PostgreSQL and `RepeatableRead` for MySQL.
- **Ack Deadline**: When set to 0, ack deadline is disabled, but this may block PostgreSQL subscribers from reading new messages due to snapshot behavior.
- **Batch Processing**: `SubscribeBatchSize` controls how many messages are queried at once. Higher values improve performance but increase redelivery risk on crashes.
- **Consumer Groups**: Multiple subscribers with the same `ConsumerGroup` share message consumption (load distribution).
- **Pgx vs Standard Library**: The pgx adapter provides better performance but requires pgx-specific connection objects. Standard library adapter works with any `database/sql` compatible driver.

## Module Information

- **Module**: `github.com/ThreeDotsLabs/watermill-sql/v4`
- **Go version**: 1.23.0+
- **Main dependencies**:
  - `github.com/ThreeDotsLabs/watermill` - Core Watermill library
  - `github.com/jackc/pgx/v5` - PostgreSQL driver
  - `github.com/go-sql-driver/mysql` - MySQL driver
  - `github.com/lib/pq` - Alternative PostgreSQL driver (standard library)
