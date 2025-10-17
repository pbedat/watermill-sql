# PostgreSQL LISTEN/NOTIFY Integration Example

This example demonstrates how to integrate PostgreSQL's native LISTEN/NOTIFY feature with Watermill SQL's `NotifyChannel` to achieve near-real-time message delivery with minimal latency.

## Overview

By default, Watermill SQL uses polling to discover new messages, which introduces latency (typically equal to the `PollInterval`). This example shows how to use PostgreSQL's LISTEN/NOTIFY to wake up subscribers immediately when new messages are published.

## Performance Improvement

Based on the test results, using LISTEN/NOTIFY provides dramatic latency improvements:

- **Without NotifyChannel (pure polling)**: ~400ms latency
- **With NotifyChannel (LISTEN/NOTIFY)**: ~6ms latency
- **Improvement**: ~63x faster message delivery

## How It Works

### 1. Database Trigger

A PostgreSQL trigger is created that sends a NOTIFY when messages are inserted:

```sql
CREATE OR REPLACE FUNCTION notify_new_message() RETURNS TRIGGER AS $$
BEGIN
    NOTIFY watermill_new_messages;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER new_message_trigger
AFTER INSERT ON messages_table
FOR EACH STATEMENT
EXECUTE FUNCTION notify_new_message();
```

### 2. PostgreSQL Listener

A listener is created using `lib/pq`'s `pq.Listener` to subscribe to PostgreSQL notifications and bridge them to Watermill's `NotifyChannel`:

```go
listener := pq.NewListener(
    connStr,
    100*time.Millisecond, // minReconnectInterval
    10*time.Second,       // maxReconnectInterval
    eventCallback,
)
listener.Listen("watermill_new_messages")

// Forward notifications to NotifyChannel
for notification := range listener.Notify {
    select {
    case notifyChannel <- struct{}{}:
        // Notification sent
    default:
        // Channel full, subscriber will poll anyway
    }
}
```

### 3. Subscriber Configuration

The subscriber is configured with the `NotifyChannel`:

```go
notifyChannel := make(chan struct{}, 100)

subscriber, err := sql.NewSubscriber(
    db,
    sql.SubscriberConfig{
        ConsumerGroup:  "my-consumer-group",
        SchemaAdapter:  schemaAdapter,
        OffsetsAdapter: offsetsAdapter,
        PollInterval:   500 * time.Millisecond, // Fallback interval
        NotifyChannel:  notifyChannel,
    },
    logger,
)
```

## Running the Tests

### Prerequisites

- Docker must be installed and running (for testcontainers)
- Go 1.23.0 or later

### Run the Test

```bash
go test -v ./examples/pg_notify/
```

The test suite includes:

1. **Latency Comparison**: Measures and compares latency with and without NotifyChannel
2. **Burst Messages**: Verifies that multiple messages published rapidly are all received correctly
3. **Fallback on Listener Failure**: Demonstrates that the subscriber gracefully falls back to polling if the LISTEN connection fails

## Key Features

### Automatic Fallback

If the PostgreSQL LISTEN connection fails or is unavailable, the subscriber automatically falls back to polling mode using the configured `PollInterval`. This ensures reliable message delivery even during network issues or database maintenance.

### Reconnection Handling

The `pq.Listener` automatically handles reconnections with exponential backoff, making the solution robust in production environments.

### Non-Blocking Notifications

The implementation uses non-blocking sends to the `NotifyChannel` to avoid deadlocks. If the channel is full, the notification is dropped, and the subscriber will discover the message on its next poll.

### Notification Draining

After receiving a notification, the subscriber drains any additional notifications from the channel to avoid processing stale notifications from multiple rapid inserts.

## Production Considerations

### Channel Buffer Size

The `NotifyChannel` should be buffered to handle bursts of notifications without blocking:

```go
notifyChannel := make(chan struct{}, 100) // Adjust based on expected throughput
```

### Multiple Topics

For applications with multiple topics, you have two options:

1. **Single channel**: Use one notification channel for all topics (simpler, less granular)
2. **Per-topic channels**: Use separate channels and NOTIFY channels per topic (more granular control)

### Monitoring

Monitor the PostgreSQL listener for reconnection events and ensure your application logs include listener status information.

## Architecture Benefits

- **Low latency**: Messages are delivered in milliseconds instead of seconds
- **Reduced database load**: Fewer unnecessary SELECT queries during idle periods
- **Backward compatible**: Works with existing Watermill SQL code (NotifyChannel is optional)
- **Robust**: Automatic fallback to polling ensures reliability

## See Also

- [Watermill Documentation](https://watermill.io/)
- [PostgreSQL LISTEN/NOTIFY Documentation](https://www.postgresql.org/docs/current/sql-notify.html)
- [lib/pq Listener Documentation](https://pkg.go.dev/github.com/lib/pq#Listener)
