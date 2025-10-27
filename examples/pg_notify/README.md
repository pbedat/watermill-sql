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

A listener is created using the `notify.PostgreSQLListener` to subscribe to PostgreSQL notifications:

```go
pool, err := pgxpool.New(ctx, connStr)
if err != nil {
    return err
}

pgListener, err := notify.NewListener(
    pool,
    notify.PostgreSQLListenerConfig{},
    logger,
)
if err != nil {
    return err
}
defer pgListener.Close()
```

### 3. Subscriber Configuration

The subscriber is configured with the `NotifyChannel` function, which takes a context and topic and returns a topic-specific notification channel:

```go
subscriber, err := sql.NewSubscriber(
    db,
    sql.SubscriberConfig{
        ConsumerGroup:  "my-consumer-group",
        SchemaAdapter:  schemaAdapter,
        OffsetsAdapter: offsetsAdapter,
        PollInterval:   500 * time.Millisecond, // Fallback interval
        NotifyChannel:  pgListener.Register,     // Pass the Register method
    },
    logger,
)
```

The `NotifyChannel` function will be called automatically by the subscriber with the subscription context and topic. This ensures:
- Each subscriber receives only notifications for its specific topic
- The notification channel is automatically cleaned up when the subscription context is cancelled
- No notifications are lost due to cross-topic interference

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

The implementation uses non-blocking sends to the notification channels to avoid deadlocks. If a channel is full, the notification is dropped, and the subscriber will discover the message on its next poll.

### Automatic Context-Based Cleanup

When a subscription is cancelled (via context cancellation), the associated notification channel is automatically cleaned up. This prevents resource leaks and ensures proper shutdown.

## Production Considerations

### Channel Buffer Size

The notification channels are buffered (100 by default) to handle bursts of notifications without blocking. This is configured internally in the `PostgreSQLListener.Register` method.

### Multiple Topics

The new architecture automatically handles multiple topics efficiently:

- Each subscriber gets only notifications for its specific topic
- Multiple subscribers can listen to the same topic
- Multiple subscribers can listen to different topics using the same `PostgreSQLListener`
- No cross-topic notification interference

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
