package notify

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/ThreeDotsLabs/watermill"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// PostgreSQLListener manages PostgreSQL LISTEN/NOTIFY and bridges to NotifyChannel
type PostgreSQLListener struct {
	conn                   *pgxpool.Pool
	notifyChannel          chan struct{}
	logger                 watermill.LoggerAdapter
	ctx                    context.Context
	cancel                 context.CancelFunc
	wg                     sync.WaitGroup
	closed                 bool
	mu                     sync.Mutex
	notificationErrTimeout time.Duration
}

type PostgreSQLListenerConfig struct {
	NotificationErrTimeout time.Duration
}

func NewPostgreSQLListener(pool *pgxpool.Pool, config PostgreSQLListenerConfig, notifyChannel chan struct{}, logger watermill.LoggerAdapter) *PostgreSQLListener {
	if config.NotificationErrTimeout <= 0 {
		config.NotificationErrTimeout = 1 * time.Second
	}

	ctx, cancel := context.WithCancel(context.Background())

	return &PostgreSQLListener{
		conn:                   pool,
		notifyChannel:          notifyChannel,
		logger:                 logger,
		notificationErrTimeout: config.NotificationErrTimeout,
		ctx:                    ctx,
		cancel:                 cancel,
	}
}

func (l *PostgreSQLListener) Start(channel string) error {
	l.mu.Lock()
	defer l.mu.Unlock()

	if l.closed {
		return fmt.Errorf("listener is closed")
	}

	// Start listening on the channel
	_, err := l.conn.Exec(l.ctx, fmt.Sprintf("LISTEN %s", pgx.Identifier{channel}.Sanitize()))
	if err != nil {
		return fmt.Errorf("failed to listen on channel %s: %w", channel, err)
	}

	l.logger.Info("Started PostgreSQL LISTEN", watermill.LogFields{
		"channel": channel,
	})

	// Start the notification forwarding goroutine
	l.wg.Add(1)
	go l.forwardNotifications()

	return nil
}

func (l *PostgreSQLListener) forwardNotifications() {
	defer l.wg.Done()

	for {
		conn, err := l.conn.Acquire(l.ctx)
		if err != nil {
			l.logger.Error("failed to acquire conn to LISTEN", err, nil)
			time.Sleep(l.notificationErrTimeout)
			continue
		}
		// Wait for notification or context cancellation
		notification, err := conn.Conn().WaitForNotification(l.ctx)

		if err != nil {
			if l.ctx.Err() != nil {
				// Context cancelled, normal shutdown
				l.logger.Debug("Stopping notification forwarder", nil)
				return
			}

			// Connection error
			l.logger.Error("Error waiting for notification", err, nil)

			time.Sleep(l.notificationErrTimeout)
			continue
		}

		if notification != nil {
			l.logger.Trace("Received PostgreSQL notification", watermill.LogFields{
				"channel": notification.Channel,
				"payload": notification.Payload,
			})

			// Send to NotifyChannel (non-blocking to avoid deadlocks)
			select {
			case l.notifyChannel <- struct{}{}:
				l.logger.Trace("Forwarded notification to NotifyChannel", nil)
			default:
				l.logger.Trace("NotifyChannel full, notification dropped (subscriber will poll anyway)", nil)
			}
		}
	}
}

func (l *PostgreSQLListener) Close() error {
	l.mu.Lock()
	if l.closed {
		l.mu.Unlock()
		return nil
	}
	l.closed = true
	l.mu.Unlock()

	l.cancel()
	l.wg.Wait()

	return nil
}
