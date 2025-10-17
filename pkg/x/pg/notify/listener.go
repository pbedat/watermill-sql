package notify

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/ThreeDotsLabs/watermill"
	"github.com/jackc/pgx/v5"
)

// PostgreSQLListener manages PostgreSQL LISTEN/NOTIFY and bridges to NotifyChannel
type PostgreSQLListener struct {
	connStr                string
	conn                   *pgx.Conn
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
	ConnStr                string
	NotificationErrTimeout time.Duration
}

func NewPostgreSQLListener(config PostgreSQLListenerConfig, notifyChannel chan struct{}, logger watermill.LoggerAdapter) *PostgreSQLListener {
	if config.NotificationErrTimeout <= 0 {
		config.NotificationErrTimeout = 1 * time.Second
	}

	ctx, cancel := context.WithCancel(context.Background())

	return &PostgreSQLListener{
		connStr:                config.ConnStr,
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

	// Connect to PostgreSQL using pgx
	conn, err := pgx.Connect(l.ctx, l.connStr)
	if err != nil {
		return fmt.Errorf("failed to connect to PostgreSQL: %w", err)
	}
	l.conn = conn

	// Start listening on the channel
	_, err = conn.Exec(l.ctx, fmt.Sprintf("LISTEN %s", pgx.Identifier{channel}.Sanitize()))
	if err != nil {
		conn.Close(l.ctx)
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
		// Wait for notification or context cancellation
		notification, err := l.conn.WaitForNotification(l.ctx)

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

	if l.conn != nil {
		return l.conn.Close(context.Background())
	}
	return nil
}
