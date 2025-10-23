package notify

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/ThreeDotsLabs/watermill"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/samber/lo"
)

// PostgreSQLListener manages a single PostgreSQL LISTEN connection and distributes
// notifications to subscribers based on topic
type PostgreSQLListener struct {
	pool                   *pgxpool.Pool
	logger                 watermill.LoggerAdapter
	ctx                    context.Context
	cancel                 context.CancelFunc
	wg                     sync.WaitGroup
	closed                 bool
	mu                     sync.RWMutex
	notificationErrTimeout time.Duration

	// Map of topic -> list of notification channels
	subscribers []*ListenChan
	subMu       sync.RWMutex
}

type PostgreSQLListenerConfig struct {
	NotificationErrTimeout time.Duration
}

// NewListener returns a singleton listener instance for the given pool
func NewListener(pool *pgxpool.Pool, config PostgreSQLListenerConfig, logger watermill.LoggerAdapter) (*PostgreSQLListener, error) {

	if config.NotificationErrTimeout <= 0 {
		config.NotificationErrTimeout = 1 * time.Second
	}

	ctx, cancel := context.WithCancel(context.Background())

	l := &PostgreSQLListener{
		pool:                   pool,
		logger:                 logger,
		notificationErrTimeout: config.NotificationErrTimeout,
		ctx:                    ctx,
		cancel:                 cancel,
	}

	// Start the single listener
	if err := l.start(); err != nil {
		cancel()
		return nil, err
	}

	return l, nil
}

// Register subscribes to notifications for a specific topic
// Returns a receive-only channel that will receive the topic name when new messages arrive
func (l *PostgreSQLListener) Register() *ListenChan {
	l.subMu.Lock()
	defer l.subMu.Unlock()

	ch := make(chan string, 100)

	lc := &ListenChan{
		C: ch,
		c: ch,
	}
	lc.close = func() {
		close(ch)
		l.subMu.Lock()
		defer l.subMu.Unlock()

		l.subscribers = lo.Without(l.subscribers, lc)
	}

	l.subscribers = append(l.subscribers, lc)

	return lc
}

type ListenChan struct {
	C     <-chan string
	c     chan string
	close func()
}

func (l *ListenChan) Close() {
	l.close()
}

func (l *PostgreSQLListener) start() error {
	l.mu.Lock()
	defer l.mu.Unlock()

	if l.closed {
		return fmt.Errorf("listener is closed")
	}

	l.logger.Info("Started PostgreSQL LISTEN", watermill.LogFields{
		"channel": "watermill_messages",
	})

	// Start the notification forwarding goroutine
	l.wg.Add(1)
	go func() {
		defer l.wg.Done()

		for {
			err := l.forwardNotifications()
			if err != nil {
				if l.ctx.Err() != nil {
					// Context cancelled, normal shutdown
					l.logger.Debug("Stopping notification forwarder (context cancelled)", nil)
					return
				}
				l.logger.Error("Error in notification forwarder", err, nil)
				time.Sleep(l.notificationErrTimeout)
			} else {
				return
			}
		}
	}()

	return nil
}

func (l *PostgreSQLListener) forwardNotifications() error {
	defer l.wg.Done()

	conn, err := l.pool.Acquire(l.ctx)
	if err != nil {
		return err
	}

	defer conn.Release()

	// Start listening on the single global channel
	_, err = conn.Exec(l.ctx, "LISTEN watermill_messages")
	if err != nil {
		return fmt.Errorf("failed to listen on channel watermill_messages: %w", err)
	}

	for {

		// Wait for notification or context cancellation
		notification, err := conn.Conn().WaitForNotification(l.ctx)
		if err != nil {
			return err
		}

		if notification != nil {
			topic := notification.Payload

			l.logger.Trace("Received PostgreSQL notification", watermill.LogFields{
				"channel": notification.Channel,
				"topic":   topic,
			})

			for _, ch := range l.subscribers {
				// Non-blocking send to avoid deadlocks
				select {
				case ch.c <- topic:
					l.logger.Trace("Forwarded notification to subscriber", watermill.LogFields{
						"topic": topic,
					})
				default:
					l.logger.Trace("Subscriber channel full, notification dropped (subscriber will poll anyway)", watermill.LogFields{
						"topic": topic,
					})
				}
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

	// Close all subscriber channels
	// Copy the list first to avoid deadlock when closing channels
	l.subMu.Lock()
	subscribers := make([]*ListenChan, len(l.subscribers))
	copy(subscribers, l.subscribers)
	l.subscribers = nil
	l.subMu.Unlock()

	// Now close them without holding the lock
	for _, ch := range subscribers {
		close(ch.c)
	}

	return nil
}
