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
	subscribers map[string][]*topicSubscriber
	subMu       sync.RWMutex
}

type topicSubscriber struct {
	ch     chan struct{}
	ctx    context.Context
	cancel context.CancelFunc
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
		subscribers:            make(map[string][]*topicSubscriber),
	}

	// Start the single listener
	if err := l.start(); err != nil {
		cancel()
		return nil, err
	}

	return l, nil
}

// Register subscribes to notifications for a specific topic.
// Returns a receive-only channel that will be notified when new messages arrive for that topic.
// The channel will be automatically cleaned up when the context is cancelled.
func (l *PostgreSQLListener) Register(ctx context.Context, topic string) <-chan struct{} {
	l.subMu.Lock()
	defer l.subMu.Unlock()

	ch := make(chan struct{}, 1)
	subCtx, cancel := context.WithCancel(ctx)

	sub := &topicSubscriber{
		ch:     ch,
		ctx:    subCtx,
		cancel: cancel,
	}

	l.subscribers[topic] = append(l.subscribers[topic], sub)

	// Start a goroutine to clean up when context is cancelled
	go func() {
		<-subCtx.Done()
		l.unregister(topic, sub)
	}()

	return ch
}

func (l *PostgreSQLListener) unregister(topic string, sub *topicSubscriber) {
	l.subMu.Lock()
	defer l.subMu.Unlock()

	subs := l.subscribers[topic]
	l.subscribers[topic] = lo.Without(subs, sub)

	// Clean up empty topic entries
	if len(l.subscribers[topic]) == 0 {
		delete(l.subscribers, topic)
	}

	close(sub.ch)
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

			l.subMu.RLock()
			subscribers := l.subscribers[topic]
			l.subMu.RUnlock()

			for _, sub := range subscribers {
				// Non-blocking send to avoid deadlocks
				select {
				case sub.ch <- struct{}{}:
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

	// Close all subscriber channels by cancelling their contexts
	l.subMu.Lock()
	for topic, subs := range l.subscribers {
		for _, sub := range subs {
			sub.cancel()
		}
		delete(l.subscribers, topic)
	}
	l.subMu.Unlock()

	return nil
}
