package notify

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/ThreeDotsLabs/watermill"
	"github.com/jackc/pgx/v5/pgxpool"
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
	subscribers map[string][]chan string
	subMu       sync.RWMutex
}

type PostgreSQLListenerConfig struct {
	NotificationErrTimeout time.Duration
}

var (
	listenerInstances = make(map[*pgxpool.Pool]*PostgreSQLListener)
	listenerMu        sync.Mutex
)

// GetOrCreateListener returns a singleton listener instance for the given pool
// This ensures only one LISTEN connection is created per database pool
func GetOrCreateListener(pool *pgxpool.Pool, config PostgreSQLListenerConfig, logger watermill.LoggerAdapter) (*PostgreSQLListener, error) {
	listenerMu.Lock()
	defer listenerMu.Unlock()

	if instance, exists := listenerInstances[pool]; exists {
		return instance, nil
	}

	if config.NotificationErrTimeout <= 0 {
		config.NotificationErrTimeout = 1 * time.Second
	}

	ctx, cancel := context.WithCancel(context.Background())

	instance := &PostgreSQLListener{
		pool:                   pool,
		logger:                 logger,
		notificationErrTimeout: config.NotificationErrTimeout,
		ctx:                    ctx,
		cancel:                 cancel,
		subscribers:            make(map[string][]chan string),
	}

	// Start the single listener
	if err := instance.start(); err != nil {
		cancel()
		return nil, err
	}

	listenerInstances[pool] = instance
	return instance, nil
}

// Register subscribes to notifications for a specific topic
// Returns a receive-only channel that will receive the topic name when new messages arrive
func (l *PostgreSQLListener) Register(topic string) <-chan string {
	l.subMu.Lock()
	defer l.subMu.Unlock()

	ch := make(chan string, 1)
	l.subscribers[topic] = append(l.subscribers[topic], ch)

	l.logger.Debug("Registered subscriber for topic", watermill.LogFields{
		"topic":             topic,
		"subscriber_count":  len(l.subscribers[topic]),
	})

	return ch
}

// Unregister removes a subscriber for a specific topic
func (l *PostgreSQLListener) Unregister(topic string, ch <-chan string) {
	l.subMu.Lock()
	defer l.subMu.Unlock()

	subscribers := l.subscribers[topic]
	for i, sub := range subscribers {
		if sub == ch {
			// Remove this subscriber
			l.subscribers[topic] = append(subscribers[:i], subscribers[i+1:]...)
			close(sub)

			l.logger.Debug("Unregistered subscriber for topic", watermill.LogFields{
				"topic":            topic,
				"subscriber_count": len(l.subscribers[topic]),
			})

			// Clean up empty topic entries
			if len(l.subscribers[topic]) == 0 {
				delete(l.subscribers, topic)
			}
			break
		}
	}
}

func (l *PostgreSQLListener) start() error {
	l.mu.Lock()
	defer l.mu.Unlock()

	if l.closed {
		return fmt.Errorf("listener is closed")
	}

	// Start listening on the single global channel
	_, err := l.pool.Exec(l.ctx, "LISTEN watermill_messages")
	if err != nil {
		return fmt.Errorf("failed to listen on channel watermill_messages: %w", err)
	}

	l.logger.Info("Started PostgreSQL LISTEN", watermill.LogFields{
		"channel": "watermill_messages",
	})

	// Start the notification forwarding goroutine
	l.wg.Add(1)
	go l.forwardNotifications()

	return nil
}

func (l *PostgreSQLListener) forwardNotifications() {
	defer l.wg.Done()

	for {
		conn, err := l.pool.Acquire(l.ctx)
		if err != nil {
			if l.ctx.Err() != nil {
				// Context cancelled, normal shutdown
				l.logger.Debug("Stopping notification forwarder (context cancelled)", nil)
				return
			}

			l.logger.Error("failed to acquire conn to LISTEN", err, nil)
			time.Sleep(l.notificationErrTimeout)
			continue
		}

		// Wait for notification or context cancellation
		notification, err := conn.Conn().WaitForNotification(l.ctx)
		conn.Release()

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
			topic := notification.Payload

			l.logger.Trace("Received PostgreSQL notification", watermill.LogFields{
				"channel": notification.Channel,
				"topic":   topic,
			})

			// Fan out to all subscribers for this topic
			l.subMu.RLock()
			subscribers := l.subscribers[topic]
			l.subMu.RUnlock()

			for _, ch := range subscribers {
				// Non-blocking send to avoid deadlocks
				select {
				case ch <- topic:
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
	l.subMu.Lock()
	for topic, subscribers := range l.subscribers {
		for _, ch := range subscribers {
			close(ch)
		}
		delete(l.subscribers, topic)
	}
	l.subMu.Unlock()

	// Remove from global map
	listenerMu.Lock()
	delete(listenerInstances, l.pool)
	listenerMu.Unlock()

	return nil
}
