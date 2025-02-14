package sql

import (
	"context"
	"sync"
	"time"

	"github.com/ThreeDotsLabs/watermill"
	"github.com/jackc/pgx/v5"
)

type pgNotificationQueueBackoffManagerDecorator struct {
	next        BackoffManager
	channelName string
	logger      watermill.LoggerAdapter

	topicNotificationMx sync.RWMutex
	topicNotifications  map[string]chan string
}

// NewPgNotificationBackoffManagerDecorator decorates a [BackoffManager], by making [BackoffManager.HandleError]
// wait for notifications from a pgsql notification queue channel, when no messages are available in the subscriber.
// The decorated [BackoffManager] will still timeout on the poll intervall of the underlying [BackoffManager],
// to pickup messages even when no notification has been sent.
//
// channelName should be a notification queue channel, where e.g. a decorated publisher, pushes the topic name
// when a message has been published.
//
// Example:
//
//	m := NewDefaultBackoffManager(time.Second * 10, time.Second)
//	m = NewPgNotificationBackoffManagerDecorator(logger, "topic_channel", m)
//
//	// will block for 10 seconds or until a notification arrives in "topic_channel", when the timeout occurs
//	// d will be 0
//	d := m.HandleError(ctx, logger, true, nil)
func NewPgNotificationBackoffManagerDecorator(logger watermill.LoggerAdapter, channelName string, next BackoffManager) BackoffManager {
	return &pgNotificationQueueBackoffManagerDecorator{
		next:               next,
		channelName:        channelName,
		logger:             logger,
		topicNotifications: map[string]chan string{},
	}
}

// Listen to channelName in the pg notififaction queue.
// Notifications will be forwarded to buffered (1) go channels and consumed in [HandleError]
func (m *pgNotificationQueueBackoffManagerDecorator) Listen(ctx context.Context, conn *pgx.Conn) error {
	_, err := conn.Exec(ctx, "listen "+m.channelName)
	if err != nil {
		return err
	}

	defer func() {
		m.topicNotificationMx.RLock()
		defer m.topicNotificationMx.RUnlock()
		for _, ch := range m.topicNotifications {
			close(ch)
		}
	}()

	for {
		if conn.IsClosed() {
			return nil
		}

		n, err := conn.WaitForNotification(ctx)
		if err != nil {
			m.logger.Error("failed to wait for pg notification", err, nil)
			continue
		}

		topic := n.Payload

		m.topicNotificationMx.RLock()
		ch, ok := m.topicNotifications[topic]
		m.topicNotificationMx.RUnlock()
		if !ok {
			ch = make(chan string, 1)
			m.topicNotificationMx.Lock()
			m.topicNotifications[topic] = ch
			m.topicNotificationMx.Unlock()
		}

		select {
		case ch <- topic:
		default:
			// TODO: add trace log here?
		}
	}
}

// HandleError calls the underlying [BackoffManager], and blocks, when `noMsg == true`.
// In this case, it awaits the next notification, from the pg notification queue or the timeout, provided
// by the underlying [BackoffManager]
func (m *pgNotificationQueueBackoffManagerDecorator) HandleError(ctx context.Context, logger watermill.LoggerAdapter, noMsg bool, err error) time.Duration {
	topic, ok := TopicFromContext(ctx)
	if !ok {
		logger.Error("no topic found in ctx, falling back to default backoff manager", nil, nil)
		return m.next.HandleError(ctx, logger, noMsg, err)
	}

	if noMsg {
		timeout := m.next.HandleError(ctx, logger, noMsg, err)

		select {
		case <-ctx.Done():
			return 0
		case <-time.After(timeout):
			return 0
		case <-m.getNotificationChannel(topic):
		}
	}

	return m.next.HandleError(ctx, logger, noMsg, err)
}

func (m *pgNotificationQueueBackoffManagerDecorator) getNotificationChannel(topic string) <-chan string {
	m.topicNotificationMx.RLock()
	ch, ok := m.topicNotifications[topic]
	m.topicNotificationMx.RUnlock()

	if !ok {
		m.topicNotificationMx.Lock()
		ch = make(chan string, 1)
		m.topicNotifications[topic] = ch
		m.topicNotificationMx.Unlock()
	}

	return ch
}

var _ BackoffManager = new(pgNotificationQueueBackoffManagerDecorator)
