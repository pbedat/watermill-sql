package pg_notify

import (
	"context"
	"database/sql"
	"fmt"
	"testing"
	"time"

	"github.com/ThreeDotsLabs/watermill"
	watermillSQL "github.com/ThreeDotsLabs/watermill-sql/v4/pkg/sql"
	"github.com/ThreeDotsLabs/watermill-sql/v4/pkg/x/pg/notify"
	"github.com/ThreeDotsLabs/watermill/message"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/modules/postgres"
	"github.com/testcontainers/testcontainers-go/wait"
)

type fixture struct {
	beginner       watermillSQL.Beginner
	schemaAdapter  watermillSQL.SchemaAdapter
	offsetsAdapter watermillSQL.OffsetsAdapter
	connStr        string
	ctx            context.Context
}

const topic = "test_notify_topic"
const consumerGroup = "test_consumer_group"

func setup(t *testing.T) fixture {
	ctx := context.Background()

	// Start PostgreSQL container
	postgresContainer, err := postgres.Run(ctx,
		"postgres:15.3",
		postgres.WithDatabase("watermill"),
		postgres.WithUsername("watermill"),
		postgres.WithPassword("password"),
		testcontainers.WithWaitStrategy(
			wait.ForLog("database system is ready to accept connections").
				WithOccurrence(2).
				WithStartupTimeout(60*time.Second)),
	)
	require.NoError(t, err)
	t.Cleanup(func() {
		if err := postgresContainer.Terminate(ctx); err != nil {
			t.Logf("failed to terminate container: %s", err)
		}
	})

	// Get connection string
	connStr, err := postgresContainer.ConnectionString(ctx, "sslmode=disable")
	require.NoError(t, err)

	// Connect to database
	db, err := sql.Open("postgres", connStr)
	require.NoError(t, err)
	t.Cleanup(func() {
		db.Close()
	})

	err = db.Ping()
	require.NoError(t, err)

	// Create schema adapter with trigger support
	schemaAdapter := &notify.NotifySchemaDecorator{
		watermillSQL.DefaultPostgreSQLSchema{
			GeneratePayloadType: func(t string) string {
				return "BYTEA" // Use BYTEA for non-JSON payloads
			},
		},
	}

	// Initialize schema
	beginner := watermillSQL.BeginnerFromStdSQL(db)

	return fixture{
		schemaAdapter:  schemaAdapter,
		offsetsAdapter: watermillSQL.DefaultPostgreSQLOffsetsAdapter{},
		beginner:       beginner,
		connStr:        connStr,
		ctx:            ctx,
	}
}

func TestNotifyChannelWithPostgreSQLListenNotify(t *testing.T) {

	logger := watermill.NewStdLogger(false, false)

	t.Run("latency_comparison", func(t *testing.T) {
		t.Parallel()
		// Test 1: Without NotifyChannel (pure polling)
		t.Run("without_notify_channel", func(t *testing.T) {
			t.Parallel()
			fixture := setup(t)

			beginner := fixture.beginner
			schemaAdapter := fixture.schemaAdapter
			offsetsAdapter := fixture.offsetsAdapter
			ctx := fixture.ctx

			publisher, err := watermillSQL.NewPublisher(
				beginner,
				watermillSQL.PublisherConfig{
					SchemaAdapter:        schemaAdapter,
					AutoInitializeSchema: true,
				},
				logger,
			)
			require.NoError(t, err)
			defer publisher.Close()

			pollInterval := time.Second

			subscriber, err := watermillSQL.NewSubscriber(
				beginner,
				watermillSQL.SubscriberConfig{
					ConsumerGroup:    consumerGroup + "_no_notify",
					SchemaAdapter:    schemaAdapter,
					OffsetsAdapter:   offsetsAdapter,
					PollInterval:     pollInterval,
					InitializeSchema: true,
				},
				logger,
			)
			require.NoError(t, err)
			defer subscriber.Close()

			require.NoError(t, subscriber.SubscribeInitialize(topic))

			messages, err := subscriber.Subscribe(ctx, topic)
			require.NoError(t, err)

			// Give subscriber time to complete initial poll
			time.Sleep(100 * time.Millisecond)

			// Publish a message and measure time to receive
			testMsg := message.NewMessage(watermill.NewUUID(), []byte("test message without notify"))
			publishTime := time.Now()

			err = publisher.Publish(topic, testMsg)
			require.NoError(t, err)

			// Wait for message
			select {
			case receivedMsg := <-messages:
				receiveTime := time.Now()
				latency := receiveTime.Sub(publishTime)

				t.Logf("Latency WITHOUT NotifyChannel: %v", latency)

				// Should be at least close to poll interval
				assert.GreaterOrEqual(t, latency, pollInterval/2,
					"Expected latency to be significant with polling")

				receivedMsg.Ack()

			case <-time.After(5 * time.Second):
				t.Fatal("Timeout waiting for message")
			}
		})

		// Test 2: With NotifyChannel (LISTEN/NOTIFY)
		t.Run("with_notify_channel", func(t *testing.T) {
			t.Parallel()
			fixture := setup(t)
			beginner := fixture.beginner
			schemaAdapter := fixture.schemaAdapter
			offsetsAdapter := fixture.offsetsAdapter
			ctx := fixture.ctx

			pollInterval := 500 * time.Millisecond

			pool, err := pgxpool.New(ctx, fixture.connStr)
			require.NoError(t, err)
			defer pool.Close()

			// Get or create singleton listener
			pgListener, err := notify.NewListener(
				pool,
				notify.PostgreSQLListenerConfig{},
				logger)
			require.NoError(t, err)
			defer pgListener.Close()

			publisher, err := watermillSQL.NewPublisher(
				beginner,
				watermillSQL.PublisherConfig{
					SchemaAdapter: schemaAdapter,
				},
				logger,
			)
			require.NoError(t, err)
			defer publisher.Close()

			subscriber, err := watermillSQL.NewSubscriber(
				beginner,
				watermillSQL.SubscriberConfig{
					ConsumerGroup:          consumerGroup + "_with_notify",
					SchemaAdapter:          schemaAdapter,
					OffsetsAdapter:         offsetsAdapter,
					PollInterval:           pollInterval,
					ResendInterval:         100 * time.Millisecond,
					SubscribeNotifications: pgListener.Register, // Enable instant notification
				},
				logger,
			)
			require.NoError(t, err)
			defer subscriber.Close()

			require.NoError(t, subscriber.SubscribeInitialize(topic))
			messages, err := subscriber.Subscribe(ctx, topic)
			require.NoError(t, err)

			// Publish a message and measure time to receive
			testMsg := message.NewMessage(watermill.NewUUID(), []byte("test message with notify"))
			publishTime := time.Now()

			err = publisher.Publish(topic, testMsg)
			require.NoError(t, err)

			// Wait for message
			select {
			case receivedMsg := <-messages:
				receiveTime := time.Now()
				latency := receiveTime.Sub(publishTime)

				t.Logf("Latency WITH NotifyChannel: %v", latency)

				// Should be much faster than poll interval
				assert.Less(t, latency, 200*time.Millisecond,
					"Expected latency to be much lower with NOTIFY")

				receivedMsg.Ack()

			case <-time.After(5 * time.Second):
				t.Fatal("Timeout waiting for message")
			}
		})
	})

	t.Run("burst_messages", func(t *testing.T) {
		t.Parallel()

		fixture := setup(t)
		beginner := fixture.beginner
		schemaAdapter := fixture.schemaAdapter
		offsetsAdapter := fixture.offsetsAdapter
		ctx := fixture.ctx
		connStr := fixture.connStr

		pool, err := pgxpool.New(ctx, connStr)
		require.NoError(t, err)
		defer pool.Close()

		// Get or create singleton listener
		pgListener, err := notify.NewListener(
			pool,
			notify.PostgreSQLListenerConfig{},
			logger)
		require.NoError(t, err)
		defer pgListener.Close()

		time.Sleep(100 * time.Millisecond)

		publisher, err := watermillSQL.NewPublisher(
			beginner,
			watermillSQL.PublisherConfig{
				SchemaAdapter: schemaAdapter,
			},
			logger,
		)
		require.NoError(t, err)
		defer publisher.Close()

		subscriber, err := watermillSQL.NewSubscriber(
			beginner,
			watermillSQL.SubscriberConfig{
				ConsumerGroup:          consumerGroup + "_burst",
				SchemaAdapter:          schemaAdapter,
				OffsetsAdapter:         offsetsAdapter,
				PollInterval:           1 * time.Second,
				ResendInterval:         100 * time.Millisecond,
				SubscribeNotifications: pgListener.Register,
			},
			logger,
		)
		require.NoError(t, err)
		defer subscriber.Close()

		require.NoError(t, subscriber.SubscribeInitialize(topic))
		messages, err := subscriber.Subscribe(ctx, topic)
		require.NoError(t, err)

		// Publish multiple messages rapidly
		messageCount := 10
		publishedUUIDs := make(map[string]bool)

		for i := range messageCount {
			msg := message.NewMessage(watermill.NewUUID(), []byte(fmt.Sprintf("burst message %d", i)))
			publishedUUIDs[msg.UUID] = false
			err = publisher.Publish(topic, msg)
			require.NoError(t, err)
		}

		// Receive all messages
		received := 0
		timeout := time.After(10 * time.Second)

		for received < messageCount {
			select {
			case msg := <-messages:
				publishedUUIDs[msg.UUID] = true
				msg.Ack()
				received++
				t.Logf("Received burst message %d/%d", received, messageCount)

			case <-timeout:
				t.Fatalf("Timeout: only received %d/%d messages", received, messageCount)
			}
		}

		// Verify all messages were received
		for uuid, wasReceived := range publishedUUIDs {
			assert.True(t, wasReceived, "Message %s was not received", uuid)
		}
	})

	t.Run("fallback_on_listener_failure", func(t *testing.T) {
		t.Parallel()

		// Clean up any existing messages
		fixture := setup(t)
		beginner := fixture.beginner
		schemaAdapter := fixture.schemaAdapter
		offsetsAdapter := fixture.offsetsAdapter
		ctx := fixture.ctx
		connStr := fixture.connStr

		pool, err := pgxpool.New(ctx, connStr)
		require.NoError(t, err)

		// Get or create singleton listener
		pgListener, err := notify.NewListener(
			pool,
			notify.PostgreSQLListenerConfig{
				NotificationErrTimeout: 500 * time.Millisecond,
			},
			logger)
		require.NoError(t, err)

		time.Sleep(100 * time.Millisecond)

		publisher, err := watermillSQL.NewPublisher(
			beginner,
			watermillSQL.PublisherConfig{
				SchemaAdapter: schemaAdapter,
			},
			logger,
		)
		require.NoError(t, err)
		defer publisher.Close()

		subscriber, err := watermillSQL.NewSubscriber(
			beginner,
			watermillSQL.SubscriberConfig{
				ConsumerGroup:          consumerGroup + "_fallback",
				SchemaAdapter:          schemaAdapter,
				OffsetsAdapter:         offsetsAdapter,
				PollInterval:           300 * time.Millisecond,
				ResendInterval:         100 * time.Millisecond,
				SubscribeNotifications: pgListener.Register,
			},
			logger,
		)
		require.NoError(t, err)
		defer subscriber.Close()

		require.NoError(t, subscriber.SubscribeInitialize(topic))
		messages, err := subscriber.Subscribe(ctx, topic)
		require.NoError(t, err)

		// Publish first message with listener active
		msg1 := message.NewMessage(watermill.NewUUID(), []byte("message before listener close"))
		err = publisher.Publish(topic, msg1)
		require.NoError(t, err)

		select {
		case receivedMsg := <-messages:
			assert.Equal(t, msg1.UUID, receivedMsg.UUID)
			receivedMsg.Ack()
		case <-time.After(2 * time.Second):
			t.Fatal("Timeout receiving first message")
		}

		// Close the listener to simulate failure
		t.Log("Closing PostgreSQL listener to test fallback")
		err = pgListener.Close()
		require.NoError(t, err)

		// Publish second message without listener - should still work via polling
		msg2 := message.NewMessage(watermill.NewUUID(), []byte("message after listener close"))
		err = publisher.Publish(topic, msg2)
		require.NoError(t, err)

		select {
		case receivedMsg := <-messages:
			assert.Equal(t, msg2.UUID, receivedMsg.UUID)
			receivedMsg.Ack()
			t.Log("Successfully received message via polling fallback")
		case <-time.After(3 * time.Second):
			t.Fatal("Timeout receiving second message - fallback failed")
		}
	})

	t.Run("multiple_topics_single_listener", func(t *testing.T) {
		t.Parallel()

		fixture := setup(t)
		beginner := fixture.beginner
		schemaAdapter := fixture.schemaAdapter
		offsetsAdapter := fixture.offsetsAdapter
		ctx := fixture.ctx
		connStr := fixture.connStr

		pool, err := pgxpool.New(ctx, connStr)
		require.NoError(t, err)
		defer pool.Close()

		// Get or create singleton listener
		pgListener, err := notify.NewListener(
			pool,
			notify.PostgreSQLListenerConfig{},
			logger)
		require.NoError(t, err)
		defer pgListener.Close()

		// Set up two different topics
		topic1 := "test_topic_1"
		topic2 := "test_topic_2"

		// Create publisher
		publisher, err := watermillSQL.NewPublisher(
			beginner,
			watermillSQL.PublisherConfig{
				SchemaAdapter: schemaAdapter,
			},
			logger,
		)
		require.NoError(t, err)
		defer publisher.Close()

		// Create subscriber for topic 1
		subscriber1, err := watermillSQL.NewSubscriber(
			beginner,
			watermillSQL.SubscriberConfig{
				ConsumerGroup:          consumerGroup + "_multi_topic1",
				SchemaAdapter:          schemaAdapter,
				OffsetsAdapter:         offsetsAdapter,
				PollInterval:           1 * time.Second,
				ResendInterval:         100 * time.Millisecond,
				SubscribeNotifications: pgListener.Register,
			},
			logger,
		)
		require.NoError(t, err)
		defer subscriber1.Close()

		// Create subscriber for topic 2
		subscriber2, err := watermillSQL.NewSubscriber(
			beginner,
			watermillSQL.SubscriberConfig{
				ConsumerGroup:          consumerGroup + "_multi_topic2",
				SchemaAdapter:          schemaAdapter,
				OffsetsAdapter:         offsetsAdapter,
				PollInterval:           1 * time.Second,
				ResendInterval:         100 * time.Millisecond,
				SubscribeNotifications: pgListener.Register,
			},
			logger,
		)
		require.NoError(t, err)
		defer subscriber2.Close()

		// Initialize schemas
		require.NoError(t, subscriber1.SubscribeInitialize(topic1))
		require.NoError(t, subscriber2.SubscribeInitialize(topic2))

		// Subscribe to both topics
		messages1, err := subscriber1.Subscribe(ctx, topic1)
		require.NoError(t, err)

		messages2, err := subscriber2.Subscribe(ctx, topic2)
		require.NoError(t, err)

		time.Sleep(200 * time.Millisecond)

		// Publish to topic 1
		msg1 := message.NewMessage(watermill.NewUUID(), []byte("message for topic 1"))
		err = publisher.Publish(topic1, msg1)
		require.NoError(t, err)

		// Publish to topic 2
		msg2 := message.NewMessage(watermill.NewUUID(), []byte("message for topic 2"))
		err = publisher.Publish(topic2, msg2)
		require.NoError(t, err)

		// Verify subscriber 1 receives only topic 1 message
		select {
		case receivedMsg := <-messages1:
			assert.Equal(t, msg1.UUID, receivedMsg.UUID)
			assert.Equal(t, "message for topic 1", string(receivedMsg.Payload))
			receivedMsg.Ack()
			t.Log("Subscriber 1 correctly received topic 1 message")
		case <-time.After(2 * time.Second):
			t.Fatal("Timeout receiving message on topic 1")
		}

		// Verify subscriber 2 receives only topic 2 message
		select {
		case receivedMsg := <-messages2:
			assert.Equal(t, msg2.UUID, receivedMsg.UUID)
			assert.Equal(t, "message for topic 2", string(receivedMsg.Payload))
			receivedMsg.Ack()
			t.Log("Subscriber 2 correctly received topic 2 message")
		case <-time.After(2 * time.Second):
			t.Fatal("Timeout receiving message on topic 2")
		}

		// Ensure no cross-topic messages are received
		select {
		case <-messages1:
			t.Fatal("Subscriber 1 incorrectly received a message (should only get topic 1)")
		case <-messages2:
			t.Fatal("Subscriber 2 incorrectly received a message (should only get topic 2)")
		case <-time.After(500 * time.Millisecond):
			t.Log("Verified no cross-topic messages received")
		}
	})
}
