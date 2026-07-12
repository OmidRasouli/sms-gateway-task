package queue

import (
	"context"

	"github.com/google/uuid"
	"github.com/segmentio/kafka-go"

	"github.com/OmidRasouli/sms-gateway-task/internal/domain"
)

// Enqueuer is the interface MessageService depends on. Using the interface
// instead of the concrete type allows tests to inject a stub.
type Enqueuer interface {
	Enqueue(ctx context.Context, userID, messageID uuid.UUID, msgType domain.MessageType) error
}

type Client struct {
	brokers       []string
	expressWriter *kafka.Writer
	normalWriter  *kafka.Writer
}

// NewClient creates a Kafka producer that routes messages to the express or
// normal topic based on message type (SLA-based topic routing). Within each
// topic, messages are partitioned by userID (via kafka.Hash on the message Key)
// so that all messages for a given user always land on the same partition —
// Kafka only guarantees ordering within a single partition, and DeductTx /
// ReverseDeductTx for the same user must be processed in order.
func NewClient(brokers []string) *Client {
	newWriter := func(topic string) *kafka.Writer {
		return &kafka.Writer{
			Addr:  kafka.TCP(brokers...),
			Topic: topic,
			// kafka.Hash routes messages to the partition whose hash matches
			// msg.Key. This is required for per-user ordering: LeastBytes
			// ignores Key entirely and provides no ordering guarantee.
			// Trade-off: keying by userID means partition load is no longer
			// balanced by byte volume. If a small number of users generate
			// disproportionate traffic, their partition(s) may become hot.
			// This is an accepted correctness trade-off; do not change to
			// a byte-balancing strategy without a plan for ordering.
			Balancer: &kafka.Hash{},
		}
	}
	return &Client{
		brokers:       brokers,
		expressWriter: newWriter(TopicExpress),
		normalWriter:  newWriter(TopicNormal),
	}
}

func (c *Client) Ping(ctx context.Context) error {
	conn, err := kafka.DialContext(ctx, "tcp", c.brokers[0])
	if err != nil {
		return err
	}
	return conn.Close()
}

func (c *Client) Close() error {
	err1 := c.expressWriter.Close()
	err2 := c.normalWriter.Close()
	if err1 != nil {
		return err1
	}
	return err2
}

func (c *Client) Enqueue(ctx context.Context, userID, messageID uuid.UUID, msgType domain.MessageType) error {
	payload, err := marshalPayload(messageID)
	if err != nil {
		return err
	}
	msg := kafka.Message{
		Key:   []byte(userID.String()),
		Value: payload,
	}
	if msgType == domain.MessageTypeExpress {
		return c.expressWriter.WriteMessages(ctx, msg)
	}
	return c.normalWriter.WriteMessages(ctx, msg)
}
