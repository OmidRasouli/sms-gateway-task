package queue

import (
	"encoding/json"

	"github.com/google/uuid"
)

const (
	TopicExpress    = "sms.express"
	TopicNormal     = "sms.normal"
	TopicExpressDLQ = TopicExpress + "-dlq"
	TopicNormalDLQ  = TopicNormal + "-dlq"
)

type SendMessagePayload struct {
	MessageID uuid.UUID `json:"message_id"`
}

func marshalPayload(messageID uuid.UUID) ([]byte, error) {
	return json.Marshal(SendMessagePayload{MessageID: messageID})
}
