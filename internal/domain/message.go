package domain

import (
	"errors"
	"time"

	"github.com/google/uuid"
)

type MessageType string

const (
	MessageTypeNormal  MessageType = "normal"
	MessageTypeExpress MessageType = "express"
)

type MessageStatus string

const (
	StatusPending MessageStatus = "pending"
	StatusSent    MessageStatus = "sent"
	StatusFailed  MessageStatus = "failed"
)

type Message struct {
	ID          uuid.UUID     `json:"id"`
	UserID      uuid.UUID     `json:"user_id"`
	PhoneNumber string        `json:"phone_number"`
	Text        string        `json:"text"`
	Type        MessageType   `json:"type"`
	Price       int64         `json:"price"`
	Status      MessageStatus `json:"status"`
	CreatedAt   time.Time     `json:"created_at"`
	SentAt      *time.Time    `json:"sent_at,omitempty"`
}

// ErrInsufficientBalance is returned when a user's balance can't cover
// the price of a message. Surfaced by BalanceRepo.DeductTx.
var ErrInsufficientBalance = errors.New("insufficient balance")

// ErrNotFound is returned when a requested resource does not exist.
var ErrNotFound = errors.New("not found")

// ErrAlreadyProcessed is returned by DeductTx / ReverseDeductTx when the
// same (messageID, type) pair already exists in balance_transactions,
// indicating the operation was already applied (Kafka at-least-once
// redelivery). Callers should treat this as a successful no-op.
var ErrAlreadyProcessed = errors.New("already processed")

// ErrUserNotFound is returned by ReverseDeductTx when the balance row for
// the given userID does not exist.
var ErrUserNotFound = errors.New("user not found")

// ErrPricingNotConfigured is returned when no price is found in the cache
// for the requested message type.
var ErrPricingNotConfigured = errors.New("pricing not configured for message type")

// ErrInvalidAmount is returned when amount is not a positive integer.
var ErrInvalidAmount = errors.New("amount must be greater than zero")
