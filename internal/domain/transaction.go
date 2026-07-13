package domain

import (
	"time"

	"github.com/google/uuid"
)

// BalanceTransactionType distinguishes deductions from reversals so the
// (message_id, type) unique index can enforce idempotency for each operation
// independently.
type BalanceTransactionType string

const (
	TransactionTypeCharge  BalanceTransactionType = "charge"
	TransactionTypeDeduct  BalanceTransactionType = "deduct"
	TransactionTypeReverse BalanceTransactionType = "reverse"
)

type BalanceTransaction struct {
	ID        uuid.UUID
	UserID    uuid.UUID
	Amount    int64
	Type      BalanceTransactionType
	MessageID *uuid.UUID
	CreatedAt time.Time
}
