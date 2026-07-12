package service

import (
	"context"
	"fmt"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"

	"github.com/OmidRasouli/sms-gateway-task/internal/domain"
	"github.com/OmidRasouli/sms-gateway-task/internal/queue"
)

type BalanceRepo interface {
	GetBalance(ctx context.Context, userID uuid.UUID) (int64, error)
	Charge(ctx context.Context, userID uuid.UUID, amount int64) (int64, error)
	DeductTx(ctx context.Context, tx pgx.Tx, userID, messageID uuid.UUID, amount int64) error
	ReverseDeductTx(ctx context.Context, tx pgx.Tx, userID, messageID uuid.UUID, amount int64) error
}

type MessageRepo interface {
	BeginTx(ctx context.Context) (pgx.Tx, error)
	CreateTx(ctx context.Context, tx pgx.Tx, m *domain.Message) error
}

// PriceGetter returns the price for a given message type.
type PriceGetter interface {
	Get(msgType domain.MessageType) (int64, bool)
}

type MessageService struct {
	balances BalanceRepo
	messages MessageRepo
	queue    queue.Enqueuer
	prices   PriceGetter
}

func NewMessageService(balances BalanceRepo, messages MessageRepo, q queue.Enqueuer, prices PriceGetter) *MessageService {
	return &MessageService{balances: balances, messages: messages, queue: q, prices: prices}
}

// SendMessage is the core write path: create the pending message, deduct the
// balance, and record the transaction — all inside one DB transaction (atomic).
// After commit, the message is enqueued for async delivery to the operator.
func (s *MessageService) SendMessage(ctx context.Context, userID uuid.UUID, phone, text string, msgType domain.MessageType) (*domain.Message, error) {
	price, ok := s.prices.Get(msgType)
	if !ok {
		return nil, fmt.Errorf("%w: %s", domain.ErrPricingNotConfigured, msgType)
	}

	tx, err := s.messages.BeginTx(ctx)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback(ctx) // no-op if already committed

	// Create the message first so we have its ID to link the balance transaction.
	msg := &domain.Message{
		UserID:      userID,
		PhoneNumber: phone,
		Text:        text,
		Type:        msgType,
		Price:       price,
		Status:      domain.StatusPending,
	}
	if err := s.messages.CreateTx(ctx, tx, msg); err != nil {
		return nil, err
	}

	// Deduct balance and record the transaction entry linked to this message.
	if err := s.balances.DeductTx(ctx, tx, userID, msg.ID, price); err != nil {
		return nil, err // domain.ErrInsufficientBalance surfaces here
	}

	if err := tx.Commit(ctx); err != nil {
		return nil, err
	}

	// Enqueue happens after commit: if this fails, the message stays
	// "pending" in Postgres and can be recovered by a sweep job — the
	// balance deduction is never lost or duplicated either way.
	if err := s.queue.Enqueue(ctx, userID, msg.ID, msgType); err != nil {
		return msg, fmt.Errorf("message saved but enqueue failed: %w", err)
	}

	return msg, nil
}
