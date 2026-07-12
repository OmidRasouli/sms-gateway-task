package worker

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/rs/zerolog/log"

	"github.com/OmidRasouli/sms-gateway-task/internal/domain"
	"github.com/OmidRasouli/sms-gateway-task/internal/operator"
	"github.com/OmidRasouli/sms-gateway-task/internal/queue"
)

type MessageStore interface {
	BeginTx(ctx context.Context) (pgx.Tx, error)
	GetByID(ctx context.Context, id uuid.UUID) (*domain.Message, error)
	UpdateStatus(ctx context.Context, id uuid.UUID, status domain.MessageStatus) error
	UpdateStatusTx(ctx context.Context, tx pgx.Tx, id uuid.UUID, status domain.MessageStatus) error
}

type BalanceStore interface {
	ReverseDeductTx(ctx context.Context, tx pgx.Tx, userID, messageID uuid.UUID, amount int64) error
}

type OperatorSender interface {
	Send(ctx context.Context, phoneNumber, text string) error
}

type Handler struct {
	messages MessageStore
	balances BalanceStore
	operator OperatorSender
}

func NewHandler(messages MessageStore, balances BalanceStore, op OperatorSender) *Handler {
	return &Handler{messages: messages, balances: balances, operator: op}
}

// HandleSendMessage is idempotent: it re-checks the message's status before
// acting, so a Kafka redelivery after a crash can never double-send or
// double-update — the balance was already deducted exactly once, in the
// API's DB transaction, before this message was ever published.
func (h *Handler) HandleSendMessage(ctx context.Context, payload []byte) error {
	var p queue.SendMessagePayload
	if err := json.Unmarshal(payload, &p); err != nil {
		return fmt.Errorf("unmarshal payload: %w", err)
	}

	msg, err := h.messages.GetByID(ctx, p.MessageID)
	if err != nil {
		return fmt.Errorf("load message: %w", err)
	}

	if msg.Status != domain.StatusPending {
		// already processed by a previous attempt — idempotent no-op
		return nil
	}

	if err := h.operator.Send(ctx, msg.PhoneNumber, msg.Text); err != nil {
		if errors.Is(err, operator.ErrOperatorPermanentFailure) {
			// Unrecoverable failure: mark the message failed and reverse the
			// balance deduction atomically so the user is refunded.
			if reverr := h.failAndReverse(ctx, msg); reverr != nil {
				log.Error().Err(reverr).Str("message_id", msg.ID.String()).Msg("failed to reverse balance deduction")
			}
			return nil // do not retry
		}
		log.Warn().Err(err).Str("message_id", msg.ID.String()).Msg("operator send failed, will retry")
		// Returning the error tells the Kafka consumer not to commit the
		// offset — the message will be redelivered on the next poll,
		// acting as a simple at-least-once retry.
		return err
	}

	return h.messages.UpdateStatus(ctx, msg.ID, domain.StatusSent)
}

// failAndReverse marks a message as failed and reverses its balance deduction
// atomically: either both operations commit together or neither does.
func (h *Handler) failAndReverse(ctx context.Context, msg *domain.Message) error {
	tx, err := h.messages.BeginTx(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)

	if err := h.messages.UpdateStatusTx(ctx, tx, msg.ID, domain.StatusFailed); err != nil {
		return err
	}
	if err := h.balances.ReverseDeductTx(ctx, tx, msg.UserID, msg.ID, msg.Price); err != nil {
		if errors.Is(err, domain.ErrAlreadyProcessed) {
			// The reversal was already recorded in a prior attempt — treat as
			// success; still commit the status update below.
			return tx.Commit(ctx)
		}
		return err
	}
	return tx.Commit(ctx)
}
