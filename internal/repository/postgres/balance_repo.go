package postgres

import (
	"context"
	"errors"
	"fmt"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/OmidRasouli/sms-gateway-task/internal/domain"
)

type BalanceRepo struct {
	pool *pgxpool.Pool
}

func NewBalanceRepo(pool *pgxpool.Pool) *BalanceRepo {
	return &BalanceRepo{pool: pool}
}

// GetBalance returns the current balance for a user (0 if no row exists yet).
func (r *BalanceRepo) GetBalance(ctx context.Context, userID uuid.UUID) (int64, error) {
	var amount int64
	err := r.pool.QueryRow(ctx,
		`SELECT amount FROM balances WHERE user_id = $1`, userID,
	).Scan(&amount)
	if errors.Is(err, pgx.ErrNoRows) {
		return 0, nil
	}
	return amount, err
}

func (r *BalanceRepo) GetTransactions(ctx context.Context, userID uuid.UUID, limit, offset int) ([]domain.BalanceTransaction, error) {
	rows, err := r.pool.Query(ctx,
		`SELECT id, amount, type, message_id, created_at
		 FROM balance_transactions
		 WHERE user_id = $1
		 ORDER BY created_at DESC
		 LIMIT $2 OFFSET $3`,
		userID, limit, offset,
	)
	if err != nil {
		return nil, fmt.Errorf("query transactions: %w", err)
	}

	txs, err := pgx.CollectRows(rows, func(row pgx.CollectableRow) (domain.BalanceTransaction, error) {
		t := domain.BalanceTransaction{UserID: userID}
		err := row.Scan(&t.ID, &t.Amount, &t.Type, &t.MessageID, &t.CreatedAt)
		return t, err
	})
	if err != nil {
		return nil, fmt.Errorf("scan transactions: %w", err)
	}

	return txs, nil
}

// Charge adds credit to a user's wallet. It atomically upserts the balance
// and records a positive entry in balance_transactions so every top-up is
// fully auditable.
func (r *BalanceRepo) Charge(ctx context.Context, userID uuid.UUID, amount int64) (int64, error) {
	tx, err := r.pool.Begin(ctx)
	if err != nil {
		return 0, err
	}
	defer tx.Rollback(ctx)

	var newAmount int64
	err = tx.QueryRow(ctx, `
		INSERT INTO balances (user_id, amount)
		VALUES ($1, $2)
		ON CONFLICT (user_id) DO UPDATE
		SET amount = balances.amount + EXCLUDED.amount, updated_at = now()
		RETURNING amount`,
		userID, amount,
	).Scan(&newAmount)
	if err != nil {
		return 0, err
	}

	_, err = tx.Exec(ctx, `
		INSERT INTO balance_transactions (user_id, amount)
		VALUES ($1, $2)`,
		userID, amount,
	)
	if err != nil {
		return 0, err
	}

	return newAmount, tx.Commit(ctx)
}

// isUniqueViolation returns true when err is a Postgres unique_violation (23505).
func isUniqueViolation(err error) bool {
	var pgErr *pgconn.PgError
	return errors.As(err, &pgErr) && pgErr.Code == "23505"
}

// DeductTx atomically deducts amount from the user's balance within an existing
// transaction and records a negative entry in balance_transactions linked to the
// message. The single-statement WHERE+UPDATE guarantees the balance never goes
// below zero without a separate lock.
//
// If the (messageID, "deduct") pair already exists in balance_transactions the
// function returns domain.ErrAlreadyProcessed — the balance UPDATE is NOT
// applied a second time, making the function safe under Kafka at-least-once
// redelivery.
func (r *BalanceRepo) DeductTx(ctx context.Context, tx pgx.Tx, userID, messageID uuid.UUID, amount int64) error {
	if amount <= 0 {
		return domain.ErrInvalidAmount
	}

	cmdTag, err := tx.Exec(ctx, `
		UPDATE balances
		SET amount = amount - $1, updated_at = now()
		WHERE user_id = $2 AND amount >= $1`,
		amount, userID,
	)
	if err != nil {
		return err
	}
	if cmdTag.RowsAffected() == 0 {
		return domain.ErrInsufficientBalance
	}

	_, err = tx.Exec(ctx, `
		INSERT INTO balance_transactions (user_id, amount, type, message_id)
		VALUES ($1, $2, 'deduct', $3)`,
		userID, -amount, messageID,
	)
	if isUniqueViolation(err) {
		// Already processed — roll back the balance change made above so the
		// caller's transaction stays clean; the caller should then return nil.
		return domain.ErrAlreadyProcessed
	}
	return err
}

// ReverseDeductTx reverses a prior deduction for a permanently failed message.
// It restores the amount to the user's balance and records a positive reversal
// entry in balance_transactions, all within the caller's transaction.
//
// Returns domain.ErrAlreadyProcessed if the reversal was already recorded
// (idempotent under Kafka redelivery), domain.ErrUserNotFound if the balance
// row does not exist, and domain.ErrInvalidAmount if amount ≤ 0.
func (r *BalanceRepo) ReverseDeductTx(ctx context.Context, tx pgx.Tx, userID, messageID uuid.UUID, amount int64) error {
	if amount <= 0 {
		return domain.ErrInvalidAmount
	}

	cmdTag, err := tx.Exec(ctx, `
		UPDATE balances
		SET amount = amount + $1, updated_at = now()
		WHERE user_id = $2`,
		amount, userID,
	)
	if err != nil {
		return err
	}
	if cmdTag.RowsAffected() == 0 {
		return domain.ErrUserNotFound
	}

	_, err = tx.Exec(ctx, `
		INSERT INTO balance_transactions (user_id, amount, type, message_id)
		VALUES ($1, $2, 'reverse', $3)`,
		userID, amount, messageID,
	)
	if isUniqueViolation(err) {
		return domain.ErrAlreadyProcessed
	}
	return err
}
