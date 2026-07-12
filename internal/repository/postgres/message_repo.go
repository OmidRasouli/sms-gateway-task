package postgres

import (
	"context"
	"errors"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/OmidRasouli/sms-gateway-task/internal/domain"
)

type MessageRepo struct {
	pool *pgxpool.Pool
}

func NewMessageRepo(pool *pgxpool.Pool) *MessageRepo {
	return &MessageRepo{pool: pool}
}

func (r *MessageRepo) BeginTx(ctx context.Context) (pgx.Tx, error) {
	return r.pool.Begin(ctx)
}

// CreateTx inserts a new pending message inside an existing transaction —
// called right after BalanceRepo.DeductTx so the deduction and the message
// row are committed together, atomically.
func (r *MessageRepo) CreateTx(ctx context.Context, tx pgx.Tx, m *domain.Message) error {
	return tx.QueryRow(ctx, `
		INSERT INTO messages (user_id, phone_number, text, message_type, price, status)
		VALUES ($1, $2, $3, $4, $5, 'pending')
		RETURNING id, created_at`,
		m.UserID, m.PhoneNumber, m.Text, m.Type, m.Price,
	).Scan(&m.ID, &m.CreatedAt)
}

// UpdateStatus updates the message's status (and sets sent_at if the new
// status is "sent"). This is called by the async worker after the operator
// successfully sends the message, or by a reconciliation job that marks
func (r *MessageRepo) UpdateStatus(ctx context.Context, id uuid.UUID, status domain.MessageStatus) error {
	_, err := r.pool.Exec(ctx, `
		UPDATE messages
		SET status = $1, sent_at = CASE WHEN $1 = 'sent' THEN now() ELSE sent_at END
		WHERE id = $2`,
		status, id,
	)
	return err
}

// UpdateStatusTx updates the message's status within an existing transaction.
// Use this when the status change must be atomic with other operations (e.g.
// marking a message failed while simultaneously reversing its balance deduction).
func (r *MessageRepo) UpdateStatusTx(ctx context.Context, tx pgx.Tx, id uuid.UUID, status domain.MessageStatus) error {
	_, err := tx.Exec(ctx, `
		UPDATE messages
		SET status = $1, sent_at = CASE WHEN $1 = 'sent' THEN now() ELSE sent_at END
		WHERE id = $2`,
		status, id,
	)
	return err
}

// ListByUser returns the last messages for a user, ordered by created_at descending.
func (r *MessageRepo) ListByUser(ctx context.Context, userID uuid.UUID, limit, offset int) ([]domain.Message, error) {
	rows, err := r.pool.Query(ctx, `
		SELECT id, user_id, phone_number, text, message_type, price, status, created_at, sent_at
		FROM messages
		WHERE user_id = $1
		ORDER BY created_at DESC
		LIMIT $2 OFFSET $3`,
		userID, limit, offset,
	)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var out []domain.Message
	for rows.Next() {
		var m domain.Message
		if err := rows.Scan(&m.ID, &m.UserID, &m.PhoneNumber, &m.Text, &m.Type, &m.Price, &m.Status, &m.CreatedAt, &m.SentAt); err != nil {
			return nil, err
		}
		out = append(out, m)
	}
	return out, rows.Err()
}

// GetByID returns a message by its ID, or domain.ErrNotFound if no row exists.
func (r *MessageRepo) GetByID(ctx context.Context, id uuid.UUID) (*domain.Message, error) {
	var m domain.Message
	err := r.pool.QueryRow(ctx, `
		SELECT id, user_id, phone_number, text, message_type, price, status, created_at, sent_at
		FROM messages WHERE id = $1`, id,
	).Scan(&m.ID, &m.UserID, &m.PhoneNumber, &m.Text, &m.Type, &m.Price, &m.Status, &m.CreatedAt, &m.SentAt)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, domain.ErrNotFound
	}
	if err != nil {
		return nil, err
	}
	return &m, nil
}
