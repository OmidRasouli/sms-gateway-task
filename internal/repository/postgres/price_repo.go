package postgres

import (
	"context"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/OmidRasouli/sms-gateway-task/internal/domain"
)

// PriceRepo loads message pricing from Postgres.
type PriceRepo struct {
	pool *pgxpool.Pool
}

// NewPriceRepo returns a PriceRepo backed by pool.
func NewPriceRepo(pool *pgxpool.Pool) *PriceRepo {
	return &PriceRepo{pool: pool}
}

// LoadPrices reads the message_pricing table and returns a map keyed by
// message type.
func (r *PriceRepo) LoadPrices(ctx context.Context) (map[domain.MessageType]int64, error) {
	rows, err := r.pool.Query(ctx, "SELECT message_type, price FROM message_pricing")
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	prices := make(map[domain.MessageType]int64)
	for rows.Next() {
		var msgType string
		var price int64
		if err := rows.Scan(&msgType, &price); err != nil {
			return nil, err
		}
		prices[domain.MessageType(msgType)] = price
	}
	return prices, rows.Err()
}
