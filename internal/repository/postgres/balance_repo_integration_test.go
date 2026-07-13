//go:build integration

package postgres_test

import (
	"context"
	"fmt"
	"os"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/OmidRasouli/sms-gateway-task/internal/domain"
	"github.com/OmidRasouli/sms-gateway-task/internal/repository/postgres"
)

// connectTestDB connects to the Postgres instance described by TEST_DB_DSN.
// The caller is responsible for applying the migrations before running.
//
// Set TEST_DB_DSN to a connection string such as:
//
//	postgres://user:pass@localhost:5432/testdb?sslmode=disable
func connectTestDB(t *testing.T) *pgxpool.Pool {
	t.Helper()
	dsn := os.Getenv("TEST_DB_DSN")
	if dsn == "" {
		t.Skip("TEST_DB_DSN not set — skipping integration test")
	}
	pool, err := pgxpool.New(context.Background(), dsn)
	if err != nil {
		t.Fatalf("pgxpool.New: %v", err)
	}
	if err := pool.Ping(context.Background()); err != nil {
		pool.Close()
		t.Fatalf("pool.Ping: %v", err)
	}
	t.Cleanup(pool.Close)
	return pool
}

// applySchema runs a minimal schema setup for the test, creating the tables
// and the idempotency index introduced in migration 000002. The test uses its
// own schema-qualified table prefix to avoid conflicting with real data.
func applySchema(t *testing.T, pool *pgxpool.Pool) string {
	t.Helper()
	schema := fmt.Sprintf("test_%s", uuid.New().String()[:8])
	ctx := context.Background()

	ddl := fmt.Sprintf(`
CREATE SCHEMA %[1]s;

CREATE TABLE %[1]s.balances (
    user_id UUID PRIMARY KEY,
    amount BIGINT NOT NULL DEFAULT 0 CHECK (amount >= 0),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE %[1]s.messages (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    user_id UUID NOT NULL,
    phone_number VARCHAR(20) NOT NULL,
    text TEXT NOT NULL,
    message_type VARCHAR(10) NOT NULL,
    price BIGINT NOT NULL,
    status VARCHAR(10) NOT NULL DEFAULT 'pending',
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    sent_at TIMESTAMPTZ
);

CREATE TABLE %[1]s.balance_transactions (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    user_id UUID NOT NULL,
    amount BIGINT NOT NULL,
    type TEXT NOT NULL CHECK (type IN ('deduct','reverse','charge')),
    message_id UUID REFERENCES %[1]s.messages(id),
    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE UNIQUE INDEX uq_bt_message_type_%[1]s
    ON %[1]s.balance_transactions (message_id, type)
    WHERE message_id IS NOT NULL;
`, schema)

	if _, err := pool.Exec(ctx, ddl); err != nil {
		t.Fatalf("applySchema: %v", err)
	}

	t.Cleanup(func() {
		pool.Exec(context.Background(), fmt.Sprintf("DROP SCHEMA %s CASCADE", schema)) //nolint:errcheck
	})

	return schema
}

// newPoolInSchema returns a pgxpool.Pool with search_path set to the given
// schema. Callers that need to begin transactions inside the test schema must
// use this pool (not the main pool) so that unqualified table references in
// queries resolve to the test schema rather than public.
func newPoolInSchema(t *testing.T, schema string) *pgxpool.Pool {
	t.Helper()
	dsn := os.Getenv("TEST_DB_DSN")
	cfg, err := pgxpool.ParseConfig(dsn + "&search_path=" + schema)
	if err != nil {
		t.Fatalf("ParseConfig with schema: %v", err)
	}
	p, err := pgxpool.NewWithConfig(context.Background(), cfg)
	if err != nil {
		t.Fatalf("pool with schema: %v", err)
	}
	t.Cleanup(p.Close)
	return p
}

// newRepoInSchema returns a BalanceRepo whose queries target the given schema
// by creating a new pool with search_path set to that schema.
func newRepoInSchema(t *testing.T, _ *pgxpool.Pool, schema string) *postgres.BalanceRepo {
	t.Helper()
	return postgres.NewBalanceRepo(newPoolInSchema(t, schema))
}

// seedBalance inserts a balance row for userID directly.
func seedBalance(t *testing.T, pool *pgxpool.Pool, schema string, userID uuid.UUID, amount int64) {
	t.Helper()
	_, err := pool.Exec(context.Background(),
		fmt.Sprintf(`INSERT INTO %s.balances (user_id, amount) VALUES ($1, $2)`, schema),
		userID, amount,
	)
	if err != nil {
		t.Fatalf("seedBalance: %v", err)
	}
}

// seedMessage inserts a placeholder message row and returns its ID.
func seedMessage(t *testing.T, pool *pgxpool.Pool, schema string, userID uuid.UUID) uuid.UUID {
	t.Helper()
	var id uuid.UUID
	err := pool.QueryRow(context.Background(),
		fmt.Sprintf(`INSERT INTO %s.messages (user_id, phone_number, text, message_type, price)
		             VALUES ($1, '09000000000', 'test', 'normal', 10) RETURNING id`, schema),
		userID,
	).Scan(&id)
	if err != nil {
		t.Fatalf("seedMessage: %v", err)
	}
	return id
}

// currentBalance returns the current balance for a user.
func currentBalance(t *testing.T, pool *pgxpool.Pool, schema string, userID uuid.UUID) int64 {
	t.Helper()
	var amount int64
	err := pool.QueryRow(context.Background(),
		fmt.Sprintf(`SELECT amount FROM %s.balances WHERE user_id = $1`, schema),
		userID,
	).Scan(&amount)
	if err != nil {
		t.Fatalf("currentBalance: %v", err)
	}
	return amount
}

// TestDeductTx_Concurrent spawns 20 goroutines each trying to deduct 10 from
// a balance seeded at 100. Exactly 10 should succeed and 10 should fail with
// ErrInsufficientBalance. The final balance must be exactly 0 (never negative).
func TestDeductTx_Concurrent(t *testing.T) {
	pool := connectTestDB(t)
	schema := applySchema(t, pool)
	repo := newRepoInSchema(t, pool, schema)
	schemaPool := newPoolInSchema(t, schema)

	userID := uuid.New()
	seedBalance(t, pool, schema, userID, 100)

	const goroutines = 20
	var (
		succeeded atomic.Int64
		wg        sync.WaitGroup
		errs      = make([]error, goroutines)
	)

	for i := 0; i < goroutines; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			ctx := context.Background()
			msgID := uuid.New()
			// Each goroutine uses its own message row in its own transaction.
			_, insertErr := pool.Exec(ctx,
				fmt.Sprintf(`INSERT INTO %s.messages (id, user_id, phone_number, text, message_type, price)
				             VALUES ($1, $2, '09000000000', 'test', 'normal', 10)`, schema),
				msgID, userID,
			)
			if insertErr != nil {
				errs[i] = insertErr
				return
			}

			tx, err := schemaPool.Begin(ctx)
			if err != nil {
				errs[i] = err
				return
			}
			defer tx.Rollback(ctx) //nolint:errcheck

			err = repo.DeductTx(ctx, tx, userID, msgID, 10)
			if err != nil {
				errs[i] = err
				return
			}
			if err := tx.Commit(ctx); err != nil {
				errs[i] = err
				return
			}
			succeeded.Add(1)
		}(i)
	}
	wg.Wait()

	var insufficientCount int
	for _, err := range errs {
		if err != nil && err != domain.ErrInsufficientBalance {
			t.Errorf("unexpected error: %v", err)
		}
		if err == domain.ErrInsufficientBalance {
			insufficientCount++
		}
	}

	got := succeeded.Load()
	if got != 10 {
		t.Errorf("expected 10 successful deductions, got %d", got)
	}
	if insufficientCount != 10 {
		t.Errorf("expected 10 ErrInsufficientBalance errors, got %d", insufficientCount)
	}

	if bal := currentBalance(t, pool, schema, userID); bal != 0 {
		t.Errorf("expected final balance 0, got %d", bal)
	}
}

// TestCharge_Concurrent spawns 10 goroutines each charging the same user 100
// units simultaneously. The UPSERT must be atomic: the final balance must be
// exactly 1000 (10 × 100) with no lost updates.
func TestCharge_Concurrent(t *testing.T) {
	pool := connectTestDB(t)
	schema := applySchema(t, pool)
	repo := newRepoInSchema(t, pool, schema)

	userID := uuid.New()
	const goroutines = 10
	const chargeAmount = int64(100)

	var wg sync.WaitGroup
	errs := make([]error, goroutines)

	for i := 0; i < goroutines; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			_, errs[i] = repo.Charge(context.Background(), userID, chargeAmount)
		}(i)
	}
	wg.Wait()

	for i, err := range errs {
		if err != nil {
			t.Errorf("goroutine %d: unexpected error: %v", i, err)
		}
	}

	if bal := currentBalance(t, pool, schema, userID); bal != goroutines*chargeAmount {
		t.Errorf("expected balance %d, got %d", goroutines*chargeAmount, bal)
	}
}

// TestChargeAndDeduct_Concurrent runs 5 charge goroutines (+100 each) and
// 10 deduct goroutines (-60 each) against the same user simultaneously,
// seeded with an initial balance of 100. The invariant is:
//
//	• The balance must never go negative (enforced by the CHECK constraint
//	  and the WHERE amount >= $1 guard in DeductTx).
//	• Every deduction that reports success must have been applied exactly once.
func TestChargeAndDeduct_Concurrent(t *testing.T) {
	pool := connectTestDB(t)
	schema := applySchema(t, pool)
	repo := newRepoInSchema(t, pool, schema)

	userID := uuid.New()
	seedBalance(t, pool, schema, userID, 100)

	const chargeGoroutines = 5
	const deductGoroutines = 10
	const chargeAmount = int64(100) // total added: 500
	const deductAmount = int64(60)  // each deduction: 60

	var (
		wg             sync.WaitGroup
		deductSucceeded atomic.Int64
	)

	// Launch charge goroutines.
	for i := 0; i < chargeGoroutines; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if _, err := repo.Charge(context.Background(), userID, chargeAmount); err != nil {
				t.Errorf("Charge: unexpected error: %v", err)
			}
		}()
	}

	// Launch deduct goroutines.
	deductErrs := make([]error, deductGoroutines)
	for i := 0; i < deductGoroutines; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			ctx := context.Background()
			msgID := uuid.New()
			_, insertErr := pool.Exec(ctx,
				fmt.Sprintf(`INSERT INTO %s.messages (id, user_id, phone_number, text, message_type, price)
				             VALUES ($1, $2, '09000000000', 'test', 'normal', %d)`, schema, deductAmount),
				msgID, userID,
			)
			if insertErr != nil {
				deductErrs[i] = insertErr
				return
			}
			tx, err := pool.Begin(ctx)
			if err != nil {
				deductErrs[i] = err
				return
			}
			defer tx.Rollback(ctx) //nolint:errcheck
			err = repo.DeductTx(ctx, tx, userID, msgID, deductAmount)
			if err == domain.ErrInsufficientBalance {
				deductErrs[i] = err
				return
			}
			if err != nil {
				deductErrs[i] = err
				return
			}
			if err := tx.Commit(ctx); err != nil {
				deductErrs[i] = err
				return
			}
			deductSucceeded.Add(1)
		}(i)
	}
	wg.Wait()

	for i, err := range deductErrs {
		if err != nil && err != domain.ErrInsufficientBalance {
			t.Errorf("deduct goroutine %d: unexpected error: %v", i, err)
		}
	}

	// final balance = 100 (seed) + 500 (charges) - succeeded*60
	expected := int64(100) + chargeGoroutines*chargeAmount - deductSucceeded.Load()*deductAmount
	if bal := currentBalance(t, pool, schema, userID); bal != expected {
		t.Errorf("expected balance %d, got %d (deductions succeeded: %d)", expected, bal, deductSucceeded.Load())
	}
}

// TestDeductTx_Idempotent verifies that calling DeductTx twice with the same
// messageID returns ErrAlreadyProcessed on the second call and does not deduct
// the balance a second time.
func TestDeductTx_Idempotent(t *testing.T) {
	pool := connectTestDB(t)
	schema := applySchema(t, pool)
	repo := newRepoInSchema(t, pool, schema)
	schemaPool := newPoolInSchema(t, schema)

	ctx := context.Background()
	userID := uuid.New()
	seedBalance(t, pool, schema, userID, 100)
	msgID := seedMessage(t, pool, schema, userID)

	// First deduction — should succeed.
	tx1, err := schemaPool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if err := repo.DeductTx(ctx, tx1, userID, msgID, 10); err != nil {
		tx1.Rollback(ctx)
		t.Fatalf("first DeductTx: %v", err)
	}
	if err := tx1.Commit(ctx); err != nil {
		t.Fatalf("tx1.Commit: %v", err)
	}

	if bal := currentBalance(t, pool, schema, userID); bal != 90 {
		t.Fatalf("expected balance 90 after first deduct, got %d", bal)
	}

	// Second deduction with the same messageID — should return ErrAlreadyProcessed.
	tx2, err := schemaPool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	err = repo.DeductTx(ctx, tx2, userID, msgID, 10)
	tx2.Rollback(ctx) // the caller must roll back when ErrAlreadyProcessed
	if err != domain.ErrAlreadyProcessed {
		t.Errorf("expected ErrAlreadyProcessed, got %v", err)
	}

	// Balance must still be 90 — the second call must not have deducted again.
	if bal := currentBalance(t, pool, schema, userID); bal != 90 {
		t.Errorf("expected balance 90 after idempotent re-delivery, got %d", bal)
	}
}
