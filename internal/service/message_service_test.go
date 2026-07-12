package service_test

import (
	"context"
	"errors"
	"testing"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"

	"github.com/OmidRasouli/sms-gateway-task/internal/domain"
	"github.com/OmidRasouli/sms-gateway-task/internal/service"
)

// mockTx satisfies pgx.Tx; only Commit and Rollback are exercised in tests.
type mockTx struct {
	commitErr    error
	commitCalled bool
}

func (m *mockTx) Begin(ctx context.Context) (pgx.Tx, error) { return m, nil }
func (m *mockTx) Commit(ctx context.Context) error {
	m.commitCalled = true
	return m.commitErr
}
func (m *mockTx) Rollback(ctx context.Context) error { return nil }
func (m *mockTx) CopyFrom(ctx context.Context, tableName pgx.Identifier, columnNames []string, rowSrc pgx.CopyFromSource) (int64, error) {
	return 0, nil
}
func (m *mockTx) SendBatch(ctx context.Context, b *pgx.Batch) pgx.BatchResults { return nil }
func (m *mockTx) LargeObjects() pgx.LargeObjects                               { return pgx.LargeObjects{} }
func (m *mockTx) Prepare(ctx context.Context, name, sql string) (*pgconn.StatementDescription, error) {
	return nil, nil
}
func (m *mockTx) Exec(ctx context.Context, sql string, arguments ...any) (pgconn.CommandTag, error) {
	return pgconn.CommandTag{}, nil
}
func (m *mockTx) Query(ctx context.Context, sql string, args ...any) (pgx.Rows, error) {
	return nil, nil
}
func (m *mockTx) QueryRow(ctx context.Context, sql string, args ...any) pgx.Row { return nil }
func (m *mockTx) Conn() *pgx.Conn                                               { return nil }

// mockMessageRepo

type mockMessageRepo struct {
	tx        pgx.Tx
	beginErr  error
	createErr error
}

func (m *mockMessageRepo) BeginTx(ctx context.Context) (pgx.Tx, error) {
	if m.beginErr != nil {
		return nil, m.beginErr
	}
	return m.tx, nil
}

func (m *mockMessageRepo) CreateTx(ctx context.Context, tx pgx.Tx, msg *domain.Message) error {
	if m.createErr != nil {
		return m.createErr
	}
	msg.ID = uuid.New()
	return nil
}

// mockBalanceRepo

type mockBalanceRepo struct {
	balance   int64
	deductErr error
}

func (m *mockBalanceRepo) GetBalance(ctx context.Context, userID uuid.UUID) (int64, error) {
	return m.balance, nil
}

func (m *mockBalanceRepo) Charge(ctx context.Context, userID uuid.UUID, amount int64) (int64, error) {
	return m.balance + amount, nil
}

func (m *mockBalanceRepo) DeductTx(ctx context.Context, tx pgx.Tx, userID, messageID uuid.UUID, amount int64) error {
	return m.deductErr
}

func (m *mockBalanceRepo) ReverseDeductTx(ctx context.Context, tx pgx.Tx, userID, messageID uuid.UUID, amount int64) error {
	return nil
}

// mockEnqueuer

type mockEnqueuer struct {
	err error
}

func (m *mockEnqueuer) Enqueue(ctx context.Context, userID, messageID uuid.UUID, msgType domain.MessageType) error {
	return m.err
}

// mockPriceCache

type mockPriceCache struct {
	prices map[domain.MessageType]int64
}

func (m *mockPriceCache) Get(msgType domain.MessageType) (int64, bool) {
	price, ok := m.prices[msgType]
	return price, ok
}

var defaultPrices = &mockPriceCache{
	prices: map[domain.MessageType]int64{
		domain.MessageTypeNormal:  10,
		domain.MessageTypeExpress: 25,
	},
}

// --- tests ---

func TestSendMessage_UnknownType(t *testing.T) {
	svc := service.NewMessageService(&mockBalanceRepo{}, &mockMessageRepo{}, &mockEnqueuer{}, defaultPrices)
	_, err := svc.SendMessage(context.Background(), uuid.New(), "+1234567890", "hello", "unknown")
	if err == nil {
		t.Fatal("expected error for unknown message type")
	}
}

func TestSendMessage_BeginTxError(t *testing.T) {
	beginErr := errors.New("db connection lost")
	svc := service.NewMessageService(
		&mockBalanceRepo{},
		&mockMessageRepo{beginErr: beginErr},
		&mockEnqueuer{},
		defaultPrices,
	)
	_, err := svc.SendMessage(context.Background(), uuid.New(), "+1234567890", "hello", domain.MessageTypeNormal)
	if !errors.Is(err, beginErr) {
		t.Fatalf("expected beginErr, got %v", err)
	}
}

func TestSendMessage_CreateTxError(t *testing.T) {
	createErr := errors.New("constraint violation")
	tx := &mockTx{}
	svc := service.NewMessageService(
		&mockBalanceRepo{},
		&mockMessageRepo{tx: tx, createErr: createErr},
		&mockEnqueuer{},
		defaultPrices,
	)
	_, err := svc.SendMessage(context.Background(), uuid.New(), "+1234567890", "hello", domain.MessageTypeNormal)
	if !errors.Is(err, createErr) {
		t.Fatalf("expected createErr, got %v", err)
	}
	if tx.commitCalled {
		t.Fatal("commit must not be called after create error")
	}
}

func TestSendMessage_InsufficientBalance(t *testing.T) {
	tx := &mockTx{}
	svc := service.NewMessageService(
		&mockBalanceRepo{deductErr: domain.ErrInsufficientBalance},
		&mockMessageRepo{tx: tx},
		&mockEnqueuer{},
		defaultPrices,
	)
	_, err := svc.SendMessage(context.Background(), uuid.New(), "+1234567890", "hello", domain.MessageTypeNormal)
	if !errors.Is(err, domain.ErrInsufficientBalance) {
		t.Fatalf("expected ErrInsufficientBalance, got %v", err)
	}
	if tx.commitCalled {
		t.Fatal("commit must not be called on insufficient balance")
	}
}

func TestSendMessage_CommitError(t *testing.T) {
	commitErr := errors.New("commit failed")
	tx := &mockTx{commitErr: commitErr}
	svc := service.NewMessageService(
		&mockBalanceRepo{},
		&mockMessageRepo{tx: tx},
		&mockEnqueuer{},
		defaultPrices,
	)
	_, err := svc.SendMessage(context.Background(), uuid.New(), "+1234567890", "hello", domain.MessageTypeNormal)
	if !errors.Is(err, commitErr) {
		t.Fatalf("expected commitErr, got %v", err)
	}
}

func TestSendMessage_EnqueueError(t *testing.T) {
	enqueueErr := errors.New("kafka unavailable")
	svc := service.NewMessageService(
		&mockBalanceRepo{},
		&mockMessageRepo{tx: &mockTx{}},
		&mockEnqueuer{err: enqueueErr},
		defaultPrices,
	)
	msg, err := svc.SendMessage(context.Background(), uuid.New(), "+1234567890", "hello", domain.MessageTypeNormal)
	// message is committed to DB but enqueue failed — caller gets both msg and err
	if msg == nil {
		t.Fatal("expected non-nil message when enqueue fails after commit")
	}
	if !errors.Is(err, enqueueErr) {
		t.Fatalf("expected wrapped enqueueErr, got %v", err)
	}
}

func TestSendMessage_NormalSuccess(t *testing.T) {
	tx := &mockTx{}
	svc := service.NewMessageService(
		&mockBalanceRepo{},
		&mockMessageRepo{tx: tx},
		&mockEnqueuer{},
		defaultPrices,
	)
	msg, err := svc.SendMessage(context.Background(), uuid.New(), "+1234567890", "hello", domain.MessageTypeNormal)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if msg.Type != domain.MessageTypeNormal {
		t.Errorf("expected Normal type, got %s", msg.Type)
	}
	if msg.Price != 10 {
		t.Errorf("expected price 10, got %d", msg.Price)
	}
	if msg.Status != domain.StatusPending {
		t.Errorf("expected pending status, got %s", msg.Status)
	}
	if !tx.commitCalled {
		t.Fatal("expected tx.Commit to be called on success")
	}
}

func TestSendMessage_ExpressSuccess(t *testing.T) {
	tx := &mockTx{}
	svc := service.NewMessageService(
		&mockBalanceRepo{},
		&mockMessageRepo{tx: tx},
		&mockEnqueuer{},
		defaultPrices,
	)
	msg, err := svc.SendMessage(context.Background(), uuid.New(), "+1234567890", "urgent", domain.MessageTypeExpress)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if msg.Price != 25 {
		t.Errorf("expected price 25, got %d", msg.Price)
	}
	if !tx.commitCalled {
		t.Fatal("expected tx.Commit to be called on success")
	}
}
