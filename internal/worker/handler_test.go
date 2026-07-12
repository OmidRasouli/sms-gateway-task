package worker_test

import (
	"context"
	"encoding/json"
	"errors"
	"testing"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"

	"github.com/OmidRasouli/sms-gateway-task/internal/domain"
	"github.com/OmidRasouli/sms-gateway-task/internal/operator"
	"github.com/OmidRasouli/sms-gateway-task/internal/queue"
	"github.com/OmidRasouli/sms-gateway-task/internal/worker"
)

// mockTx satisfies pgx.Tx; only Commit and Rollback matter for these tests.
type mockTx struct {
	commitErr error
}

func (m *mockTx) Begin(ctx context.Context) (pgx.Tx, error) { return m, nil }
func (m *mockTx) Commit(ctx context.Context) error          { return m.commitErr }
func (m *mockTx) Rollback(ctx context.Context) error        { return nil }
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

// mockMessageStore

type mockMessageStore struct {
	tx          pgx.Tx
	beginErr    error
	getMsg      *domain.Message
	getErr      error
	updateErr   error
	updateTxErr error
}

func (m *mockMessageStore) BeginTx(ctx context.Context) (pgx.Tx, error) {
	if m.beginErr != nil {
		return nil, m.beginErr
	}
	return m.tx, nil
}

func (m *mockMessageStore) GetByID(ctx context.Context, id uuid.UUID) (*domain.Message, error) {
	return m.getMsg, m.getErr
}

func (m *mockMessageStore) UpdateStatus(ctx context.Context, id uuid.UUID, status domain.MessageStatus) error {
	return m.updateErr
}

func (m *mockMessageStore) UpdateStatusTx(ctx context.Context, tx pgx.Tx, id uuid.UUID, status domain.MessageStatus) error {
	return m.updateTxErr
}

// mockBalanceStore

type mockBalanceStore struct {
	reverseErr error
}

func (m *mockBalanceStore) ReverseDeductTx(ctx context.Context, tx pgx.Tx, userID, messageID uuid.UUID, amount int64) error {
	return m.reverseErr
}

// mockOperatorSender

type mockOperatorSender struct {
	err error
}

func (m *mockOperatorSender) Send(ctx context.Context, phoneNumber, text string) error {
	return m.err
}

// helpers

func marshalPayload(t *testing.T, messageID uuid.UUID) []byte {
	t.Helper()
	b, err := json.Marshal(queue.SendMessagePayload{MessageID: messageID})
	if err != nil {
		t.Fatalf("marshal payload: %v", err)
	}
	return b
}

// --- tests ---

func TestWorkerHandler_InvalidPayload(t *testing.T) {
	h := worker.NewHandler(&mockMessageStore{}, &mockBalanceStore{}, &mockOperatorSender{})
	err := h.HandleSendMessage(context.Background(), []byte("not json"))
	if err == nil {
		t.Fatal("expected error for invalid JSON payload")
	}
}

func TestWorkerHandler_GetByIDError(t *testing.T) {
	getErr := errors.New("message not found in db")
	h := worker.NewHandler(
		&mockMessageStore{getErr: getErr},
		&mockBalanceStore{},
		&mockOperatorSender{},
	)
	payload := marshalPayload(t, uuid.New())
	err := h.HandleSendMessage(context.Background(), payload)
	if !errors.Is(err, getErr) {
		t.Fatalf("expected getErr, got %v", err)
	}
}

func TestWorkerHandler_AlreadyProcessed_Sent(t *testing.T) {
	msg := &domain.Message{ID: uuid.New(), Status: domain.StatusSent}
	h := worker.NewHandler(
		&mockMessageStore{getMsg: msg},
		&mockBalanceStore{},
		// operator should never be called for already-processed messages
		&mockOperatorSender{err: errors.New("should not be called")},
	)
	payload := marshalPayload(t, msg.ID)
	if err := h.HandleSendMessage(context.Background(), payload); err != nil {
		t.Fatalf("unexpected error for already-sent message: %v", err)
	}
}

func TestWorkerHandler_AlreadyProcessed_Failed(t *testing.T) {
	msg := &domain.Message{ID: uuid.New(), Status: domain.StatusFailed}
	h := worker.NewHandler(
		&mockMessageStore{getMsg: msg},
		&mockBalanceStore{},
		&mockOperatorSender{err: errors.New("should not be called")},
	)
	payload := marshalPayload(t, msg.ID)
	if err := h.HandleSendMessage(context.Background(), payload); err != nil {
		t.Fatalf("unexpected error for already-failed message: %v", err)
	}
}

func TestWorkerHandler_OperatorPermanentFailure(t *testing.T) {
	msg := &domain.Message{
		ID:          uuid.New(),
		UserID:      uuid.New(),
		PhoneNumber: "+1234567890",
		Text:        "hello",
		Status:      domain.StatusPending,
		Price:       10,
	}
	h := worker.NewHandler(
		&mockMessageStore{getMsg: msg, tx: &mockTx{}},
		&mockBalanceStore{},
		&mockOperatorSender{err: operator.ErrOperatorPermanentFailure},
	)
	payload := marshalPayload(t, msg.ID)
	// permanent failures are not retried — handler must return nil
	if err := h.HandleSendMessage(context.Background(), payload); err != nil {
		t.Fatalf("expected nil error for permanent failure, got %v", err)
	}
}

func TestWorkerHandler_OperatorTransientFailure(t *testing.T) {
	msg := &domain.Message{
		ID:          uuid.New(),
		UserID:      uuid.New(),
		PhoneNumber: "+1234567890",
		Text:        "hello",
		Status:      domain.StatusPending,
		Price:       10,
	}
	h := worker.NewHandler(
		&mockMessageStore{getMsg: msg},
		&mockBalanceStore{},
		&mockOperatorSender{err: operator.ErrOperatorUnavailable},
	)
	payload := marshalPayload(t, msg.ID)
	// transient failure: non-nil error signals Kafka to redeliver
	if err := h.HandleSendMessage(context.Background(), payload); err == nil {
		t.Fatal("expected error for transient failure (triggers Kafka retry)")
	}
}

func TestWorkerHandler_Success(t *testing.T) {
	msg := &domain.Message{
		ID:          uuid.New(),
		UserID:      uuid.New(),
		PhoneNumber: "+1234567890",
		Text:        "hello",
		Status:      domain.StatusPending,
		Price:       10,
	}
	h := worker.NewHandler(
		&mockMessageStore{getMsg: msg},
		&mockBalanceStore{},
		&mockOperatorSender{},
	)
	payload := marshalPayload(t, msg.ID)
	if err := h.HandleSendMessage(context.Background(), payload); err != nil {
		t.Fatalf("unexpected error on successful send: %v", err)
	}
}

func TestWorkerHandler_UpdateStatusError(t *testing.T) {
	updateErr := errors.New("db write failed")
	msg := &domain.Message{
		ID:          uuid.New(),
		UserID:      uuid.New(),
		PhoneNumber: "+1234567890",
		Text:        "hello",
		Status:      domain.StatusPending,
		Price:       10,
	}
	h := worker.NewHandler(
		&mockMessageStore{getMsg: msg, updateErr: updateErr},
		&mockBalanceStore{},
		&mockOperatorSender{},
	)
	payload := marshalPayload(t, msg.ID)
	if err := h.HandleSendMessage(context.Background(), payload); !errors.Is(err, updateErr) {
		t.Fatalf("expected updateErr, got %v", err)
	}
}
