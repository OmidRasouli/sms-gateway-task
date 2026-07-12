package http_test

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/gin-gonic/gin"
	"github.com/google/uuid"

	"github.com/OmidRasouli/sms-gateway-task/internal/domain"
	httphandler "github.com/OmidRasouli/sms-gateway-task/internal/handler/http"
)

type mockBalanceRepository struct {
	balance      int64
	newBalance   int64
	transactions []domain.BalanceTransaction
	getErr       error
	chargeErr    error
	txErr        error
}

func (m *mockBalanceRepository) GetBalance(_ context.Context, _ uuid.UUID) (int64, error) {
	return m.balance, m.getErr
}

func (m *mockBalanceRepository) GetTransactions(_ context.Context, _ uuid.UUID, _, _ int) ([]domain.BalanceTransaction, error) {
	return m.transactions, m.txErr
}

func (m *mockBalanceRepository) Charge(_ context.Context, _ uuid.UUID, _ int64) (int64, error) {
	return m.newBalance, m.chargeErr
}

func newBalanceRouter(repo *mockBalanceRepository) *gin.Engine {
	r := gin.New()
	h := httphandler.NewBalanceHandler(repo)
	r.GET("/balance/:userID", h.Get)
	r.GET("/transactions/:userID", h.Transactions)
	r.POST("/balance/charge", h.Charge)
	return r
}

// --- Get ---

func TestBalanceHandler_Get_InvalidUserID(t *testing.T) {
	r := newBalanceRouter(&mockBalanceRepository{})
	w := httptest.NewRecorder()
	req, _ := http.NewRequest(http.MethodGet, "/balance/not-a-uuid", nil)
	r.ServeHTTP(w, req)
	if w.Code != http.StatusBadRequest {
		t.Errorf("expected 400, got %d", w.Code)
	}
}

func TestBalanceHandler_Get_Success(t *testing.T) {
	r := newBalanceRouter(&mockBalanceRepository{balance: 100})
	w := httptest.NewRecorder()
	req, _ := http.NewRequest(http.MethodGet, "/balance/"+uuid.New().String(), nil)
	r.ServeHTTP(w, req)
	if w.Code != http.StatusOK {
		t.Errorf("expected 200, got %d", w.Code)
	}
	var resp map[string]interface{}
	if err := json.NewDecoder(w.Body).Decode(&resp); err != nil {
		t.Fatalf("failed to decode response: %v", err)
	}
	if resp["amount"] != float64(100) {
		t.Errorf("expected amount 100, got %v", resp["amount"])
	}
}

// --- Transactions ---

func TestBalanceHandler_Transactions_InvalidUserID(t *testing.T) {
	r := newBalanceRouter(&mockBalanceRepository{})
	w := httptest.NewRecorder()
	req, _ := http.NewRequest(http.MethodGet, "/transactions/not-a-uuid", nil)
	r.ServeHTTP(w, req)
	if w.Code != http.StatusBadRequest {
		t.Errorf("expected 400, got %d", w.Code)
	}
}

func TestBalanceHandler_Transactions_InvalidLimit(t *testing.T) {
	r := newBalanceRouter(&mockBalanceRepository{})
	w := httptest.NewRecorder()
	req, _ := http.NewRequest(http.MethodGet, "/transactions/"+uuid.New().String()+"?limit=0", nil)
	r.ServeHTTP(w, req)
	if w.Code != http.StatusBadRequest {
		t.Errorf("expected 400, got %d", w.Code)
	}
}

func TestBalanceHandler_Transactions_ExceedMaxLimit(t *testing.T) {
	r := newBalanceRouter(&mockBalanceRepository{})
	w := httptest.NewRecorder()
	req, _ := http.NewRequest(http.MethodGet, "/transactions/"+uuid.New().String()+"?limit=999", nil)
	r.ServeHTTP(w, req)
	if w.Code != http.StatusBadRequest {
		t.Errorf("expected 400, got %d", w.Code)
	}
}

func TestBalanceHandler_Transactions_Success(t *testing.T) {
	msgID := uuid.New()
	txs := []domain.BalanceTransaction{{ID: uuid.New(), Amount: -10, MessageID: &msgID}}
	r := newBalanceRouter(&mockBalanceRepository{transactions: txs})
	w := httptest.NewRecorder()
	req, _ := http.NewRequest(http.MethodGet, "/transactions/"+uuid.New().String(), nil)
	r.ServeHTTP(w, req)
	if w.Code != http.StatusOK {
		t.Errorf("expected 200, got %d", w.Code)
	}
	var resp map[string]interface{}
	if err := json.NewDecoder(w.Body).Decode(&resp); err != nil {
		t.Fatalf("failed to decode response: %v", err)
	}
	transactions, ok := resp["transactions"].([]interface{})
	if !ok || len(transactions) != 1 {
		t.Errorf("expected 1 transaction, got %v", resp["transactions"])
	}
}

// --- Charge ---

func TestBalanceHandler_Charge_InvalidBody(t *testing.T) {
	r := newBalanceRouter(&mockBalanceRepository{})
	w := httptest.NewRecorder()
	req, _ := http.NewRequest(http.MethodPost, "/balance/charge", bytes.NewBufferString(`{}`))
	req.Header.Set("Content-Type", "application/json")
	r.ServeHTTP(w, req)
	if w.Code != http.StatusBadRequest {
		t.Errorf("expected 400, got %d", w.Code)
	}
}

func TestBalanceHandler_Charge_NegativeAmount(t *testing.T) {
	r := newBalanceRouter(&mockBalanceRepository{})
	w := httptest.NewRecorder()
	body, _ := json.Marshal(map[string]interface{}{"user_id": uuid.New().String(), "amount": -10})
	req, _ := http.NewRequest(http.MethodPost, "/balance/charge", bytes.NewBuffer(body))
	req.Header.Set("Content-Type", "application/json")
	r.ServeHTTP(w, req)
	if w.Code != http.StatusBadRequest {
		t.Errorf("expected 400, got %d", w.Code)
	}
}

func TestBalanceHandler_Charge_Success(t *testing.T) {
	r := newBalanceRouter(&mockBalanceRepository{newBalance: 150})
	w := httptest.NewRecorder()
	body, _ := json.Marshal(map[string]interface{}{"user_id": uuid.New().String(), "amount": 50})
	req, _ := http.NewRequest(http.MethodPost, "/balance/charge", bytes.NewBuffer(body))
	req.Header.Set("Content-Type", "application/json")
	r.ServeHTTP(w, req)
	if w.Code != http.StatusOK {
		t.Errorf("expected 200, got %d", w.Code)
	}
	var resp map[string]interface{}
	if err := json.NewDecoder(w.Body).Decode(&resp); err != nil {
		t.Fatalf("failed to decode response: %v", err)
	}
	if resp["amount"] != float64(150) {
		t.Errorf("expected amount 150, got %v", resp["amount"])
	}
}
