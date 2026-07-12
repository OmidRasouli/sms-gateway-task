package http_test

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/gin-gonic/gin"
	"github.com/google/uuid"

	"github.com/OmidRasouli/sms-gateway-task/internal/domain"
	httphandler "github.com/OmidRasouli/sms-gateway-task/internal/handler/http"
)

func init() {
	gin.SetMode(gin.TestMode)
}

// mockMessageService

type mockMessageService struct {
	msg *domain.Message
	err error
}

func (m *mockMessageService) SendMessage(_ context.Context, _ uuid.UUID, _, _ string, _ domain.MessageType) (*domain.Message, error) {
	return m.msg, m.err
}

// mockMessageRepo

type mockMessageRepo struct {
	messages []domain.Message
	msg      *domain.Message
	listErr  error
	getErr   error
}

func (m *mockMessageRepo) ListByUser(_ context.Context, _ uuid.UUID, _, _ int) ([]domain.Message, error) {
	return m.messages, m.listErr
}

func (m *mockMessageRepo) GetByID(_ context.Context, _ uuid.UUID) (*domain.Message, error) {
	return m.msg, m.getErr
}

func newMessageRouter(svc *mockMessageService, repo *mockMessageRepo) *gin.Engine {
	r := gin.New()
	h := httphandler.NewMessageHandler(svc, repo)
	r.POST("/api/v1/messages", h.Send)
	r.GET("/api/v1/messages/:userID", h.List)
	r.GET("/api/v1/messages/:userID/:id", h.Get)
	return r
}

// --- Send ---

func TestMessageHandler_Send_MissingUserID(t *testing.T) {
	r := newMessageRouter(&mockMessageService{}, &mockMessageRepo{})
	w := httptest.NewRecorder()
	body := `{"phone_number":"+1234567890","text":"hello","type":"normal"}`
	req, _ := http.NewRequest(http.MethodPost, "/api/v1/messages", bytes.NewBufferString(body))
	req.Header.Set("Content-Type", "application/json")
	r.ServeHTTP(w, req)
	if w.Code != http.StatusBadRequest {
		t.Errorf("expected 400, got %d", w.Code)
	}
}

func TestMessageHandler_Send_InvalidBody(t *testing.T) {
	r := newMessageRouter(&mockMessageService{}, &mockMessageRepo{})
	w := httptest.NewRecorder()
	req, _ := http.NewRequest(http.MethodPost, "/api/v1/messages", bytes.NewBufferString(`{}`))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("X-User-ID", uuid.New().String())
	r.ServeHTTP(w, req)
	if w.Code != http.StatusBadRequest {
		t.Errorf("expected 400, got %d", w.Code)
	}
}

func TestMessageHandler_Send_InsufficientBalance(t *testing.T) {
	svc := &mockMessageService{err: domain.ErrInsufficientBalance}
	r := newMessageRouter(svc, &mockMessageRepo{})
	w := httptest.NewRecorder()
	body := `{"phone_number":"+1234567890","text":"hello","type":"normal"}`
	req, _ := http.NewRequest(http.MethodPost, "/api/v1/messages", bytes.NewBufferString(body))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("X-User-ID", uuid.New().String())
	r.ServeHTTP(w, req)
	if w.Code != http.StatusPaymentRequired {
		t.Errorf("expected 402, got %d", w.Code)
	}
}

func TestMessageHandler_Send_InternalError(t *testing.T) {
	svc := &mockMessageService{err: errors.New("db down")}
	r := newMessageRouter(svc, &mockMessageRepo{})
	w := httptest.NewRecorder()
	body := `{"phone_number":"+1234567890","text":"hello","type":"normal"}`
	req, _ := http.NewRequest(http.MethodPost, "/api/v1/messages", bytes.NewBufferString(body))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("X-User-ID", uuid.New().String())
	r.ServeHTTP(w, req)
	if w.Code != http.StatusInternalServerError {
		t.Errorf("expected 500, got %d", w.Code)
	}
}

func TestMessageHandler_Send_Success(t *testing.T) {
	userID := uuid.New()
	msg := &domain.Message{
		ID:          uuid.New(),
		UserID:      userID,
		PhoneNumber: "+1234567890",
		Text:        "hello",
		Type:        domain.MessageTypeNormal,
		Price:       10,
		Status:      domain.StatusPending,
	}
	svc := &mockMessageService{msg: msg}
	r := newMessageRouter(svc, &mockMessageRepo{})
	w := httptest.NewRecorder()
	body := `{"phone_number":"+1234567890","text":"hello","type":"normal"}`
	req, _ := http.NewRequest(http.MethodPost, "/api/v1/messages", bytes.NewBufferString(body))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("X-User-ID", userID.String())
	r.ServeHTTP(w, req)
	if w.Code != http.StatusAccepted {
		t.Errorf("expected 202, got %d", w.Code)
	}
	var resp domain.Message
	if err := json.NewDecoder(w.Body).Decode(&resp); err != nil {
		t.Fatalf("failed to decode response: %v", err)
	}
	if resp.ID != msg.ID {
		t.Errorf("expected message ID %s, got %s", msg.ID, resp.ID)
	}
}

// --- List ---

func TestMessageHandler_List_InvalidUserID(t *testing.T) {
	r := newMessageRouter(&mockMessageService{}, &mockMessageRepo{})
	w := httptest.NewRecorder()
	req, _ := http.NewRequest(http.MethodGet, "/api/v1/messages/not-a-uuid", nil)
	r.ServeHTTP(w, req)
	if w.Code != http.StatusBadRequest {
		t.Errorf("expected 400, got %d", w.Code)
	}
}

func TestMessageHandler_List_Success(t *testing.T) {
	msgs := []domain.Message{
		{ID: uuid.New(), Type: domain.MessageTypeNormal, Status: domain.StatusSent},
	}
	r := newMessageRouter(&mockMessageService{}, &mockMessageRepo{messages: msgs})
	w := httptest.NewRecorder()
	req, _ := http.NewRequest(http.MethodGet, "/api/v1/messages/"+uuid.New().String(), nil)
	r.ServeHTTP(w, req)
	if w.Code != http.StatusOK {
		t.Errorf("expected 200, got %d", w.Code)
	}
	var resp []domain.Message
	if err := json.NewDecoder(w.Body).Decode(&resp); err != nil {
		t.Fatalf("failed to decode response: %v", err)
	}
	if len(resp) != 1 {
		t.Errorf("expected 1 message, got %d", len(resp))
	}
}

// --- Get ---

func TestMessageHandler_Get_InvalidID(t *testing.T) {
	r := newMessageRouter(&mockMessageService{}, &mockMessageRepo{})
	w := httptest.NewRecorder()
	req, _ := http.NewRequest(http.MethodGet, "/api/v1/messages/"+uuid.New().String()+"/not-a-uuid", nil)
	r.ServeHTTP(w, req)
	if w.Code != http.StatusBadRequest {
		t.Errorf("expected 400, got %d", w.Code)
	}
}

func TestMessageHandler_Get_NotFound(t *testing.T) {
	r := newMessageRouter(&mockMessageService{}, &mockMessageRepo{getErr: domain.ErrNotFound})
	w := httptest.NewRecorder()
	req, _ := http.NewRequest(http.MethodGet, "/api/v1/messages/"+uuid.New().String()+"/"+uuid.New().String(), nil)
	r.ServeHTTP(w, req)
	if w.Code != http.StatusNotFound {
		t.Errorf("expected 404, got %d", w.Code)
	}
}

func TestMessageHandler_Get_Success(t *testing.T) {
	msg := &domain.Message{ID: uuid.New(), Type: domain.MessageTypeExpress, Status: domain.StatusSent}
	r := newMessageRouter(&mockMessageService{}, &mockMessageRepo{msg: msg})
	w := httptest.NewRecorder()
	req, _ := http.NewRequest(http.MethodGet, "/api/v1/messages/"+uuid.New().String()+"/"+msg.ID.String(), nil)
	r.ServeHTTP(w, req)
	if w.Code != http.StatusOK {
		t.Errorf("expected 200, got %d", w.Code)
	}
	var resp domain.Message
	if err := json.NewDecoder(w.Body).Decode(&resp); err != nil {
		t.Fatalf("failed to decode response: %v", err)
	}
	if resp.ID != msg.ID {
		t.Errorf("expected message ID %s, got %s", msg.ID, resp.ID)
	}
}
