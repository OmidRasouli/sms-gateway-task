package http

import (
	"context"
	"errors"
	"net/http"

	"github.com/gin-gonic/gin"
	"github.com/google/uuid"

	"github.com/OmidRasouli/sms-gateway-task/internal/domain"
)

type MessageService interface {
	SendMessage(ctx context.Context, userID uuid.UUID, phone, text string, msgType domain.MessageType) (*domain.Message, error)
}

type MessageRepository interface {
	ListByUser(ctx context.Context, userID uuid.UUID, limit, offset int) ([]domain.Message, error)
	GetByID(ctx context.Context, id uuid.UUID) (*domain.Message, error)
}

type MessageHandler struct {
	svc      MessageService
	messages MessageRepository
}

func NewMessageHandler(svc MessageService, messages MessageRepository) *MessageHandler {
	return &MessageHandler{svc: svc, messages: messages}
}

type sendMessageRequest struct {
	PhoneNumber string             `json:"phone_number" binding:"required"`
	Text        string             `json:"text" binding:"required"`
	Type        domain.MessageType `json:"type" binding:"required,oneof=normal express"`
}

// Send handles POST /api/v1/messages. No auth exists per challenge scope,
// so the caller identifies itself via the X-User-ID header.
func (h *MessageHandler) Send(c *gin.Context) {
	userID, err := uuid.Parse(c.GetHeader("X-User-ID"))
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "missing or invalid X-User-ID header"})
		return
	}

	var req sendMessageRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}

	msg, err := h.svc.SendMessage(c.Request.Context(), userID, req.PhoneNumber, req.Text, req.Type)
	if errors.Is(err, domain.ErrInsufficientBalance) {
		c.JSON(http.StatusPaymentRequired, gin.H{"error": "insufficient balance"})
		return
	}
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}

	// 202: the message is durably queued, not yet necessarily delivered —
	// matches the async delivery model in the HLD.
	c.JSON(http.StatusAccepted, msg)
}

// List handles GET /api/v1/messages/:userID. It returns the last 50 messages for the user.
func (h *MessageHandler) List(c *gin.Context) {
	userID, err := uuid.Parse(c.Param("userID"))
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "invalid userID"})
		return
	}

	msgs, err := h.messages.ListByUser(c.Request.Context(), userID, 50, 0)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}
	if msgs == nil {
		msgs = []domain.Message{}
	}
	c.JSON(http.StatusOK, msgs)
}

func (h *MessageHandler) Get(c *gin.Context) {
	id, err := uuid.Parse(c.Param("id"))
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "invalid message id"})
		return
	}

	msg, err := h.messages.GetByID(c.Request.Context(), id)
	if errors.Is(err, domain.ErrNotFound) {
		c.JSON(http.StatusNotFound, gin.H{"error": "message not found"})
		return
	}
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}
	c.JSON(http.StatusOK, msg)
}
