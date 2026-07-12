package http

import (
	"context"
	"net/http"
	"strconv"

	"github.com/OmidRasouli/sms-gateway-task/internal/domain"
	"github.com/gin-gonic/gin"
	"github.com/google/uuid"
)

type BalanceRepository interface {
	GetBalance(ctx context.Context, userID uuid.UUID) (int64, error)
	GetTransactions(ctx context.Context, userID uuid.UUID, limit, offset int) ([]domain.BalanceTransaction, error)
	Charge(ctx context.Context, userID uuid.UUID, amount int64) (int64, error)
}

type BalanceHandler struct {
	balances BalanceRepository
}

func NewBalanceHandler(balances BalanceRepository) *BalanceHandler {
	return &BalanceHandler{balances: balances}
}

func (h *BalanceHandler) Get(c *gin.Context) {
	userID, err := uuid.Parse(c.Param("userID"))
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "invalid userID"})
		return
	}

	amount, err := h.balances.GetBalance(c.Request.Context(), userID)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}
	c.JSON(http.StatusOK, gin.H{"user_id": userID, "amount": amount})
}

const (
	defaultLimit = 20
	maxLimit     = 100
)

func (h *BalanceHandler) Transactions(c *gin.Context) {
	userID, err := uuid.Parse(c.Param("userID"))
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "invalid userID"})
		return
	}

	limit := defaultLimit
	if v := c.Query("limit"); v != "" {
		limit, err = strconv.Atoi(v)
		if err != nil || limit <= 0 || limit > maxLimit {
			c.JSON(http.StatusBadRequest, gin.H{"error": "limit must be between 1 and 100"})
			return
		}
	}

	offset := 0
	if v := c.Query("offset"); v != "" {
		offset, err = strconv.Atoi(v)
		if err != nil || offset < 0 {
			c.JSON(http.StatusBadRequest, gin.H{"error": "offset must be a non-negative integer"})
			return
		}
	}

	transactions, err := h.balances.GetTransactions(c.Request.Context(), userID, limit, offset)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}
	if transactions == nil {
		transactions = []domain.BalanceTransaction{}
	}
	c.JSON(http.StatusOK, gin.H{"limit": limit, "offset": offset, "transactions": transactions})
}

type chargeRequest struct {
	UserID uuid.UUID `json:"user_id" binding:"required"`
	Amount int64     `json:"amount" binding:"required,gt=0"`
}

func (h *BalanceHandler) Charge(c *gin.Context) {
	var req chargeRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}
	newAmount, err := h.balances.Charge(c.Request.Context(), req.UserID, req.Amount)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}
	c.JSON(http.StatusOK, gin.H{"user_id": req.UserID, "amount": newAmount})
}
