package http

import (
	"context"
	"net/http"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/jackc/pgx/v5/pgxpool"
)

type kafkaPinger interface {
	Ping(ctx context.Context) error
}

func NewRouter(msgHandler *MessageHandler, balHandler *BalanceHandler, db *pgxpool.Pool, kafka kafkaPinger) *gin.Engine {
	r := gin.New()
	r.Use(gin.Recovery())
	r.Use(gin.LoggerWithConfig(gin.LoggerConfig{
		SkipPaths: []string{"/livez", "/readyz"},
	}))
	// Liveness: process is running
	r.GET("/livez", func(c *gin.Context) { c.Status(http.StatusOK) })

	// Readiness: dependencies (DB + Kafka) are reachable
	r.GET("/readyz", func(c *gin.Context) {
		ctx, cancel := context.WithTimeout(c.Request.Context(), 2*time.Second)
		defer cancel()
		if err := db.Ping(ctx); err != nil {
			c.JSON(http.StatusServiceUnavailable, gin.H{"error": "database unavailable"})
			return
		}
		if err := kafka.Ping(ctx); err != nil {
			c.JSON(http.StatusServiceUnavailable, gin.H{"error": "kafka unavailable"})
			return
		}
		c.Status(http.StatusOK)
	})

	v1 := r.Group("/api/v1")
	{
		v1.POST("/messages", msgHandler.Send)
		v1.GET("/messages/:userID", msgHandler.List)
		v1.GET("/messages/:userID/:id", msgHandler.Get)

		v1.GET("/balance/:userID", balHandler.Get)
		v1.GET("/transactions/:userID", balHandler.Transactions)
		v1.POST("/balance/charge", balHandler.Charge)
	}

	return r
}
