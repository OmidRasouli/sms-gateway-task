package http

import (
	"context"
	"net/http"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/rs/zerolog/log"
)

type kafkaPinger interface {
	Ping(ctx context.Context) error
}

// requestLogger is a structured zerolog middleware for Gin that logs each
// request with method, path, status, latency, and client IP.
func requestLogger() gin.HandlerFunc {
	return func(c *gin.Context) {
		start := time.Now()
		path := c.Request.URL.Path

		c.Next()

		// Skip health-check endpoints to reduce noise.
		if path == "/livez" || path == "/readyz" {
			return
		}

		latency := time.Since(start)
		status := c.Writer.Status()

		event := log.Info()
		if status >= 500 {
			event = log.Error()
		} else if status >= 400 {
			event = log.Warn()
		}

		event.
			Str("method", c.Request.Method).
			Str("path", path).
			Int("status", status).
			Dur("latency", latency).
			Str("client_ip", c.ClientIP()).
			Str("user_id", c.GetHeader("X-User-ID")).
			Msg("request")
	}
}

func NewRouter(msgHandler *MessageHandler, balHandler *BalanceHandler, db *pgxpool.Pool, kafka kafkaPinger) *gin.Engine {
	gin.SetMode(gin.ReleaseMode)
	r := gin.New()
	r.Use(gin.Recovery())
	r.Use(requestLogger())
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
