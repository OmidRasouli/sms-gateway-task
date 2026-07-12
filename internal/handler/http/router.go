package http

import "github.com/gin-gonic/gin"

func NewRouter(msgHandler *MessageHandler, balHandler *BalanceHandler) *gin.Engine {
	r := gin.Default()

	r.GET("/healthz", func(c *gin.Context) { c.Status(200) })

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
