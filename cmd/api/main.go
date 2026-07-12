package main

import (
	"context"
	"log"
	"strings"

	"github.com/rs/zerolog"

	"github.com/OmidRasouli/sms-gateway-task/internal/config"
	httphandler "github.com/OmidRasouli/sms-gateway-task/internal/handler/http"
	"github.com/OmidRasouli/sms-gateway-task/internal/pricing"
	"github.com/OmidRasouli/sms-gateway-task/internal/queue"
	"github.com/OmidRasouli/sms-gateway-task/internal/repository/postgres"
	"github.com/OmidRasouli/sms-gateway-task/internal/service"
)

func main() {
	zerolog.SetGlobalLevel(zerolog.InfoLevel)

	cfg, err := config.Load()
	if err != nil {
		log.Fatalf("config: %v", err)
	}

	ctx := context.Background()
	pool, err := postgres.NewPool(ctx, cfg.DatabaseURL)
	if err != nil {
		log.Fatalf("postgres: %v", err)
	}
	defer pool.Close()

	balanceRepo := postgres.NewBalanceRepo(pool)
	messageRepo := postgres.NewMessageRepo(pool)

	priceRepo := postgres.NewPriceRepo(pool)
	priceCache := pricing.NewPriceCache(priceRepo)
	if err := priceCache.LoadFromDB(ctx); err != nil {
		log.Fatalf("price cache: initial load failed: %v", err)
	}
	priceCache.StartRefresh(ctx, cfg.PriceCacheRefreshInterval)

	brokers := strings.Split(cfg.KafkaBrokers, ",")
	queueClient := queue.NewClient(brokers)
	defer queueClient.Close()

	svc := service.NewMessageService(balanceRepo, messageRepo, queueClient, priceCache)

	msgHandler := httphandler.NewMessageHandler(svc, messageRepo)
	balHandler := httphandler.NewBalanceHandler(balanceRepo)

	router := httphandler.NewRouter(msgHandler, balHandler)

	log.Printf("api listening on :%s", cfg.Port)
	if err := router.Run(":" + cfg.Port); err != nil {
		log.Fatalf("server: %v", err)
	}
}
