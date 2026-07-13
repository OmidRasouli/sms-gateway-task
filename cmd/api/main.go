package main

import (
	"context"
	"strings"

	"github.com/rs/zerolog/log"

	"github.com/OmidRasouli/sms-gateway-task/internal/config"
	httphandler "github.com/OmidRasouli/sms-gateway-task/internal/handler/http"
	"github.com/OmidRasouli/sms-gateway-task/internal/logger"
	"github.com/OmidRasouli/sms-gateway-task/internal/pricing"
	"github.com/OmidRasouli/sms-gateway-task/internal/queue"
	"github.com/OmidRasouli/sms-gateway-task/internal/repository/postgres"
	"github.com/OmidRasouli/sms-gateway-task/internal/service"
)

func main() {
	cfg, err := config.Load()
	if err != nil {
		// Use stdlib fatal before logger is set up.
		panic("config: " + err.Error())
	}

	logger.Setup(cfg.LogLevel, cfg.LogFormat)

	ctx := context.Background()
	pool, err := postgres.NewPool(ctx, cfg.DatabaseURL)
	if err != nil {
		log.Fatal().Err(err).Msg("postgres: failed to connect")
	}
	defer pool.Close()

	balanceRepo := postgres.NewBalanceRepo(pool)
	messageRepo := postgres.NewMessageRepo(pool)

	priceRepo := postgres.NewPriceRepo(pool)
	priceCache := pricing.NewPriceCache(priceRepo)
	if err := priceCache.LoadFromDB(ctx); err != nil {
		log.Fatal().Err(err).Msg("price cache: initial load failed")
	}
	priceCache.StartRefresh(ctx, cfg.PriceCacheRefreshInterval)

	brokers := strings.Split(cfg.KafkaBrokers, ",")
	queueClient := queue.NewClient(brokers)
	defer queueClient.Close()

	svc := service.NewMessageService(balanceRepo, messageRepo, queueClient, priceCache)

	msgHandler := httphandler.NewMessageHandler(svc, messageRepo)
	balHandler := httphandler.NewBalanceHandler(balanceRepo)

	router := httphandler.NewRouter(msgHandler, balHandler, pool, queueClient)

	log.Info().Str("port", cfg.Port).Msg("api listening")
	if err := router.Run(":" + cfg.Port); err != nil {
		log.Fatal().Err(err).Msg("server: failed")
	}
}
