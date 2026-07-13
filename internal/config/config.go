package config

import (
	"time"

	"github.com/kelseyhightower/envconfig"
)

type Config struct {
	Port                      string        `envconfig:"PORT" default:"8080"`
	DatabaseURL               string        `envconfig:"DATABASE_URL" required:"true"`
	KafkaBrokers              string        `envconfig:"KAFKA_BROKERS" default:"localhost:9092"`
	ExpressQueueConcurrency   int           `envconfig:"EXPRESS_CONCURRENCY" default:"15"`
	NormalQueueConcurrency    int           `envconfig:"NORMAL_CONCURRENCY" default:"10"`
	PriceCacheRefreshInterval time.Duration `envconfig:"PRICE_CACHE_REFRESH_INTERVAL" default:"5m"`
	MaxRetryAttempts          int           `envconfig:"MAX_RETRY_ATTEMPTS" default:"3"`
	LogLevel                  string        `envconfig:"LOG_LEVEL" default:"info"`
	LogFormat                 string        `envconfig:"LOG_FORMAT" default:"json"`
}

func Load() (*Config, error) {
	var cfg Config
	if err := envconfig.Process("", &cfg); err != nil {
		return nil, err
	}
	return &cfg, nil
}
