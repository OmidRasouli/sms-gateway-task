package logger

import (
	"os"
	"time"

	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
)

// Setup configures the global zerolog logger.
// level accepts zerolog level names: trace, debug, info, warn, error, fatal, panic.
// format accepts "json" (default, structured) or "pretty" (human-readable console output).
func Setup(level, format string) {
	lvl, err := zerolog.ParseLevel(level)
	if err != nil {
		lvl = zerolog.InfoLevel
	}
	zerolog.SetGlobalLevel(lvl)

	zerolog.TimeFieldFormat = time.RFC3339

	if format == "pretty" {
		log.Logger = log.Output(zerolog.ConsoleWriter{Out: os.Stderr, TimeFormat: time.RFC3339})
	} else {
		log.Logger = zerolog.New(os.Stderr).With().Timestamp().Logger()
	}
}
