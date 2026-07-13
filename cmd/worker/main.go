package main

import (
	"context"
	"os"
	"os/signal"
	"strings"
	"sync"
	"syscall"

	"github.com/rs/zerolog/log"
	"github.com/segmentio/kafka-go"

	"github.com/OmidRasouli/sms-gateway-task/internal/config"
	"github.com/OmidRasouli/sms-gateway-task/internal/logger"
	"github.com/OmidRasouli/sms-gateway-task/internal/operator"
	"github.com/OmidRasouli/sms-gateway-task/internal/queue"
	"github.com/OmidRasouli/sms-gateway-task/internal/repository/postgres"
	workerpkg "github.com/OmidRasouli/sms-gateway-task/internal/worker"
)

func newDLQWriter(brokers []string, topic string) *kafka.Writer {
	return &kafka.Writer{
		Addr:                   kafka.TCP(brokers...),
		Topic:                  topic,
		AllowAutoTopicCreation: true,
	}
}

const consumerGroup = "sms-worker"

func main() {
	cfg, err := config.Load()
	if err != nil {
		panic("config: " + err.Error())
	}

	logger.Setup(cfg.LogLevel, cfg.LogFormat)

	pool, err := postgres.NewPool(context.Background(), cfg.DatabaseURL)
	if err != nil {
		log.Fatal().Err(err).Msg("postgres: failed to connect")
	}
	defer pool.Close()

	messageRepo := postgres.NewMessageRepo(pool)
	balanceRepo := postgres.NewBalanceRepo(pool)
	mockOperator := operator.NewMockAdapter()
	handler := workerpkg.NewHandler(messageRepo, balanceRepo, mockOperator)

	brokers := strings.Split(cfg.KafkaBrokers, ",")

	expressDLQ := newDLQWriter(brokers, queue.TopicExpressDLQ)
	normalDLQ := newDLQWriter(brokers, queue.TopicNormalDLQ)
	defer expressDLQ.Close()
	defer normalDLQ.Close()

	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()

	// Express topic gets more consumer goroutines (higher concurrency) than
	// the normal topic — this is how the priority SLA requirement is honored.
	var wg sync.WaitGroup
	startConsumers(ctx, &wg, brokers, queue.TopicExpress, cfg.ExpressQueueConcurrency, cfg.MaxRetryAttempts, handler, expressDLQ)
	startConsumers(ctx, &wg, brokers, queue.TopicNormal, cfg.NormalQueueConcurrency, cfg.MaxRetryAttempts, handler, normalDLQ)

	log.Info().Msg("worker started")
	wg.Wait()
	log.Info().Msg("worker stopped")
}

// startConsumers launches `concurrency` goroutines that each own a separate
// kafka.Reader in the same consumer group. The Kafka group protocol assigns
// each partition to exactly one reader at a time, so messages within a
// partition (all messages for a given userID, by the producer's Hash key) are
// always consumed sequentially by a single goroutine — the per-user ordering
// guarantee introduced on the producer side is preserved here.
//
// Retry and dead-letter semantics: kafka-go's CommitMessages uses cumulative
// offset semantics — committing offset N implicitly marks every earlier offset
// on the same partition as consumed. Because of this, the naive strategy of
// "skip commit on failure so the message is redelivered" is broken: if msg1
// fails and msg2 (later offset) succeeds and is committed, msg1 is silently
// and permanently lost. To prevent this, each message is fully resolved
// (successfully processed or dead-lettered) before the goroutine advances to
// the next one. Specifically:
//
//   - If handleFn returns an error, it is retried up to maxRetries times with
//     exponential backoff. This blocks the goroutine on the failing offset,
//     preserving per-partition ordering.
//   - If all retries are exhausted, the message is published to the DLQ topic
//     (topic + "-dlq") with metadata headers, and only then is the original
//     offset committed so the partition can advance.
//   - If the DLQ publish itself fails, the goroutine blocks (logging loudly)
//     until it succeeds or the context is cancelled, rather than committing and
//     silently losing the message.
//   - If the context is cancelled mid-retry or while blocked on a failing DLQ,
//     the goroutine exits without committing. The broker will redeliver the
//     uncommitted message on the next startup.
//
// Ordering note: kafka-go uses eager (stop-the-world) rebalancing by default.
// During a rebalance no goroutine processes messages, so there is no window
// where two goroutines could race on the same partition.
func startConsumers(ctx context.Context, wg *sync.WaitGroup, brokers []string, topic string, concurrency int, maxRetries int, h *workerpkg.Handler, dlq dlqPublisher) {
	for range concurrency {
		wg.Add(1)
		go func() {
			defer wg.Done()
			r := kafka.NewReader(kafka.ReaderConfig{
				Brokers: brokers,
				Topic:   topic,
				GroupID: consumerGroup,
			})
			defer r.Close()

			for {
				msg, err := r.FetchMessage(ctx)
				if err != nil {
					if ctx.Err() != nil {
						return
					}
					log.Printf("consumer: fetch error topic=%s err=%v", topic, err)
					continue
				}

				ok := processWithRetryAndDLQ(ctx, msg, maxRetries, dlq, h.HandleSendMessage,
					func(ctx context.Context, m kafka.Message) error {
						return r.CommitMessages(ctx, m)
					},
				)
				if !ok {
					// ctx was cancelled mid-retry or while blocking on DLQ.
					// Exit without committing so the broker redelivers on restart.
					return
				}
			}
		}()
	}
}
