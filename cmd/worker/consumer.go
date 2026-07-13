package main

import (
	"context"
	"math/rand"
	"strconv"
	"time"

	"github.com/rs/zerolog/log"
	"github.com/segmentio/kafka-go"
)

// dlqPublisher abstracts the kafka.Writer used for dead-letter publishing.
// *kafka.Writer satisfies this interface.
type dlqPublisher interface {
	WriteMessages(ctx context.Context, msgs ...kafka.Message) error
}

// backoffFunc computes the sleep duration before retry number n (1-based).
// It is a package-level variable so tests can override it to avoid slow sleeps.
var backoffFunc = retryBackoff

// dlqRetryDelay is the pause between consecutive DLQ publish attempts when the
// DLQ broker is unavailable. It is a package-level variable so tests can
// override it without waiting 5 seconds per iteration.
var dlqRetryDelay = 5 * time.Second

// processWithRetryAndDLQ attempts to process msg using handleFn, retrying up
// to maxAttempts times (including the first attempt) with exponential backoff.
// It guarantees one of these outcomes before returning:
//
//  1. handleFn succeeds → commitFn is called to advance the committed offset;
//     returns true.
//
//  2. All attempts exhausted → msg is published to the DLQ via dlq with
//     metadata headers, then commitFn is called; returns true.
//
//  3. ctx is cancelled during a backoff sleep or while blocking on a failing
//     DLQ → returns false without committing. The caller must exit its consumer
//     loop. Because no later message has been committed by this goroutine yet
//     in the current fetch cycle, the broker will redeliver msg on restart.
//
//  4. DLQ publish fails → the function blocks in a 5-second retry loop, logging
//     loudly on each failure, until the DLQ accepts the message or ctx is
//     cancelled (case 3). Committing the original offset before the DLQ write
//     is durable would permanently lose the message.
//
// Callers must never skip ahead to the next message without this function
// returning true. kafka-go uses cumulative offset commits: committing offset N
// implicitly marks every earlier offset on that partition as consumed. Skipping
// a failed message and committing a later one silently loses the failed message
// — exactly the bug this retry-then-DLQ approach is designed to prevent.
func processWithRetryAndDLQ(
	ctx context.Context,
	msg kafka.Message,
	maxAttempts int,
	dlq dlqPublisher,
	handleFn func(context.Context, []byte) error,
	commitFn func(context.Context, kafka.Message) error,
) bool {
	var lastErr error

	for attempt := 1; attempt <= maxAttempts; attempt++ {
		// Sleep before every attempt after the first.
		if attempt > 1 {
			select {
			case <-ctx.Done():
				log.Warn().Str("topic", msg.Topic).Int("partition", msg.Partition).Int64("offset", msg.Offset).Msg("consumer: ctx cancelled during retry backoff")
				return false
			case <-time.After(backoffFunc(attempt - 1)):
			}
		}

		err := handleFn(ctx, msg.Value)
		if err == nil {
			if cerr := commitFn(ctx, msg); cerr != nil {
				log.Error().Err(cerr).Str("topic", msg.Topic).Int("partition", msg.Partition).Int64("offset", msg.Offset).Msg("consumer: commit error")
			}
			return true
		}

		// If the context was cancelled, do not retry and do not commit.
		if ctx.Err() != nil {
			log.Warn().Str("topic", msg.Topic).Int("partition", msg.Partition).Int64("offset", msg.Offset).Msg("consumer: ctx cancelled after handler error")
			return false
		}

		lastErr = err
		log.Warn().Err(err).Int("attempt", attempt).Int("max_attempts", maxAttempts).Str("topic", msg.Topic).Int("partition", msg.Partition).Int64("offset", msg.Offset).Msg("consumer: transient failure")
	}

	// All attempts exhausted — publish to the dead-letter topic.
	log.Error().Err(lastErr).Str("topic", msg.Topic).Int("partition", msg.Partition).Int64("offset", msg.Offset).Msg("consumer: retries exhausted, routing to DLQ")

	dlqMsg := buildDLQMessage(msg, lastErr)

	// Block until the DLQ write succeeds or ctx is cancelled.
	// We must not commit the original offset before the DLQ message is
	// durably enqueued: an uncommitted offset causes the broker to redeliver
	// on restart, which is our safety net if everything else fails.
	for {
		if err := dlq.WriteMessages(ctx, dlqMsg); err != nil {
			if ctx.Err() != nil {
				log.Warn().Str("topic", msg.Topic).Int("partition", msg.Partition).Int64("offset", msg.Offset).Msg("consumer: ctx cancelled while publishing to DLQ")
				return false
			}
			log.Error().Err(err).Str("topic", msg.Topic).Int("partition", msg.Partition).Int64("offset", msg.Offset).Msg("consumer: DLQ PUBLISH FAILED (DATA LOSS RISK) — will retry in 5s")
			select {
			case <-ctx.Done():
				return false
			case <-time.After(dlqRetryDelay):
			}
			continue
		}
		break
	}

	log.Info().Str("topic", msg.Topic).Int("partition", msg.Partition).Int64("offset", msg.Offset).Msg("consumer: message routed to DLQ")

	if cerr := commitFn(ctx, msg); cerr != nil {
		log.Error().Err(cerr).Str("topic", msg.Topic).Int("partition", msg.Partition).Int64("offset", msg.Offset).Msg("consumer: commit error after DLQ publish")
	}
	return true
}

// buildDLQMessage wraps the original message for the dead-letter topic.
// The original value is preserved intact; metadata is carried in headers so
// downstream consumers can inspect or replay failed messages.
func buildDLQMessage(msg kafka.Message, cause error) kafka.Message {
	errMsg := ""
	if cause != nil {
		errMsg = cause.Error()
	}
	return kafka.Message{
		Key:   msg.Key,
		Value: msg.Value,
		Headers: []kafka.Header{
			{Key: "x-original-topic", Value: []byte(msg.Topic)},
			{Key: "x-original-partition", Value: []byte(strconv.Itoa(msg.Partition))},
			{Key: "x-original-offset", Value: []byte(strconv.FormatInt(msg.Offset, 10))},
			{Key: "x-error", Value: []byte(errMsg)},
			{Key: "x-timestamp", Value: []byte(time.Now().UTC().Format(time.RFC3339))},
		},
	}
}

// retryBackoff returns the sleep duration before retry number n (1-based).
// It uses exponential backoff (base 200ms, cap 5s) with up to 50% random
// jitter to spread out concurrent goroutine retries (thundering-herd mitigation).
func retryBackoff(n int) time.Duration {
	const base = 200 * time.Millisecond
	const maxBackoff = 5 * time.Second

	b := base * (1 << uint(n-1))
	if b > maxBackoff {
		b = maxBackoff
	}
	// Jitter: uniform random in [0, b/2).
	jitter := time.Duration(rand.Int63n(int64(b) / 2))
	return b + jitter
}
