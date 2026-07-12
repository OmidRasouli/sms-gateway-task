package main

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/segmentio/kafka-go"
)

// Override backoffFunc for all tests in this file so retries don't sleep.
func init() {
	backoffFunc = func(int) time.Duration { return time.Millisecond }
}

// --- fakes ---

// fakeDLQ records messages written to it and can be configured to fail.
type fakeDLQ struct {
	messages  []kafka.Message
	failUntil int // fail the first failUntil calls
	calls     int
}

func (f *fakeDLQ) WriteMessages(_ context.Context, msgs ...kafka.Message) error {
	f.calls++
	if f.calls <= f.failUntil {
		return errors.New("kafka: leader not available")
	}
	f.messages = append(f.messages, msgs...)
	return nil
}

// commitRecorder tracks whether commitFn was called.
type commitRecorder struct{ called bool }

func (c *commitRecorder) fn(_ context.Context, _ kafka.Message) error {
	c.called = true
	return nil
}

// --- helpers ---

func testMsg() kafka.Message {
	return kafka.Message{
		Topic:     "sms.express",
		Partition: 0,
		Offset:    42,
		Key:       []byte("user-key"),
		Value:     []byte(`{"message_id":"aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee"}`),
	}
}

func headersMap(msg kafka.Message) map[string]string {
	m := make(map[string]string, len(msg.Headers))
	for _, h := range msg.Headers {
		m[h.Key] = string(h.Value)
	}
	return m
}

// --- tests ---

// TestProcess_SuccessFirstAttempt verifies that on success the offset is
// committed and nothing is written to the DLQ.
func TestProcess_SuccessFirstAttempt(t *testing.T) {
	dlq := &fakeDLQ{}
	commit := &commitRecorder{}
	msg := testMsg()

	got := processWithRetryAndDLQ(
		context.Background(), msg, 3, dlq,
		func(_ context.Context, _ []byte) error { return nil },
		commit.fn,
	)

	if !got {
		t.Fatal("expected true (success)")
	}
	if len(dlq.messages) != 0 {
		t.Fatalf("expected 0 DLQ messages, got %d", len(dlq.messages))
	}
	if !commit.called {
		t.Fatal("expected commit to be called on success")
	}
}

// TestProcess_SuccessOnRetry verifies that a transient failure followed by
// success commits the offset and writes nothing to the DLQ.
func TestProcess_SuccessOnRetry(t *testing.T) {
	dlq := &fakeDLQ{}
	commit := &commitRecorder{}
	msg := testMsg()
	attempt := 0

	got := processWithRetryAndDLQ(
		context.Background(), msg, 3, dlq,
		func(_ context.Context, _ []byte) error {
			attempt++
			if attempt < 2 {
				return errors.New("transient")
			}
			return nil
		},
		commit.fn,
	)

	if !got {
		t.Fatal("expected true")
	}
	if attempt != 2 {
		t.Fatalf("expected 2 handler calls, got %d", attempt)
	}
	if len(dlq.messages) != 0 {
		t.Fatal("expected no DLQ message when retry succeeds")
	}
	if !commit.called {
		t.Fatal("expected commit after successful retry")
	}
}

// TestProcess_AllRetriesExhausted_SentToDLQAndCommitted is the key regression
// test for the cumulative-offset bug. When all attempts fail the message must
// end up on the DLQ, the original offset must be committed so the partition
// advances, and the caller receives true (continue consuming).
func TestProcess_AllRetriesExhausted_SentToDLQAndCommitted(t *testing.T) {
	dlq := &fakeDLQ{}
	commit := &commitRecorder{}
	msg := testMsg()
	handleErr := errors.New("operator unavailable")
	callCount := 0

	// maxAttempts=1 avoids backoff sleeps while still exercising the DLQ path.
	got := processWithRetryAndDLQ(
		context.Background(), msg, 1, dlq,
		func(_ context.Context, _ []byte) error {
			callCount++
			return handleErr
		},
		commit.fn,
	)

	if !got {
		t.Fatal("expected true after successful DLQ publish")
	}
	if callCount != 1 {
		t.Fatalf("expected exactly 1 handler call with maxAttempts=1, got %d", callCount)
	}

	// DLQ must have received exactly one message.
	if len(dlq.messages) != 1 {
		t.Fatalf("expected 1 DLQ message, got %d", len(dlq.messages))
	}

	// Original value preserved.
	dlqMsg := dlq.messages[0]
	if string(dlqMsg.Value) != string(msg.Value) {
		t.Errorf("DLQ message value mismatch: got %q, want %q", dlqMsg.Value, msg.Value)
	}

	// Verify required metadata headers.
	headers := headersMap(dlqMsg)
	if headers["x-original-topic"] != "sms.express" {
		t.Errorf("x-original-topic: got %q, want %q", headers["x-original-topic"], "sms.express")
	}
	if headers["x-original-partition"] != "0" {
		t.Errorf("x-original-partition: got %q, want %q", headers["x-original-partition"], "0")
	}
	if headers["x-original-offset"] != "42" {
		t.Errorf("x-original-offset: got %q, want %q", headers["x-original-offset"], "42")
	}
	if headers["x-error"] != "operator unavailable" {
		t.Errorf("x-error: got %q, want %q", headers["x-error"], "operator unavailable")
	}
	if headers["x-timestamp"] == "" {
		t.Error("x-timestamp header must not be empty")
	}

	// Crucially: offset must be committed so the partition advances past the
	// failed message — this is what prevents the cumulative-commit silent loss.
	if !commit.called {
		t.Fatal("offset must be committed after DLQ publish so the partition advances")
	}
}

// TestProcess_MultipleAttemptsThenDLQ verifies the retry count with maxAttempts=3.
func TestProcess_MultipleAttemptsThenDLQ(t *testing.T) {
	dlq := &fakeDLQ{}
	commit := &commitRecorder{}
	callCount := 0

	processWithRetryAndDLQ(
		context.Background(), testMsg(), 3, dlq,
		func(_ context.Context, _ []byte) error {
			callCount++
			return errors.New("fail")
		},
		commit.fn,
	)

	if callCount != 3 {
		t.Fatalf("expected 3 handler calls for maxAttempts=3, got %d", callCount)
	}
	if len(dlq.messages) != 1 {
		t.Fatalf("expected 1 DLQ message, got %d", len(dlq.messages))
	}
	if !commit.called {
		t.Fatal("expected commit after DLQ publish")
	}
}

// TestProcess_CtxCancelledDuringRetry verifies that when the context is
// cancelled mid-retry the function returns false without committing or
// publishing to the DLQ.
func TestProcess_CtxCancelledDuringRetry(t *testing.T) {
	dlq := &fakeDLQ{}
	commit := &commitRecorder{}
	ctx, cancel := context.WithCancel(context.Background())

	processWithRetryAndDLQ(
		ctx, testMsg(), 3, dlq,
		func(_ context.Context, _ []byte) error {
			cancel() // cancel after first attempt
			return errors.New("error")
		},
		commit.fn,
	)

	// With ctx cancelled the backoff select returns immediately via ctx.Done(),
	// so the function must return false.
	got := processWithRetryAndDLQ(
		ctx, testMsg(), 3, dlq,
		func(_ context.Context, _ []byte) error { return errors.New("error") },
		commit.fn,
	)

	if got {
		t.Fatal("expected false when ctx is already cancelled")
	}
	if commit.called {
		t.Fatal("must not commit when ctx is cancelled")
	}
}

// TestProcess_DLQFailureThenCtxCancel verifies that when the DLQ itself is
// unavailable the function blocks without committing and returns false only
// when the context is cancelled.
func TestProcess_DLQFailureThenCtxCancel(t *testing.T) {
	// DLQ always fails.
	dlq := &fakeDLQ{failUntil: 999}
	commit := &commitRecorder{}

	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()

	got := processWithRetryAndDLQ(
		ctx, testMsg(), 1, dlq,
		func(_ context.Context, _ []byte) error { return errors.New("fail") },
		commit.fn,
	)

	if got {
		t.Fatal("expected false when ctx times out while DLQ is unavailable")
	}
	if commit.called {
		t.Fatal("must not commit when DLQ publish never succeeded")
	}
}

// TestProcess_DLQFailThenRecover verifies that after a temporary DLQ outage
// the function eventually commits once the DLQ recovers.
func TestProcess_DLQFailThenRecover(t *testing.T) {
	// Fail the first 2 DLQ write attempts, then succeed on the third.
	dlq := &fakeDLQ{failUntil: 2}
	commit := &commitRecorder{}

	// Override the 5-second DLQ retry sleep for this test.
	origDLQRetry := dlqRetryDelay
	dlqRetryDelay = time.Millisecond
	defer func() { dlqRetryDelay = origDLQRetry }()

	got := processWithRetryAndDLQ(
		context.Background(), testMsg(), 1, dlq,
		func(_ context.Context, _ []byte) error { return errors.New("fail") },
		commit.fn,
	)

	if !got {
		t.Fatal("expected true after DLQ recovered")
	}
	if len(dlq.messages) != 1 {
		t.Fatalf("expected 1 DLQ message after recovery, got %d", len(dlq.messages))
	}
	if !commit.called {
		t.Fatal("expected commit after DLQ recovered and published")
	}
}
