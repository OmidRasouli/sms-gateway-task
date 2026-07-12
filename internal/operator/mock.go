package operator

import (
	"context"
	"errors"
	"math/rand"
	"time"
)

// MockAdapter simulates sending a message to a real telecom operator.
// Swap this for a real HTTP-based adapter once an operator contract exists —
// callers only depend on the Send method below.
type MockAdapter struct{}

func NewMockAdapter() *MockAdapter {
	return &MockAdapter{}
}

var ErrOperatorUnavailable = errors.New("operator temporarily unavailable")

// ErrOperatorPermanentFailure signals an unrecoverable send failure (e.g. invalid
// destination). The worker should not retry and should reverse the balance deduction.
var ErrOperatorPermanentFailure = errors.New("operator permanent failure: invalid destination")

func (m *MockAdapter) Send(ctx context.Context, phoneNumber, text string) error {
	// simulate network latency
	select {
	case <-time.After(time.Duration(50+rand.Intn(150)) * time.Millisecond):
	case <-ctx.Done():
		return ctx.Err()
	}
	r := rand.Intn(100)
	// ~1% permanent failure (invalid destination) — triggers balance reversal
	if r < 1 {
		return ErrOperatorPermanentFailure
	}
	// ~2% transient failure — triggers retry via Kafka offset non-commit
	if r < 3 {
		return ErrOperatorUnavailable
	}
	return nil
}
