package pricing_test

import (
	"context"
	"errors"
	"testing"

	"github.com/OmidRasouli/sms-gateway-task/internal/domain"
	"github.com/OmidRasouli/sms-gateway-task/internal/pricing"
)

// mockLoader is a test double for pricing.DBLoader.
type mockLoader struct {
	prices map[domain.MessageType]int64
	err    error
}

func (m *mockLoader) LoadPrices(_ context.Context) (map[domain.MessageType]int64, error) {
	if m.err != nil {
		return nil, m.err
	}
	// Return a copy so tests can mutate m.prices independently.
	result := make(map[domain.MessageType]int64, len(m.prices))
	for k, v := range m.prices {
		result[k] = v
	}
	return result, nil
}

func TestPriceCache_LoadAndGet(t *testing.T) {
	loader := &mockLoader{
		prices: map[domain.MessageType]int64{
			domain.MessageTypeNormal:  10,
			domain.MessageTypeExpress: 25,
		},
	}
	pc := pricing.NewPriceCache(loader)

	if err := pc.LoadFromDB(context.Background()); err != nil {
		t.Fatalf("LoadFromDB: %v", err)
	}

	got, ok := pc.Get(domain.MessageTypeNormal)
	if !ok || got != 10 {
		t.Errorf("normal: got %d ok=%v, want 10 true", got, ok)
	}

	got, ok = pc.Get(domain.MessageTypeExpress)
	if !ok || got != 25 {
		t.Errorf("express: got %d ok=%v, want 25 true", got, ok)
	}
}

func TestPriceCache_Get_UnknownType(t *testing.T) {
	loader := &mockLoader{
		prices: map[domain.MessageType]int64{
			domain.MessageTypeNormal:  10,
			domain.MessageTypeExpress: 25,
		},
	}
	pc := pricing.NewPriceCache(loader)
	if err := pc.LoadFromDB(context.Background()); err != nil {
		t.Fatalf("LoadFromDB: %v", err)
	}

	_, ok := pc.Get("bulk")
	if ok {
		t.Error("expected ok=false for unknown message type, got true")
	}
}

func TestPriceCache_LoadFromDB_Error(t *testing.T) {
	loader := &mockLoader{err: errors.New("db unavailable")}
	pc := pricing.NewPriceCache(loader)

	if err := pc.LoadFromDB(context.Background()); err == nil {
		t.Fatal("expected error from LoadFromDB when loader fails")
	}
}

func TestPriceCache_RefreshFailure_KeepsStaleValues(t *testing.T) {
	loader := &mockLoader{
		prices: map[domain.MessageType]int64{
			domain.MessageTypeNormal:  10,
			domain.MessageTypeExpress: 25,
		},
	}
	pc := pricing.NewPriceCache(loader)
	if err := pc.LoadFromDB(context.Background()); err != nil {
		t.Fatalf("LoadFromDB: %v", err)
	}

	// Simulate a transient DB error on refresh.
	loader.err = errors.New("transient db error")

	err := pc.Reload(context.Background())
	if err == nil {
		t.Fatal("expected Reload to return an error when loader fails")
	}

	// Stale values must still be served.
	got, ok := pc.Get(domain.MessageTypeNormal)
	if !ok || got != 10 {
		t.Errorf("normal after failed refresh: got %d ok=%v, want 10 true", got, ok)
	}

	got, ok = pc.Get(domain.MessageTypeExpress)
	if !ok || got != 25 {
		t.Errorf("express after failed refresh: got %d ok=%v, want 25 true", got, ok)
	}
}
