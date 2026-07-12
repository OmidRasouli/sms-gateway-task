package pricing

import (
	"context"
	"log"
	"sync"
	"time"

	"github.com/OmidRasouli/sms-gateway-task/internal/domain"
)

// DBLoader fetches the full pricing table from the backing store.
type DBLoader interface {
	LoadPrices(ctx context.Context) (map[domain.MessageType]int64, error)
}

// PriceCache is a read-optimised, periodically refreshed in-memory cache of
// per-message-type prices.  It is safe for concurrent use.
type PriceCache struct {
	mu     sync.RWMutex
	prices map[domain.MessageType]int64
	loader DBLoader
}

// NewPriceCache returns a PriceCache backed by loader. Call LoadFromDB before
// serving traffic.
func NewPriceCache(loader DBLoader) *PriceCache {
	return &PriceCache{loader: loader}
}

// LoadFromDB fetches prices from the DB and replaces the cached map.  It must
// be called once at startup; if it returns an error the process should abort
// rather than serve with an empty cache.
func (pc *PriceCache) LoadFromDB(ctx context.Context) error {
	prices, err := pc.loader.LoadPrices(ctx)
	if err != nil {
		return err
	}
	pc.mu.Lock()
	pc.prices = prices
	pc.mu.Unlock()
	return nil
}

// Reload fetches prices from the DB and updates the cache on success.  On
// failure it logs a warning and keeps the previous values intact.  Returns the
// error for observability; callers should not treat a non-nil return as fatal.
func (pc *PriceCache) Reload(ctx context.Context) error {
	prices, err := pc.loader.LoadPrices(ctx)
	if err != nil {
		log.Printf("price cache: refresh failed (keeping stale prices): %v", err)
		return err
	}
	pc.mu.Lock()
	pc.prices = prices
	pc.mu.Unlock()
	return nil
}

// StartRefresh launches a background goroutine that calls Reload every
// interval.  The goroutine exits when ctx is cancelled.
func (pc *PriceCache) StartRefresh(ctx context.Context, interval time.Duration) {
	go func() {
		ticker := time.NewTicker(interval)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				_ = pc.Reload(ctx)
			}
		}
	}()
}

// Get returns the price for msgType and whether it was found.
func (pc *PriceCache) Get(msgType domain.MessageType) (int64, bool) {
	pc.mu.RLock()
	defer pc.mu.RUnlock()
	price, ok := pc.prices[msgType]
	return price, ok
}
