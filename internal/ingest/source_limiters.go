package ingest

import (
	"sync"
	"time"
)

// SourceLimiters keeps independent token buckets for registered source IDs.
// Idle buckets are pruned when the cache grows, so revoked sources do not
// cause unbounded process memory growth.
type SourceLimiters struct {
	mu      sync.Mutex
	buckets map[string]sourceBucket
}

type sourceBucket struct {
	limiter  *Limiter
	lastUsed time.Time
}

func (s *SourceLimiters) Allow(sourceID string, rate int) bool {
	if rate <= 0 {
		return false
	}
	now := time.Now()
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.buckets == nil {
		s.buckets = make(map[string]sourceBucket)
	}
	bucket, exists := s.buckets[sourceID]
	if !exists || bucket.limiter.rate != float64(rate) {
		bucket = sourceBucket{limiter: NewLimiter(rate, rate), lastUsed: now}
	}
	bucket.lastUsed = now
	s.buckets[sourceID] = bucket
	if len(s.buckets) > 4096 {
		cutoff := now.Add(-time.Hour)
		for id, entry := range s.buckets {
			if entry.lastUsed.Before(cutoff) {
				delete(s.buckets, id)
			}
		}
	}
	return bucket.limiter.Allow()
}
