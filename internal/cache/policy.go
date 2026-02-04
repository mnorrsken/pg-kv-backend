// Package cache provides caching with intelligent cache policies.
package cache

import (
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

// Policy determines whether a key should be cached based on various heuristics.
// This helps avoid caching hot/frequently-written keys (like those used for
// pub/sub or messaging) while caching stable keys (like static web content).
type Policy struct {
	cfg         PolicyConfig
	writeStats  sync.Map // key -> *keyStats
	patternLock sync.RWMutex
}

// PolicyConfig holds configuration for intelligent cache decisions
type PolicyConfig struct {
	// MinTTLForCache is the minimum TTL required for a key to be cached.
	// Keys with shorter TTL (or no TTL) are considered transient and won't be cached.
	// Set to 0 to disable TTL-based filtering.
	// Default: 1 second
	MinTTLForCache time.Duration

	// MaxWriteFrequency is the maximum writes per second allowed for a key to be cached.
	// Keys written more frequently are considered "hot" (e.g., counters, pubsub) and won't be cached.
	// Set to 0 to disable write frequency tracking.
	// Default: 10 writes/second
	MaxWriteFrequency float64

	// WriteTrackingWindow is how long to track writes for frequency calculation.
	// Default: 10 seconds
	WriteTrackingWindow time.Duration

	// ExcludePatterns is a list of key patterns to never cache (glob-style).
	// Useful for explicitly excluding known pubsub or messaging keys.
	// Example: ["pubsub:*", "channel:*", "lock:*"]
	ExcludePatterns []string

	// IncludePatterns is a list of key patterns to always cache (glob-style).
	// These patterns take precedence over exclusion rules.
	// Useful for explicitly caching known static content.
	// Example: ["static:*", "cache:*"]
	IncludePatterns []string

	// CleanupInterval for removing old write stats
	// Default: 30 seconds
	CleanupInterval time.Duration
}

// keyStats tracks write statistics for a single key
type keyStats struct {
	writes    int64     // atomic counter
	windowEnd time.Time // when current window ends
}

// DefaultPolicyConfig returns sensible defaults for intelligent caching
func DefaultPolicyConfig() PolicyConfig {
	return PolicyConfig{
		MinTTLForCache:      1 * time.Second,
		MaxWriteFrequency:   10.0,
		WriteTrackingWindow: 10 * time.Second,
		CleanupInterval:     30 * time.Second,
	}
}

// NewPolicy creates a new cache policy with the given configuration
func NewPolicy(cfg PolicyConfig) *Policy {
	if cfg.WriteTrackingWindow == 0 {
		cfg.WriteTrackingWindow = 10 * time.Second
	}
	if cfg.CleanupInterval == 0 {
		cfg.CleanupInterval = 30 * time.Second
	}

	p := &Policy{cfg: cfg}

	// Start cleanup goroutine if write tracking is enabled
	if cfg.MaxWriteFrequency > 0 {
		go p.cleanupLoop()
	}

	return p
}

// CacheDecision represents why a key should or shouldn't be cached
type CacheDecision struct {
	ShouldCache bool
	Reason      string
}

// ShouldCache determines if a key should be cached based on the policy.
// ttl is the TTL of the key (0 means no expiry).
func (p *Policy) ShouldCache(key string, ttl time.Duration) CacheDecision {
	// Check include patterns first (they take precedence)
	if len(p.cfg.IncludePatterns) > 0 && p.matchesPattern(key, p.cfg.IncludePatterns) {
		return CacheDecision{ShouldCache: true, Reason: "include_pattern"}
	}

	// Check exclude patterns
	if len(p.cfg.ExcludePatterns) > 0 && p.matchesPattern(key, p.cfg.ExcludePatterns) {
		return CacheDecision{ShouldCache: false, Reason: "exclude_pattern"}
	}

	// Check TTL threshold
	if p.cfg.MinTTLForCache > 0 {
		if ttl == 0 {
			// No expiry - could be permanent or very short-lived
			// We'll allow caching for no-TTL keys unless they're hot
		} else if ttl < p.cfg.MinTTLForCache {
			return CacheDecision{ShouldCache: false, Reason: "ttl_too_short"}
		}
	}

	// Check write frequency
	if p.cfg.MaxWriteFrequency > 0 {
		freq := p.getWriteFrequency(key)
		if freq > p.cfg.MaxWriteFrequency {
			return CacheDecision{ShouldCache: false, Reason: "write_frequency_too_high"}
		}
	}

	return CacheDecision{ShouldCache: true, Reason: "allowed"}
}

// RecordWrite records a write operation for frequency tracking
func (p *Policy) RecordWrite(key string) {
	if p.cfg.MaxWriteFrequency <= 0 {
		return
	}

	now := time.Now()
	windowEnd := now.Add(p.cfg.WriteTrackingWindow)

	val, loaded := p.writeStats.LoadOrStore(key, &keyStats{
		writes:    1,
		windowEnd: windowEnd,
	})

	if loaded {
		stats := val.(*keyStats)
		// Check if window expired
		if now.After(stats.windowEnd) {
			// Reset window
			stats.windowEnd = windowEnd
			atomic.StoreInt64(&stats.writes, 1)
		} else {
			atomic.AddInt64(&stats.writes, 1)
		}
	}
}

// getWriteFrequency returns writes per second for a key
func (p *Policy) getWriteFrequency(key string) float64 {
	val, ok := p.writeStats.Load(key)
	if !ok {
		return 0
	}

	stats := val.(*keyStats)
	now := time.Now()

	// If window expired, no recent writes
	if now.After(stats.windowEnd) {
		return 0
	}

	writes := atomic.LoadInt64(&stats.writes)
	windowSecs := p.cfg.WriteTrackingWindow.Seconds()
	if windowSecs == 0 {
		return 0
	}

	return float64(writes) / windowSecs
}

// matchesPattern checks if a key matches any of the glob patterns
func (p *Policy) matchesPattern(key string, patterns []string) bool {
	for _, pattern := range patterns {
		if matchGlob(pattern, key) {
			return true
		}
	}
	return false
}

// matchGlob performs simple glob matching with * as wildcard
func matchGlob(pattern, str string) bool {
	// Handle exact match
	if pattern == str {
		return true
	}

	// Handle simple prefix match (pattern:*)
	if strings.HasSuffix(pattern, "*") {
		prefix := strings.TrimSuffix(pattern, "*")
		return strings.HasPrefix(str, prefix)
	}

	// Handle simple suffix match (*:pattern)
	if strings.HasPrefix(pattern, "*") {
		suffix := strings.TrimPrefix(pattern, "*")
		return strings.HasSuffix(str, suffix)
	}

	// Handle middle wildcard (prefix*suffix)
	if idx := strings.Index(pattern, "*"); idx >= 0 {
		prefix := pattern[:idx]
		suffix := pattern[idx+1:]
		return strings.HasPrefix(str, prefix) && strings.HasSuffix(str, suffix)
	}

	return false
}

// cleanupLoop periodically cleans up old write stats
func (p *Policy) cleanupLoop() {
	ticker := time.NewTicker(p.cfg.CleanupInterval)
	defer ticker.Stop()

	for range ticker.C {
		p.cleanup()
	}
}

// cleanup removes expired write stats
func (p *Policy) cleanup() {
	now := time.Now()
	p.writeStats.Range(func(key, value interface{}) bool {
		stats := value.(*keyStats)
		if now.After(stats.windowEnd) {
			p.writeStats.Delete(key)
		}
		return true
	})
}

// Stats returns current policy statistics
func (p *Policy) Stats() PolicyStats {
	var count int
	p.writeStats.Range(func(key, value interface{}) bool {
		count++
		return true
	})

	return PolicyStats{
		TrackedKeys: count,
	}
}

// PolicyStats contains policy statistics
type PolicyStats struct {
	TrackedKeys int
}
