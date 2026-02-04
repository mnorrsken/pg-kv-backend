package cache

import (
	"testing"
	"time"
)

func TestPolicy_ShouldCache_TTLThreshold(t *testing.T) {
	policy := NewPolicy(PolicyConfig{
		MinTTLForCache: 5 * time.Second,
	})

	tests := []struct {
		name        string
		key         string
		ttl         time.Duration
		shouldCache bool
		reason      string
	}{
		{
			name:        "TTL above threshold",
			key:         "key1",
			ttl:         10 * time.Second,
			shouldCache: true,
			reason:      "allowed",
		},
		{
			name:        "TTL below threshold",
			key:         "key2",
			ttl:         1 * time.Second,
			shouldCache: false,
			reason:      "ttl_too_short",
		},
		{
			name:        "No TTL (permanent key)",
			key:         "key3",
			ttl:         0,
			shouldCache: true,
			reason:      "allowed",
		},
		{
			name:        "TTL exactly at threshold",
			key:         "key4",
			ttl:         5 * time.Second,
			shouldCache: true,
			reason:      "allowed",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			decision := policy.ShouldCache(tt.key, tt.ttl)
			if decision.ShouldCache != tt.shouldCache {
				t.Errorf("ShouldCache = %v, want %v", decision.ShouldCache, tt.shouldCache)
			}
			if decision.Reason != tt.reason {
				t.Errorf("Reason = %q, want %q", decision.Reason, tt.reason)
			}
		})
	}
}

func TestPolicy_ShouldCache_WriteFrequency(t *testing.T) {
	policy := NewPolicy(PolicyConfig{
		MaxWriteFrequency:   5.0, // 5 writes/sec threshold
		WriteTrackingWindow: 1 * time.Second,
	})

	key := "hot-key"

	// Initially, should be cacheable
	decision := policy.ShouldCache(key, 0)
	if !decision.ShouldCache {
		t.Error("Expected key to be cacheable initially")
	}

	// Record 10 writes (exceeds 5/sec threshold in 1 second window)
	for i := 0; i < 10; i++ {
		policy.RecordWrite(key)
	}

	// Now should not be cacheable
	decision = policy.ShouldCache(key, 0)
	if decision.ShouldCache {
		t.Error("Expected hot key to not be cacheable")
	}
	if decision.Reason != "write_frequency_too_high" {
		t.Errorf("Expected reason 'write_frequency_too_high', got %q", decision.Reason)
	}

	// A different key should still be cacheable
	decision = policy.ShouldCache("cold-key", 0)
	if !decision.ShouldCache {
		t.Error("Expected cold key to be cacheable")
	}
}

func TestPolicy_ShouldCache_Patterns(t *testing.T) {
	policy := NewPolicy(PolicyConfig{
		ExcludePatterns: []string{"pubsub:*", "lock:*", "*:temp"},
		IncludePatterns: []string{"static:*", "cache:*"},
	})

	tests := []struct {
		name        string
		key         string
		shouldCache bool
		reason      string
	}{
		{
			name:        "Excluded by prefix pattern",
			key:         "pubsub:channel1",
			shouldCache: false,
			reason:      "exclude_pattern",
		},
		{
			name:        "Excluded by suffix pattern",
			key:         "session:temp",
			shouldCache: false,
			reason:      "exclude_pattern",
		},
		{
			name:        "Included by pattern (overrides exclusion)",
			key:         "static:logo.png",
			shouldCache: true,
			reason:      "include_pattern",
		},
		{
			name:        "Regular key",
			key:         "user:123",
			shouldCache: true,
			reason:      "allowed",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			decision := policy.ShouldCache(tt.key, 0)
			if decision.ShouldCache != tt.shouldCache {
				t.Errorf("ShouldCache = %v, want %v", decision.ShouldCache, tt.shouldCache)
			}
			if decision.Reason != tt.reason {
				t.Errorf("Reason = %q, want %q", decision.Reason, tt.reason)
			}
		})
	}
}

func TestPolicy_IncludeOverridesExclude(t *testing.T) {
	// Test that include patterns take precedence over exclusion rules
	policy := NewPolicy(PolicyConfig{
		MinTTLForCache:    10 * time.Second,
		ExcludePatterns:   []string{"temp:*"},
		IncludePatterns:   []string{"temp:important:*"},
	})

	// temp:important:* should be cached even though temp:* is excluded
	// and even though TTL is below threshold
	decision := policy.ShouldCache("temp:important:data", 1*time.Second)
	if !decision.ShouldCache {
		t.Error("Include pattern should override other exclusion rules")
	}
	if decision.Reason != "include_pattern" {
		t.Errorf("Reason should be 'include_pattern', got %q", decision.Reason)
	}
}

func TestMatchGlob(t *testing.T) {
	tests := []struct {
		pattern string
		str     string
		match   bool
	}{
		{"pubsub:*", "pubsub:channel", true},
		{"pubsub:*", "pubsub:", true},
		{"pubsub:*", "other:channel", false},
		{"*:temp", "session:temp", true},
		{"*:temp", "session:permanent", false},
		{"prefix*suffix", "prefixmiddlesuffix", true},
		{"prefix*suffix", "prefixsuffix", true},
		{"prefix*suffix", "prefixmiddle", false},
		{"exact", "exact", true},
		{"exact", "notexact", false},
	}

	for _, tt := range tests {
		t.Run(tt.pattern+"/"+tt.str, func(t *testing.T) {
			result := matchGlob(tt.pattern, tt.str)
			if result != tt.match {
				t.Errorf("matchGlob(%q, %q) = %v, want %v", tt.pattern, tt.str, result, tt.match)
			}
		})
	}
}

func TestPolicy_DefaultConfig(t *testing.T) {
	cfg := DefaultPolicyConfig()
	
	if cfg.MinTTLForCache != 1*time.Second {
		t.Errorf("Default MinTTLForCache = %v, want 1s", cfg.MinTTLForCache)
	}
	if cfg.MaxWriteFrequency != 10.0 {
		t.Errorf("Default MaxWriteFrequency = %v, want 10.0", cfg.MaxWriteFrequency)
	}
	if cfg.WriteTrackingWindow != 10*time.Second {
		t.Errorf("Default WriteTrackingWindow = %v, want 10s", cfg.WriteTrackingWindow)
	}
}
