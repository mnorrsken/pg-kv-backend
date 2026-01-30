package cache

import (
	"sync"
	"testing"
	"time"
)

func TestCache_GetSet(t *testing.T) {
	c := New(Config{
		TTL:     time.Minute,
		MaxSize: 100,
	})
	defer c.Stop()

	// Set a value
	c.Set("key1", "value1")

	// Get existing key
	val, found := c.Get("key1")
	if !found {
		t.Error("expected key1 to be found")
	}
	if val != "value1" {
		t.Errorf("expected 'value1', got %q", val)
	}

	// Get non-existent key
	_, found = c.Get("nonexistent")
	if found {
		t.Error("expected nonexistent key to not be found")
	}
}

func TestCache_Overwrite(t *testing.T) {
	c := New(Config{
		TTL:     time.Minute,
		MaxSize: 100,
	})
	defer c.Stop()

	c.Set("key", "value1")
	c.Set("key", "value2")

	val, found := c.Get("key")
	if !found || val != "value2" {
		t.Errorf("expected 'value2', got %q (found=%v)", val, found)
	}
}

func TestCache_Delete(t *testing.T) {
	c := New(Config{
		TTL:     time.Minute,
		MaxSize: 100,
	})
	defer c.Stop()

	c.Set("key", "value")
	c.Delete("key")

	_, found := c.Get("key")
	if found {
		t.Error("expected key to be deleted")
	}
}

func TestCache_DeleteMulti(t *testing.T) {
	c := New(Config{
		TTL:     time.Minute,
		MaxSize: 100,
	})
	defer c.Stop()

	c.Set("key1", "value1")
	c.Set("key2", "value2")
	c.Set("key3", "value3")

	c.DeleteMulti([]string{"key1", "key2"})

	if _, found := c.Get("key1"); found {
		t.Error("key1 should be deleted")
	}
	if _, found := c.Get("key2"); found {
		t.Error("key2 should be deleted")
	}
	if _, found := c.Get("key3"); !found {
		t.Error("key3 should still exist")
	}
}

func TestCache_Invalidate(t *testing.T) {
	c := New(Config{
		TTL:     time.Minute,
		MaxSize: 100,
	})
	defer c.Stop()

	c.Set("key", "value")
	c.Invalidate("key")

	_, found := c.Get("key")
	if found {
		t.Error("expected key to be invalidated")
	}
}

func TestCache_Flush(t *testing.T) {
	c := New(Config{
		TTL:     time.Minute,
		MaxSize: 100,
	})
	defer c.Stop()

	c.Set("key1", "value1")
	c.Set("key2", "value2")
	c.Set("key3", "value3")

	if c.Size() != 3 {
		t.Errorf("expected size 3, got %d", c.Size())
	}

	c.Flush()

	if c.Size() != 0 {
		t.Errorf("expected size 0 after flush, got %d", c.Size())
	}
}

func TestCache_Size(t *testing.T) {
	c := New(Config{
		TTL:     time.Minute,
		MaxSize: 100,
	})
	defer c.Stop()

	if c.Size() != 0 {
		t.Errorf("expected size 0, got %d", c.Size())
	}

	c.Set("key1", "value1")
	c.Set("key2", "value2")

	if c.Size() != 2 {
		t.Errorf("expected size 2, got %d", c.Size())
	}
}

func TestCache_TTLExpiration(t *testing.T) {
	c := New(Config{
		TTL:             50 * time.Millisecond,
		MaxSize:         100,
		CleanupInterval: 10 * time.Millisecond,
	})
	defer c.Stop()

	c.Set("key", "value")

	// Should exist immediately
	val, found := c.Get("key")
	if !found || val != "value" {
		t.Error("key should exist immediately after set")
	}

	// Wait for TTL to expire
	time.Sleep(100 * time.Millisecond)

	// Should no longer be found (even before cleanup, Get checks expiry)
	_, found = c.Get("key")
	if found {
		t.Error("key should have expired")
	}
}

func TestCache_CleanupRemovesExpired(t *testing.T) {
	c := New(Config{
		TTL:             30 * time.Millisecond,
		MaxSize:         100,
		CleanupInterval: 20 * time.Millisecond,
	})
	defer c.Stop()

	c.Set("key1", "value1")
	c.Set("key2", "value2")

	if c.Size() != 2 {
		t.Errorf("expected size 2, got %d", c.Size())
	}

	// Wait for TTL + cleanup interval
	time.Sleep(100 * time.Millisecond)

	// Cleanup should have removed expired entries
	if c.Size() != 0 {
		t.Errorf("expected size 0 after cleanup, got %d", c.Size())
	}
}

func TestCache_MaxSizeEviction(t *testing.T) {
	c := New(Config{
		TTL:     time.Minute,
		MaxSize: 3,
	})
	defer c.Stop()

	c.Set("key1", "value1")
	c.Set("key2", "value2")
	c.Set("key3", "value3")

	// At max capacity - new entries should be rejected
	c.Set("key4", "value4")

	// key4 should not be added (simple eviction policy)
	_, found := c.Get("key4")
	if found {
		t.Error("key4 should not be added when at max capacity")
	}

	// Original keys should still exist
	for _, key := range []string{"key1", "key2", "key3"} {
		if _, found := c.Get(key); !found {
			t.Errorf("%s should still exist", key)
		}
	}
}

func TestCache_MaxSizeUpdateExisting(t *testing.T) {
	c := New(Config{
		TTL:     time.Minute,
		MaxSize: 2,
	})
	defer c.Stop()

	c.Set("key1", "value1")
	c.Set("key2", "value2")

	// Updating existing key should work even at capacity
	c.Set("key1", "updated")

	val, found := c.Get("key1")
	if !found || val != "updated" {
		t.Errorf("expected 'updated', got %q (found=%v)", val, found)
	}
}

func TestCache_ConcurrentAccess(t *testing.T) {
	c := New(Config{
		TTL:     time.Minute,
		MaxSize: 1000,
	})
	defer c.Stop()

	var wg sync.WaitGroup
	numGoroutines := 100
	numOperations := 100

	// Concurrent writes
	for i := 0; i < numGoroutines; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			for j := 0; j < numOperations; j++ {
				key := string(rune('a' + (id+j)%26))
				c.Set(key, "value")
			}
		}(i)
	}

	// Concurrent reads
	for i := 0; i < numGoroutines; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			for j := 0; j < numOperations; j++ {
				key := string(rune('a' + (id+j)%26))
				c.Get(key)
			}
		}(i)
	}

	// Concurrent deletes
	for i := 0; i < numGoroutines/2; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			for j := 0; j < numOperations; j++ {
				key := string(rune('a' + (id+j)%26))
				c.Delete(key)
			}
		}(i)
	}

	wg.Wait()

	// No panics = success
	// Just verify cache is in a valid state
	_ = c.Size()
}

func TestCache_ConcurrentReadWrite(t *testing.T) {
	c := New(Config{
		TTL:     time.Minute,
		MaxSize: 100,
	})
	defer c.Stop()

	var wg sync.WaitGroup
	done := make(chan struct{})

	// Writer goroutine
	wg.Add(1)
	go func() {
		defer wg.Done()
		i := 0
		for {
			select {
			case <-done:
				return
			default:
				c.Set("shared", string(rune('a'+i%26)))
				i++
			}
		}
	}()

	// Reader goroutines
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				select {
				case <-done:
					return
				default:
					c.Get("shared")
				}
			}
		}()
	}

	// Run for a bit
	time.Sleep(50 * time.Millisecond)
	close(done)
	wg.Wait()
}

func TestCache_StopTerminatesCleanup(t *testing.T) {
	c := New(Config{
		TTL:             time.Minute,
		MaxSize:         100,
		CleanupInterval: 10 * time.Millisecond,
	})

	c.Set("key", "value")

	// Stop should not block or panic
	c.Stop()

	// Operations after stop should still work (just no cleanup)
	c.Set("key2", "value2")
	c.Get("key")
}

func TestCache_DefaultCleanupInterval(t *testing.T) {
	// Test that cleanup interval defaults to TTL/2
	c := New(Config{
		TTL:     100 * time.Millisecond,
		MaxSize: 100,
		// CleanupInterval not set
	})
	defer c.Stop()

	c.Set("key", "value")

	// Wait for expiry and default cleanup
	time.Sleep(150 * time.Millisecond)

	if c.Size() != 0 {
		t.Errorf("expected cleanup to have run, size=%d", c.Size())
	}
}

func TestCache_MinimumCleanupInterval(t *testing.T) {
	// Very short TTL should still have reasonable cleanup interval
	c := New(Config{
		TTL:     1 * time.Millisecond,
		MaxSize: 100,
	})
	defer c.Stop()

	c.Set("key", "value")

	// Wait and verify cleanup runs
	time.Sleep(50 * time.Millisecond)

	_, found := c.Get("key")
	if found {
		t.Error("key should have expired")
	}
}

func TestCache_EmptyKey(t *testing.T) {
	c := New(Config{
		TTL:     time.Minute,
		MaxSize: 100,
	})
	defer c.Stop()

	c.Set("", "empty key value")

	val, found := c.Get("")
	if !found || val != "empty key value" {
		t.Errorf("empty key should work, got %q (found=%v)", val, found)
	}
}

func TestCache_EmptyValue(t *testing.T) {
	c := New(Config{
		TTL:     time.Minute,
		MaxSize: 100,
	})
	defer c.Stop()

	c.Set("key", "")

	val, found := c.Get("key")
	if !found || val != "" {
		t.Errorf("empty value should work, got %q (found=%v)", val, found)
	}
}

func TestCache_LargeValue(t *testing.T) {
	c := New(Config{
		TTL:     time.Minute,
		MaxSize: 100,
	})
	defer c.Stop()

	// 1MB value
	largeValue := make([]byte, 1024*1024)
	for i := range largeValue {
		largeValue[i] = byte(i % 256)
	}

	c.Set("large", string(largeValue))

	val, found := c.Get("large")
	if !found || val != string(largeValue) {
		t.Error("large value should be stored and retrieved correctly")
	}
}

// ============== Benchmark Tests ==============

func BenchmarkCache_Set(b *testing.B) {
	c := New(Config{
		TTL:     time.Minute,
		MaxSize: 100000,
	})
	defer c.Stop()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		c.Set("key", "value")
	}
}

func BenchmarkCache_Get(b *testing.B) {
	c := New(Config{
		TTL:     time.Minute,
		MaxSize: 100000,
	})
	defer c.Stop()
	c.Set("key", "value")

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		c.Get("key")
	}
}

func BenchmarkCache_GetMiss(b *testing.B) {
	c := New(Config{
		TTL:     time.Minute,
		MaxSize: 100000,
	})
	defer c.Stop()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		c.Get("nonexistent")
	}
}

func BenchmarkCache_ConcurrentGetSet(b *testing.B) {
	c := New(Config{
		TTL:     time.Minute,
		MaxSize: 100000,
	})
	defer c.Stop()

	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			if i%2 == 0 {
				c.Set("key", "value")
			} else {
				c.Get("key")
			}
			i++
		}
	})
}
