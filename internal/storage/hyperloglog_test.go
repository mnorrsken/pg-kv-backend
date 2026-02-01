package storage

import (
	"fmt"
	"hash/fnv"
	"testing"
)

func TestHyperLogLog(t *testing.T) {
	hll := NewHyperLogLog()

	// Debug: show hash values
	elements := []string{"a", "b", "c", "d", "e"}
	for _, elem := range elements {
		h := fnv.New64a()
		h.Write([]byte(elem))
		hash := h.Sum64()
		index := hash >> (64 - 14)
		t.Logf("Element %s: hash=%016x, index=%d", elem, hash, index)
		changed := hll.Add(elem)
		t.Logf("Add %s: changed=%v, register[%d]=%d", elem, changed, index, hll.registers[index])
	}

	// Count non-zero registers
	nonzero := 0
	for _, v := range hll.registers {
		if v > 0 {
			nonzero++
		}
	}
	t.Logf("Non-zero registers: %d", nonzero)

	count := hll.Count()
	t.Logf("Count: %d", count)

	// HyperLogLog is probabilistic, allow some error but 5 elements should be close
	if count < 4 || count > 6 {
		t.Errorf("Expected approximately 5, got %d", count)
	}
}

func TestHyperLogLogIndexDistribution(t *testing.T) {
	// Test that different elements get different indices
	indices := make(map[uint64]string)
	for i := 0; i < 100; i++ {
		elem := fmt.Sprintf("element%d", i)
		h := fnv.New64a()
		h.Write([]byte(elem))
		hash := h.Sum64()
		index := hash >> (64 - 14)
		if prev, exists := indices[index]; exists {
			t.Logf("Collision: %s and %s both map to index %d", prev, elem, index)
		}
		indices[index] = elem
	}
	t.Logf("100 elements mapped to %d unique indices", len(indices))
}

func TestHyperLogLogLarge(t *testing.T) {
	hll := NewHyperLogLog()

	// Add 1000 unique elements
	for i := 0; i < 1000; i++ {
		hll.Add(fmt.Sprintf("element%d", i))
	}

	// Count non-zero registers
	nonzero := 0
	for _, v := range hll.registers {
		if v > 0 {
			nonzero++
		}
	}
	t.Logf("Non-zero registers for 1000 elements: %d", nonzero)

	count := hll.Count()
	t.Logf("Count for 1000 elements: %d", count)

	// Allow 5% error
	if count < 950 || count > 1050 {
		t.Errorf("Expected approximately 1000, got %d", count)
	}
}

func TestHyperLogLogMerge(t *testing.T) {
	hll1 := NewHyperLogLog()
	hll2 := NewHyperLogLog()

	// Add different elements to each
	for _, elem := range []string{"a", "b", "c"} {
		hll1.Add(elem)
	}
	for _, elem := range []string{"d", "e", "f"} {
		hll2.Add(elem)
	}

	// Merge
	hll1.Merge(hll2)

	count := hll1.Count()
	t.Logf("Merged count: %d", count)

	if count < 5 || count > 7 {
		t.Errorf("Expected approximately 6, got %d", count)
	}
}
