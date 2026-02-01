package storage

import (
	"context"
	"fmt"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"
)

// MockStore provides an in-memory implementation of Backend for testing
type MockStore struct {
	mu          sync.RWMutex
	strings     map[string]string
	hashes      map[string]map[string]string
	lists       map[string][]string
	sets        map[string]map[string]struct{}
	zsets       map[string]map[string]float64
	hyperloglogs map[string]*HyperLogLog
	keyTypes    map[string]KeyType
	expiresAt   map[string]time.Time
}

// NewMockStore creates a new in-memory mock store
func NewMockStore() *MockStore {
	return &MockStore{
		strings:      make(map[string]string),
		hashes:       make(map[string]map[string]string),
		lists:        make(map[string][]string),
		sets:         make(map[string]map[string]struct{}),
		zsets:        make(map[string]map[string]float64),
		hyperloglogs: make(map[string]*HyperLogLog),
		keyTypes:     make(map[string]KeyType),
		expiresAt:    make(map[string]time.Time),
	}
}

// Close is a no-op for mock store
func (m *MockStore) Close() {}

func (m *MockStore) isExpired(key string) bool {
	if exp, ok := m.expiresAt[key]; ok {
		if time.Now().After(exp) {
			m.deleteKey(key)
			return true
		}
	}
	return false
}

func (m *MockStore) deleteKey(key string) {
	delete(m.strings, key)
	delete(m.hashes, key)
	delete(m.lists, key)
	delete(m.sets, key)
	delete(m.zsets, key)
	delete(m.hyperloglogs, key)
	delete(m.keyTypes, key)
	delete(m.expiresAt, key)
}

// ============== String Commands ==============

func (m *MockStore) Get(ctx context.Context, key string) (string, bool, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	if m.isExpired(key) {
		return "", false, nil
	}

	if t, ok := m.keyTypes[key]; ok && t != TypeString {
		return "", false, fmt.Errorf("WRONGTYPE Operation against a key holding the wrong kind of value")
	}

	val, ok := m.strings[key]
	return val, ok, nil
}

func (m *MockStore) Set(ctx context.Context, key, value string, ttl time.Duration) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.deleteKey(key)
	m.strings[key] = value
	m.keyTypes[key] = TypeString

	if ttl > 0 {
		m.expiresAt[key] = time.Now().Add(ttl)
	}

	return nil
}

func (m *MockStore) SetNX(ctx context.Context, key, value string) (bool, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	if m.isExpired(key) {
		// Key expired, treat as non-existent
	} else if t, ok := m.keyTypes[key]; ok {
		if t != TypeString {
			return false, fmt.Errorf("WRONGTYPE Operation against a key holding the wrong kind of value")
		}
		// Key exists as string, return false (NX means set if not exists)
		return false, nil
	}

	m.strings[key] = value
	m.keyTypes[key] = TypeString
	return true, nil
}

func (m *MockStore) MGet(ctx context.Context, keys []string) ([]interface{}, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	results := make([]interface{}, len(keys))
	for i, key := range keys {
		if m.isExpired(key) {
			results[i] = nil
			continue
		}
		if val, ok := m.strings[key]; ok {
			results[i] = val
		} else {
			results[i] = nil
		}
	}
	return results, nil
}

func (m *MockStore) MSet(ctx context.Context, pairs map[string]string) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	for key, value := range pairs {
		m.deleteKey(key)
		m.strings[key] = value
		m.keyTypes[key] = TypeString
	}
	return nil
}

func (m *MockStore) Incr(ctx context.Context, key string, delta int64) (int64, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	if m.isExpired(key) {
		// Key expired
	}

	if t, ok := m.keyTypes[key]; ok && t != TypeString {
		return 0, fmt.Errorf("WRONGTYPE Operation against a key holding the wrong kind of value")
	}

	var current int64
	if val, ok := m.strings[key]; ok {
		var err error
		current, err = strconv.ParseInt(val, 10, 64)
		if err != nil {
			return 0, fmt.Errorf("value is not an integer")
		}
	}

	result := current + delta
	m.strings[key] = strconv.FormatInt(result, 10)
	m.keyTypes[key] = TypeString
	return result, nil
}

func (m *MockStore) Append(ctx context.Context, key, value string) (int64, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	if m.isExpired(key) {
		// Key expired, start fresh
	}

	if t, ok := m.keyTypes[key]; ok && t != TypeString {
		return 0, fmt.Errorf("WRONGTYPE Operation against a key holding the wrong kind of value")
	}

	m.strings[key] = m.strings[key] + value
	m.keyTypes[key] = TypeString
	return int64(len(m.strings[key])), nil
}

func (m *MockStore) GetRange(ctx context.Context, key string, start, end int64) (string, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	if m.isExpired(key) {
		return "", nil
	}

	value, ok := m.strings[key]
	if !ok {
		return "", nil
	}

	length := int64(len(value))
	if length == 0 {
		return "", nil
	}

	if start < 0 {
		start = length + start
	}
	if end < 0 {
		end = length + end
	}
	if start < 0 {
		start = 0
	}
	if end >= length {
		end = length - 1
	}
	if start > end || start >= length {
		return "", nil
	}

	return value[start : end+1], nil
}

func (m *MockStore) SetRange(ctx context.Context, key string, offset int64, value string) (int64, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	if m.isExpired(key) {
		delete(m.strings, key)
		delete(m.keyTypes, key)
		delete(m.expiresAt, key)
	}

	if t, ok := m.keyTypes[key]; ok && t != TypeString {
		return 0, fmt.Errorf("WRONGTYPE Operation against a key holding the wrong kind of value")
	}

	existing := m.strings[key]
	endPos := int(offset) + len(value)
	if len(existing) < endPos {
		newBuf := make([]byte, endPos)
		copy(newBuf, existing)
		existing = string(newBuf)
	}

	result := []byte(existing)
	copy(result[offset:], value)
	m.strings[key] = string(result)
	m.keyTypes[key] = TypeString

	return int64(len(m.strings[key])), nil
}

func (m *MockStore) BitField(ctx context.Context, key string, ops []BitFieldOp) ([]int64, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	if m.isExpired(key) {
		delete(m.strings, key)
		delete(m.keyTypes, key)
		delete(m.expiresAt, key)
	}

	if t, ok := m.keyTypes[key]; ok && t != TypeString {
		return nil, fmt.Errorf("WRONGTYPE Operation against a key holding the wrong kind of value")
	}

	value := []byte(m.strings[key])
	results := make([]int64, 0, len(ops))
	modified := false

	for _, op := range ops {
		signed := len(op.Encoding) > 0 && op.Encoding[0] == 'i'
		bitWidth := int64(8)
		if len(op.Encoding) > 1 {
			if bw, err := strconv.ParseInt(op.Encoding[1:], 10, 64); err == nil && bw > 0 && bw <= 64 {
				bitWidth = bw
			}
		}

		bitOffset := op.Offset
		neededBytes := (bitOffset + bitWidth + 7) / 8
		if int64(len(value)) < neededBytes {
			newValue := make([]byte, neededBytes)
			copy(newValue, value)
			value = newValue
		}

		switch op.OpType {
		case "GET":
			results = append(results, mockGetBitField(value, bitOffset, bitWidth, signed))
		case "SET":
			results = append(results, mockGetBitField(value, bitOffset, bitWidth, signed))
			mockSetBitField(value, bitOffset, bitWidth, op.Value)
			modified = true
		case "INCRBY":
			oldVal := mockGetBitField(value, bitOffset, bitWidth, signed)
			newVal := oldVal + op.Value
			if signed {
				max := int64(1) << (bitWidth - 1)
				for newVal >= max {
					newVal -= max * 2
				}
				for newVal < -max {
					newVal += max * 2
				}
			} else {
				newVal &= (1 << bitWidth) - 1
			}
			mockSetBitField(value, bitOffset, bitWidth, newVal)
			results = append(results, newVal)
			modified = true
		}
	}

	if modified {
		m.strings[key] = string(value)
		m.keyTypes[key] = TypeString
	}

	return results, nil
}

func mockGetBitField(data []byte, bitOffset, bitWidth int64, signed bool) int64 {
	var result int64
	for i := int64(0); i < bitWidth; i++ {
		byteIdx := (bitOffset + i) / 8
		bitIdx := 7 - ((bitOffset + i) % 8)
		if byteIdx < int64(len(data)) && data[byteIdx]&(1<<bitIdx) != 0 {
			result |= 1 << (bitWidth - 1 - i)
		}
	}
	if signed && bitWidth > 0 && (result&(1<<(bitWidth-1))) != 0 {
		result |= ^((1 << bitWidth) - 1)
	}
	return result
}

func mockSetBitField(data []byte, bitOffset, bitWidth, value int64) {
	for i := int64(0); i < bitWidth; i++ {
		byteIdx := (bitOffset + i) / 8
		bitIdx := 7 - ((bitOffset + i) % 8)
		if byteIdx < int64(len(data)) {
			if (value>>(bitWidth-1-i))&1 != 0 {
				data[byteIdx] |= 1 << bitIdx
			} else {
				data[byteIdx] &^= 1 << bitIdx
			}
		}
	}
}

func (m *MockStore) StrLen(ctx context.Context, key string) (int64, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	if m.isExpired(key) {
		return 0, nil
	}

	if val, ok := m.strings[key]; ok {
		return int64(len(val)), nil
	}
	return 0, nil
}

func (m *MockStore) GetEx(ctx context.Context, key string, ttl time.Duration, persist bool) (string, bool, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	if m.isExpired(key) {
		delete(m.strings, key)
		delete(m.keyTypes, key)
		delete(m.expiresAt, key)
		return "", false, nil
	}

	val, ok := m.strings[key]
	if !ok {
		return "", false, nil
	}

	if persist {
		delete(m.expiresAt, key)
	} else if ttl > 0 {
		m.expiresAt[key] = time.Now().Add(ttl)
	}

	return val, true, nil
}

func (m *MockStore) GetDel(ctx context.Context, key string) (string, bool, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	if m.isExpired(key) {
		delete(m.strings, key)
		delete(m.keyTypes, key)
		delete(m.expiresAt, key)
		return "", false, nil
	}

	val, ok := m.strings[key]
	if !ok {
		return "", false, nil
	}

	delete(m.strings, key)
	delete(m.keyTypes, key)
	delete(m.expiresAt, key)

	return val, true, nil
}

func (m *MockStore) GetSet(ctx context.Context, key, value string) (string, bool, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	if m.isExpired(key) {
		delete(m.strings, key)
		delete(m.keyTypes, key)
		delete(m.expiresAt, key)
	}

	oldVal, exists := m.strings[key]
	m.strings[key] = value
	m.keyTypes[key] = TypeString
	delete(m.expiresAt, key) // GETSET clears TTL

	return oldVal, exists, nil
}

func (m *MockStore) IncrByFloat(ctx context.Context, key string, delta float64) (float64, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	if m.isExpired(key) {
		delete(m.strings, key)
		delete(m.keyTypes, key)
		delete(m.expiresAt, key)
	}

	if t, ok := m.keyTypes[key]; ok && t != TypeString {
		return 0, fmt.Errorf("WRONGTYPE Operation against a key holding the wrong kind of value")
	}

	var current float64
	if val, ok := m.strings[key]; ok {
		var err error
		current, err = strconv.ParseFloat(val, 64)
		if err != nil {
			return 0, fmt.Errorf("ERR value is not a valid float")
		}
	}

	newVal := current + delta
	m.strings[key] = strconv.FormatFloat(newVal, 'f', -1, 64)
	m.keyTypes[key] = TypeString

	return newVal, nil
}

// ============== Key Commands ==============

func (m *MockStore) Del(ctx context.Context, keys []string) (int64, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	var deleted int64
	for _, key := range keys {
		if _, ok := m.keyTypes[key]; ok {
			m.deleteKey(key)
			deleted++
		}
	}
	return deleted, nil
}

func (m *MockStore) Exists(ctx context.Context, keys []string) (int64, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	var count int64
	for _, key := range keys {
		if m.isExpired(key) {
			continue
		}
		if _, ok := m.keyTypes[key]; ok {
			count++
		}
	}
	return count, nil
}

func (m *MockStore) Expire(ctx context.Context, key string, ttl time.Duration) (bool, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	if m.isExpired(key) {
		return false, nil
	}

	if _, ok := m.keyTypes[key]; !ok {
		return false, nil
	}

	m.expiresAt[key] = time.Now().Add(ttl)
	return true, nil
}

func (m *MockStore) TTL(ctx context.Context, key string) (int64, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	if m.isExpired(key) {
		return -2, nil
	}

	if _, ok := m.keyTypes[key]; !ok {
		return -2, nil
	}

	if exp, ok := m.expiresAt[key]; ok {
		ttl := time.Until(exp).Seconds()
		if ttl < 0 {
			return -2, nil
		}
		return int64(ttl), nil
	}

	return -1, nil
}

func (m *MockStore) PTTL(ctx context.Context, key string) (int64, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	if m.isExpired(key) {
		return -2, nil
	}

	if _, ok := m.keyTypes[key]; !ok {
		return -2, nil
	}

	if exp, ok := m.expiresAt[key]; ok {
		pttl := time.Until(exp).Milliseconds()
		if pttl < 0 {
			return -2, nil
		}
		return pttl, nil
	}

	return -1, nil
}

func (m *MockStore) Persist(ctx context.Context, key string) (bool, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	if _, ok := m.expiresAt[key]; ok {
		delete(m.expiresAt, key)
		return true, nil
	}
	return false, nil
}

func (m *MockStore) Keys(ctx context.Context, pattern string) ([]string, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	// Convert glob pattern to regex
	regexPattern := "^" + regexp.QuoteMeta(pattern) + "$"
	regexPattern = strings.ReplaceAll(regexPattern, `\*`, `.*`)
	regexPattern = strings.ReplaceAll(regexPattern, `\?`, `.`)

	re, err := regexp.Compile(regexPattern)
	if err != nil {
		return nil, err
	}

	var keys []string
	for key := range m.keyTypes {
		if m.isExpired(key) {
			continue
		}
		if re.MatchString(key) {
			keys = append(keys, key)
		}
	}

	sort.Strings(keys)
	return keys, nil
}

func (m *MockStore) Type(ctx context.Context, key string) (KeyType, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	if m.isExpired(key) {
		return TypeNone, nil
	}

	if t, ok := m.keyTypes[key]; ok {
		return t, nil
	}
	return TypeNone, nil
}

func (m *MockStore) Rename(ctx context.Context, oldKey, newKey string) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	if m.isExpired(oldKey) {
		return fmt.Errorf("no such key")
	}

	keyType, ok := m.keyTypes[oldKey]
	if !ok {
		return fmt.Errorf("no such key")
	}

	// Delete new key if exists
	m.deleteKey(newKey)

	// Move data
	switch keyType {
	case TypeString:
		m.strings[newKey] = m.strings[oldKey]
		delete(m.strings, oldKey)
	case TypeHash:
		m.hashes[newKey] = m.hashes[oldKey]
		delete(m.hashes, oldKey)
	case TypeList:
		m.lists[newKey] = m.lists[oldKey]
		delete(m.lists, oldKey)
	case TypeSet:
		m.sets[newKey] = m.sets[oldKey]
		delete(m.sets, oldKey)
	}

	m.keyTypes[newKey] = keyType
	delete(m.keyTypes, oldKey)

	if exp, ok := m.expiresAt[oldKey]; ok {
		m.expiresAt[newKey] = exp
		delete(m.expiresAt, oldKey)
	}

	return nil
}

// ============== Hash Commands ==============

func (m *MockStore) HGet(ctx context.Context, key, field string) (string, bool, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	if m.isExpired(key) {
		return "", false, nil
	}

	if t, ok := m.keyTypes[key]; ok && t != TypeHash {
		return "", false, fmt.Errorf("WRONGTYPE Operation against a key holding the wrong kind of value")
	}

	if h, ok := m.hashes[key]; ok {
		if val, ok := h[field]; ok {
			return val, true, nil
		}
	}
	return "", false, nil
}

func (m *MockStore) HSet(ctx context.Context, key string, fields map[string]string) (int64, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	if m.isExpired(key) {
		// Key expired
	}

	if t, ok := m.keyTypes[key]; ok && t != TypeHash {
		return 0, fmt.Errorf("WRONGTYPE Operation against a key holding the wrong kind of value")
	}

	if m.hashes[key] == nil {
		m.hashes[key] = make(map[string]string)
	}

	var created int64
	for field, value := range fields {
		if _, exists := m.hashes[key][field]; !exists {
			created++
		}
		m.hashes[key][field] = value
	}

	m.keyTypes[key] = TypeHash
	return created, nil
}

func (m *MockStore) HDel(ctx context.Context, key string, fields []string) (int64, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	if m.isExpired(key) {
		return 0, nil
	}

	if t, ok := m.keyTypes[key]; ok && t != TypeHash {
		return 0, fmt.Errorf("WRONGTYPE Operation against a key holding the wrong kind of value")
	}

	h, ok := m.hashes[key]
	if !ok {
		return 0, nil
	}

	var deleted int64
	for _, field := range fields {
		if _, exists := h[field]; exists {
			delete(h, field)
			deleted++
		}
	}

	if len(h) == 0 {
		m.deleteKey(key)
	}

	return deleted, nil
}

func (m *MockStore) HGetAll(ctx context.Context, key string) (map[string]string, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	if m.isExpired(key) {
		return make(map[string]string), nil
	}

	if t, ok := m.keyTypes[key]; ok && t != TypeHash {
		return nil, fmt.Errorf("WRONGTYPE Operation against a key holding the wrong kind of value")
	}

	result := make(map[string]string)
	if h, ok := m.hashes[key]; ok {
		for k, v := range h {
			result[k] = v
		}
	}
	return result, nil
}

func (m *MockStore) HMGet(ctx context.Context, key string, fields []string) ([]interface{}, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	results := make([]interface{}, len(fields))

	if m.isExpired(key) {
		return results, nil
	}

	if t, ok := m.keyTypes[key]; ok && t != TypeHash {
		return nil, fmt.Errorf("WRONGTYPE Operation against a key holding the wrong kind of value")
	}

	h, ok := m.hashes[key]
	if !ok {
		return results, nil
	}

	for i, field := range fields {
		if val, ok := h[field]; ok {
			results[i] = val
		} else {
			results[i] = nil
		}
	}

	return results, nil
}

func (m *MockStore) HExists(ctx context.Context, key, field string) (bool, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	if m.isExpired(key) {
		return false, nil
	}

	if t, ok := m.keyTypes[key]; ok && t != TypeHash {
		return false, fmt.Errorf("WRONGTYPE Operation against a key holding the wrong kind of value")
	}

	if h, ok := m.hashes[key]; ok {
		_, exists := h[field]
		return exists, nil
	}
	return false, nil
}

func (m *MockStore) HKeys(ctx context.Context, key string) ([]string, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	if m.isExpired(key) {
		return nil, nil
	}

	if t, ok := m.keyTypes[key]; ok && t != TypeHash {
		return nil, fmt.Errorf("WRONGTYPE Operation against a key holding the wrong kind of value")
	}

	h, ok := m.hashes[key]
	if !ok {
		return nil, nil
	}

	keys := make([]string, 0, len(h))
	for k := range h {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	return keys, nil
}

func (m *MockStore) HVals(ctx context.Context, key string) ([]string, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	if m.isExpired(key) {
		return nil, nil
	}

	if t, ok := m.keyTypes[key]; ok && t != TypeHash {
		return nil, fmt.Errorf("WRONGTYPE Operation against a key holding the wrong kind of value")
	}

	h, ok := m.hashes[key]
	if !ok {
		return nil, nil
	}

	vals := make([]string, 0, len(h))
	for _, v := range h {
		vals = append(vals, v)
	}
	return vals, nil
}

func (m *MockStore) HLen(ctx context.Context, key string) (int64, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	if m.isExpired(key) {
		return 0, nil
	}

	if t, ok := m.keyTypes[key]; ok && t != TypeHash {
		return 0, fmt.Errorf("WRONGTYPE Operation against a key holding the wrong kind of value")
	}

	if h, ok := m.hashes[key]; ok {
		return int64(len(h)), nil
	}
	return 0, nil
}

func (m *MockStore) HIncrBy(ctx context.Context, key, field string, increment int64) (int64, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	if m.isExpired(key) {
		delete(m.hashes, key)
		delete(m.keyTypes, key)
		delete(m.expiresAt, key)
	}

	if t, ok := m.keyTypes[key]; ok && t != TypeHash {
		return 0, fmt.Errorf("WRONGTYPE Operation against a key holding the wrong kind of value")
	}

	if m.hashes[key] == nil {
		m.hashes[key] = make(map[string]string)
	}

	var currentValue int64 = 0
	if val, ok := m.hashes[key][field]; ok {
		var err error
		currentValue, err = strconv.ParseInt(val, 10, 64)
		if err != nil {
			return 0, fmt.Errorf("ERR hash value is not an integer")
		}
	}

	newValue := currentValue + increment
	m.hashes[key][field] = strconv.FormatInt(newValue, 10)
	m.keyTypes[key] = TypeHash

	return newValue, nil
}

// ============== List Commands ==============

func (m *MockStore) LPush(ctx context.Context, key string, values []string) (int64, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	if m.isExpired(key) {
		// Key expired
	}

	if t, ok := m.keyTypes[key]; ok && t != TypeList {
		return 0, fmt.Errorf("WRONGTYPE Operation against a key holding the wrong kind of value")
	}

	// Prepend values (in reverse order to match Redis behavior)
	newList := make([]string, len(values))
	for i, v := range values {
		newList[len(values)-1-i] = v
	}
	m.lists[key] = append(newList, m.lists[key]...)
	m.keyTypes[key] = TypeList

	return int64(len(m.lists[key])), nil
}

func (m *MockStore) RPush(ctx context.Context, key string, values []string) (int64, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	if m.isExpired(key) {
		// Key expired
	}

	if t, ok := m.keyTypes[key]; ok && t != TypeList {
		return 0, fmt.Errorf("WRONGTYPE Operation against a key holding the wrong kind of value")
	}

	m.lists[key] = append(m.lists[key], values...)
	m.keyTypes[key] = TypeList

	return int64(len(m.lists[key])), nil
}

func (m *MockStore) LPop(ctx context.Context, key string) (string, bool, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	if m.isExpired(key) {
		return "", false, nil
	}

	if t, ok := m.keyTypes[key]; ok && t != TypeList {
		return "", false, fmt.Errorf("WRONGTYPE Operation against a key holding the wrong kind of value")
	}

	list, ok := m.lists[key]
	if !ok || len(list) == 0 {
		return "", false, nil
	}

	val := list[0]
	m.lists[key] = list[1:]

	if len(m.lists[key]) == 0 {
		m.deleteKey(key)
	}

	return val, true, nil
}

func (m *MockStore) RPop(ctx context.Context, key string) (string, bool, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	if m.isExpired(key) {
		return "", false, nil
	}

	if t, ok := m.keyTypes[key]; ok && t != TypeList {
		return "", false, fmt.Errorf("WRONGTYPE Operation against a key holding the wrong kind of value")
	}

	list, ok := m.lists[key]
	if !ok || len(list) == 0 {
		return "", false, nil
	}

	val := list[len(list)-1]
	m.lists[key] = list[:len(list)-1]

	if len(m.lists[key]) == 0 {
		m.deleteKey(key)
	}

	return val, true, nil
}

func (m *MockStore) LLen(ctx context.Context, key string) (int64, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	if m.isExpired(key) {
		return 0, nil
	}

	if t, ok := m.keyTypes[key]; ok && t != TypeList {
		return 0, fmt.Errorf("WRONGTYPE Operation against a key holding the wrong kind of value")
	}

	return int64(len(m.lists[key])), nil
}

func (m *MockStore) LRange(ctx context.Context, key string, start, stop int64) ([]string, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	if m.isExpired(key) {
		return []string{}, nil
	}

	if t, ok := m.keyTypes[key]; ok && t != TypeList {
		return nil, fmt.Errorf("WRONGTYPE Operation against a key holding the wrong kind of value")
	}

	list, ok := m.lists[key]
	if !ok {
		return []string{}, nil
	}

	length := int64(len(list))
	if length == 0 {
		return []string{}, nil
	}

	// Handle negative indices
	if start < 0 {
		start = length + start
	}
	if stop < 0 {
		stop = length + stop
	}

	// Clamp
	if start < 0 {
		start = 0
	}
	if stop >= length {
		stop = length - 1
	}
	if start > stop || start >= length {
		return []string{}, nil
	}

	result := make([]string, stop-start+1)
	copy(result, list[start:stop+1])
	return result, nil
}

func (m *MockStore) LIndex(ctx context.Context, key string, index int64) (string, bool, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	if m.isExpired(key) {
		return "", false, nil
	}

	if t, ok := m.keyTypes[key]; ok && t != TypeList {
		return "", false, fmt.Errorf("WRONGTYPE Operation against a key holding the wrong kind of value")
	}

	list, ok := m.lists[key]
	if !ok {
		return "", false, nil
	}

	length := int64(len(list))
	if index < 0 {
		index = length + index
	}
	if index < 0 || index >= length {
		return "", false, nil
	}

	return list[index], true, nil
}

// ============== Set Commands ==============

func (m *MockStore) SAdd(ctx context.Context, key string, members []string) (int64, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	if m.isExpired(key) {
		// Key expired
	}

	if t, ok := m.keyTypes[key]; ok && t != TypeSet {
		return 0, fmt.Errorf("WRONGTYPE Operation against a key holding the wrong kind of value")
	}

	if m.sets[key] == nil {
		m.sets[key] = make(map[string]struct{})
	}

	var added int64
	for _, member := range members {
		if _, exists := m.sets[key][member]; !exists {
			m.sets[key][member] = struct{}{}
			added++
		}
	}

	m.keyTypes[key] = TypeSet
	return added, nil
}

func (m *MockStore) SRem(ctx context.Context, key string, members []string) (int64, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	if m.isExpired(key) {
		return 0, nil
	}

	if t, ok := m.keyTypes[key]; ok && t != TypeSet {
		return 0, fmt.Errorf("WRONGTYPE Operation against a key holding the wrong kind of value")
	}

	s, ok := m.sets[key]
	if !ok {
		return 0, nil
	}

	var removed int64
	for _, member := range members {
		if _, exists := s[member]; exists {
			delete(s, member)
			removed++
		}
	}

	if len(s) == 0 {
		m.deleteKey(key)
	}

	return removed, nil
}

func (m *MockStore) SMembers(ctx context.Context, key string) ([]string, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	if m.isExpired(key) {
		return nil, nil
	}

	if t, ok := m.keyTypes[key]; ok && t != TypeSet {
		return nil, fmt.Errorf("WRONGTYPE Operation against a key holding the wrong kind of value")
	}

	s, ok := m.sets[key]
	if !ok {
		return nil, nil
	}

	members := make([]string, 0, len(s))
	for member := range s {
		members = append(members, member)
	}
	sort.Strings(members)
	return members, nil
}

func (m *MockStore) SIsMember(ctx context.Context, key, member string) (bool, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	if m.isExpired(key) {
		return false, nil
	}

	if t, ok := m.keyTypes[key]; ok && t != TypeSet {
		return false, fmt.Errorf("WRONGTYPE Operation against a key holding the wrong kind of value")
	}

	if s, ok := m.sets[key]; ok {
		_, exists := s[member]
		return exists, nil
	}
	return false, nil
}

func (m *MockStore) SCard(ctx context.Context, key string) (int64, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	if m.isExpired(key) {
		return 0, nil
	}

	if t, ok := m.keyTypes[key]; ok && t != TypeSet {
		return 0, fmt.Errorf("WRONGTYPE Operation against a key holding the wrong kind of value")
	}

	if s, ok := m.sets[key]; ok {
		return int64(len(s)), nil
	}
	return 0, nil
}

// ============== Sorted Set Commands ==============

func (m *MockStore) ZAdd(ctx context.Context, key string, members []ZMember) (int64, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	if m.isExpired(key) {
		// Key expired, treat as non-existent
	} else if t, ok := m.keyTypes[key]; ok && t != TypeZSet {
		return 0, fmt.Errorf("WRONGTYPE Operation against a key holding the wrong kind of value")
	}

	if m.zsets[key] == nil {
		m.zsets[key] = make(map[string]float64)
	}

	var added int64
	for _, zm := range members {
		if _, exists := m.zsets[key][zm.Member]; !exists {
			added++
		}
		m.zsets[key][zm.Member] = zm.Score
	}

	m.keyTypes[key] = TypeZSet
	return added, nil
}

func (m *MockStore) ZRange(ctx context.Context, key string, start, stop int64, withScores bool) ([]ZMember, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	if m.isExpired(key) {
		return []ZMember{}, nil
	}

	if t, ok := m.keyTypes[key]; ok && t != TypeZSet {
		return nil, fmt.Errorf("WRONGTYPE Operation against a key holding the wrong kind of value")
	}

	zset, ok := m.zsets[key]
	if !ok || len(zset) == 0 {
		return []ZMember{}, nil
	}

	// Convert map to sorted slice
	members := make([]ZMember, 0, len(zset))
	for member, score := range zset {
		members = append(members, ZMember{Member: member, Score: score})
	}
	sort.Slice(members, func(i, j int) bool {
		if members[i].Score != members[j].Score {
			return members[i].Score < members[j].Score
		}
		return members[i].Member < members[j].Member
	})

	count := int64(len(members))
	if start < 0 {
		start = count + start
	}
	if stop < 0 {
		stop = count + stop
	}
	if start < 0 {
		start = 0
	}
	if stop >= count {
		stop = count - 1
	}
	if start > stop {
		return []ZMember{}, nil
	}

	return members[start : stop+1], nil
}

func (m *MockStore) ZScore(ctx context.Context, key, member string) (float64, bool, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	if m.isExpired(key) {
		return 0, false, nil
	}

	if t, ok := m.keyTypes[key]; ok && t != TypeZSet {
		return 0, false, fmt.Errorf("WRONGTYPE Operation against a key holding the wrong kind of value")
	}

	if zset, ok := m.zsets[key]; ok {
		if score, exists := zset[member]; exists {
			return score, true, nil
		}
	}
	return 0, false, nil
}

func (m *MockStore) ZRem(ctx context.Context, key string, members []string) (int64, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	if m.isExpired(key) {
		return 0, nil
	}

	if t, ok := m.keyTypes[key]; ok && t != TypeZSet {
		return 0, fmt.Errorf("WRONGTYPE Operation against a key holding the wrong kind of value")
	}

	zset, ok := m.zsets[key]
	if !ok {
		return 0, nil
	}

	var removed int64
	for _, member := range members {
		if _, exists := zset[member]; exists {
			delete(zset, member)
			removed++
		}
	}
	return removed, nil
}

func (m *MockStore) ZCard(ctx context.Context, key string) (int64, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	if m.isExpired(key) {
		return 0, nil
	}

	if t, ok := m.keyTypes[key]; ok && t != TypeZSet {
		return 0, fmt.Errorf("WRONGTYPE Operation against a key holding the wrong kind of value")
	}

	if zset, ok := m.zsets[key]; ok {
		return int64(len(zset)), nil
	}
	return 0, nil
}

func (m *MockStore) ZRangeByScore(ctx context.Context, key string, min, max float64, withScores bool, offset, count int64) ([]ZMember, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	if m.isExpired(key) {
		return nil, nil
	}

	if t, ok := m.keyTypes[key]; ok && t != TypeZSet {
		return nil, fmt.Errorf("WRONGTYPE Operation against a key holding the wrong kind of value")
	}

	zset, ok := m.zsets[key]
	if !ok {
		return nil, nil
	}

	// Collect members in score range
	var members []ZMember
	for member, score := range zset {
		if score >= min && score <= max {
			members = append(members, ZMember{Member: member, Score: score})
		}
	}

	// Sort by score, then by member
	sort.Slice(members, func(i, j int) bool {
		if members[i].Score != members[j].Score {
			return members[i].Score < members[j].Score
		}
		return members[i].Member < members[j].Member
	})

	// Apply offset and count
	if offset > 0 {
		if offset >= int64(len(members)) {
			return nil, nil
		}
		members = members[offset:]
	}
	if count > 0 && count < int64(len(members)) {
		members = members[:count]
	}

	return members, nil
}

func (m *MockStore) ZRemRangeByScore(ctx context.Context, key string, min, max float64) (int64, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	if m.isExpired(key) {
		return 0, nil
	}

	if t, ok := m.keyTypes[key]; ok && t != TypeZSet {
		return 0, fmt.Errorf("WRONGTYPE Operation against a key holding the wrong kind of value")
	}

	zset, ok := m.zsets[key]
	if !ok {
		return 0, nil
	}

	var removed int64
	for member, score := range zset {
		if score >= min && score <= max {
			delete(zset, member)
			removed++
		}
	}
	return removed, nil
}

func (m *MockStore) ZRemRangeByRank(ctx context.Context, key string, start, stop int64) (int64, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	if m.isExpired(key) {
		return 0, nil
	}

	if t, ok := m.keyTypes[key]; ok && t != TypeZSet {
		return 0, fmt.Errorf("WRONGTYPE Operation against a key holding the wrong kind of value")
	}

	zset, ok := m.zsets[key]
	if !ok {
		return 0, nil
	}

	// Get sorted members
	var members []ZMember
	for member, score := range zset {
		members = append(members, ZMember{Member: member, Score: score})
	}
	sort.Slice(members, func(i, j int) bool {
		if members[i].Score != members[j].Score {
			return members[i].Score < members[j].Score
		}
		return members[i].Member < members[j].Member
	})

	// Handle negative indices
	count := int64(len(members))
	if start < 0 {
		start = count + start
	}
	if stop < 0 {
		stop = count + stop
	}
	if start < 0 {
		start = 0
	}
	if stop >= count {
		stop = count - 1
	}
	if start > stop || start >= count {
		return 0, nil
	}

	// Remove members in range
	var removed int64
	for i := start; i <= stop; i++ {
		delete(zset, members[i].Member)
		removed++
	}
	return removed, nil
}

func (m *MockStore) ZIncrBy(ctx context.Context, key string, increment float64, member string) (float64, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	if m.isExpired(key) {
		delete(m.strings, key)
		delete(m.hashes, key)
		delete(m.lists, key)
		delete(m.sets, key)
		delete(m.zsets, key)
		delete(m.keyTypes, key)
		delete(m.expiresAt, key)
	}

	if t, ok := m.keyTypes[key]; ok && t != TypeZSet {
		return 0, fmt.Errorf("WRONGTYPE Operation against a key holding the wrong kind of value")
	}

	if m.zsets[key] == nil {
		m.zsets[key] = make(map[string]float64)
		m.keyTypes[key] = TypeZSet
	}

	m.zsets[key][member] += increment
	return m.zsets[key][member], nil
}

func (m *MockStore) ZPopMin(ctx context.Context, key string, count int64) ([]ZMember, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	if m.isExpired(key) {
		return nil, nil
	}

	if t, ok := m.keyTypes[key]; ok && t != TypeZSet {
		return nil, fmt.Errorf("WRONGTYPE Operation against a key holding the wrong kind of value")
	}

	zset, ok := m.zsets[key]
	if !ok || len(zset) == 0 {
		return nil, nil
	}

	// Get sorted members
	var members []ZMember
	for member, score := range zset {
		members = append(members, ZMember{Member: member, Score: score})
	}
	sort.Slice(members, func(i, j int) bool {
		if members[i].Score != members[j].Score {
			return members[i].Score < members[j].Score
		}
		return members[i].Member < members[j].Member
	})

	// Pop min members
	if count > int64(len(members)) {
		count = int64(len(members))
	}

	result := members[:count]
	for _, m2 := range result {
		delete(zset, m2.Member)
	}

	return result, nil
}

func (m *MockStore) LRem(ctx context.Context, key string, count int64, element string) (int64, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	if m.isExpired(key) {
		return 0, nil
	}

	if t, ok := m.keyTypes[key]; ok && t != TypeList {
		return 0, fmt.Errorf("WRONGTYPE Operation against a key holding the wrong kind of value")
	}

	list, ok := m.lists[key]
	if !ok {
		return 0, nil
	}

	var removed int64
	var newList []string

	if count == 0 {
		// Remove all occurrences
		for _, v := range list {
			if v == element {
				removed++
			} else {
				newList = append(newList, v)
			}
		}
	} else if count > 0 {
		// Remove count occurrences from head
		for _, v := range list {
			if v == element && removed < count {
				removed++
			} else {
				newList = append(newList, v)
			}
		}
	} else {
		// Remove -count occurrences from tail
		absCount := -count
		// Count occurrences
		var indices []int
		for i, v := range list {
			if v == element {
				indices = append(indices, i)
			}
		}
		// Keep only those to remove from tail
		removeSet := make(map[int]bool)
		for i := len(indices) - 1; i >= 0 && removed < absCount; i-- {
			removeSet[indices[i]] = true
			removed++
		}
		for i, v := range list {
			if !removeSet[i] {
				newList = append(newList, v)
			}
		}
	}

	m.lists[key] = newList
	return removed, nil
}

func (m *MockStore) LTrim(ctx context.Context, key string, start, stop int64) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	if m.isExpired(key) {
		return nil
	}

	if t, ok := m.keyTypes[key]; ok && t != TypeList {
		return fmt.Errorf("WRONGTYPE Operation against a key holding the wrong kind of value")
	}

	list, ok := m.lists[key]
	if !ok || len(list) == 0 {
		return nil
	}

	length := int64(len(list))

	// Normalize negative indices
	if start < 0 {
		start = length + start
	}
	if stop < 0 {
		stop = length + stop
	}

	// Bound to valid range
	if start < 0 {
		start = 0
	}
	if stop >= length {
		stop = length - 1
	}

	// If start > stop, delete entire list
	if start > stop {
		delete(m.lists, key)
		delete(m.keyTypes, key)
		delete(m.expiresAt, key)
		return nil
	}

	m.lists[key] = list[start : stop+1]
	return nil
}

func (m *MockStore) RPopLPush(ctx context.Context, source, destination string) (string, bool, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	if m.isExpired(source) {
		delete(m.lists, source)
		delete(m.keyTypes, source)
		delete(m.expiresAt, source)
	}

	if t, ok := m.keyTypes[source]; ok && t != TypeList {
		return "", false, fmt.Errorf("WRONGTYPE Operation against a key holding the wrong kind of value")
	}
	if t, ok := m.keyTypes[destination]; ok && t != TypeList {
		return "", false, fmt.Errorf("WRONGTYPE Operation against a key holding the wrong kind of value")
	}

	srcList, ok := m.lists[source]
	if !ok || len(srcList) == 0 {
		return "", false, nil
	}

	// Pop from right
	value := srcList[len(srcList)-1]
	m.lists[source] = srcList[:len(srcList)-1]

	// Push to left of destination
	if m.lists[destination] == nil {
		m.lists[destination] = []string{}
		m.keyTypes[destination] = TypeList
	}
	m.lists[destination] = append([]string{value}, m.lists[destination]...)

	return value, true, nil
}

// ============== HyperLogLog Commands ==============

func (m *MockStore) PFAdd(ctx context.Context, key string, elements []string) (int64, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	if m.isExpired(key) {
		delete(m.hyperloglogs, key)
		delete(m.keyTypes, key)
	}

	if t, ok := m.keyTypes[key]; ok && t != "hyperloglog" {
		return 0, fmt.Errorf("WRONGTYPE Operation against a key holding the wrong kind of value")
	}

	hll, ok := m.hyperloglogs[key]
	if !ok {
		hll = NewHyperLogLog()
		m.hyperloglogs[key] = hll
		m.keyTypes[key] = "hyperloglog"
	}

	changed := false
	for _, elem := range elements {
		if hll.Add(elem) {
			changed = true
		}
	}

	if changed {
		return 1, nil
	}
	return 0, nil
}

func (m *MockStore) PFCount(ctx context.Context, keys []string) (int64, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	if len(keys) == 0 {
		return 0, nil
	}

	if len(keys) == 1 {
		if m.isExpired(keys[0]) {
			return 0, nil
		}
		hll, ok := m.hyperloglogs[keys[0]]
		if !ok {
			return 0, nil
		}
		return hll.Count(), nil
	}

	// Multiple keys - merge then count
	merged := NewHyperLogLog()
	for _, key := range keys {
		if m.isExpired(key) {
			continue
		}
		hll, ok := m.hyperloglogs[key]
		if !ok {
			continue
		}
		merged.Merge(hll)
	}

	return merged.Count(), nil
}

func (m *MockStore) PFMerge(ctx context.Context, destKey string, sourceKeys []string) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	if t, ok := m.keyTypes[destKey]; ok && t != "hyperloglog" {
		return fmt.Errorf("WRONGTYPE Operation against a key holding the wrong kind of value")
	}

	// Start with dest key's existing HLL (if any)
	merged := NewHyperLogLog()
	if hll, ok := m.hyperloglogs[destKey]; ok && !m.isExpired(destKey) {
		merged.Merge(hll)
	}

	// Merge all source keys
	for _, key := range sourceKeys {
		if m.isExpired(key) {
			continue
		}
		if hll, ok := m.hyperloglogs[key]; ok {
			merged.Merge(hll)
		}
	}

	m.hyperloglogs[destKey] = merged
	m.keyTypes[destKey] = "hyperloglog"

	return nil
}

// ============== Server Commands ==============

func (m *MockStore) DBSize(ctx context.Context) (int64, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	var count int64
	for key := range m.keyTypes {
		if !m.isExpired(key) {
			count++
		}
	}
	return count, nil
}

func (m *MockStore) FlushDB(ctx context.Context) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.strings = make(map[string]string)
	m.hashes = make(map[string]map[string]string)
	m.lists = make(map[string][]string)
	m.sets = make(map[string]map[string]struct{})
	m.zsets = make(map[string]map[string]float64)
	m.hyperloglogs = make(map[string]*HyperLogLog)
	m.keyTypes = make(map[string]KeyType)
	m.expiresAt = make(map[string]time.Time)

	return nil
}

// Ensure MockStore implements Backend
var _ Backend = (*MockStore)(nil)
