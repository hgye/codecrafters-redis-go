package redis

import (
	"sync"
	"time"
)

type storeEntry struct {
	value     string
	expiresAt time.Time // zero value means no expiry
}

type Store struct {
	mu sync.RWMutex
	kv map[string]storeEntry
}

func NewStore() *Store {
	return &Store{kv: make(map[string]storeEntry)}
}

func (s *Store) Set(key, value string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.kv[key] = storeEntry{value: value}
}

func (s *Store) SetWithExpiry(key, value string, ttl time.Duration) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.kv[key] = storeEntry{value: value, expiresAt: time.Now().Add(ttl)}
}

func (s *Store) Keys() []string {
	s.mu.RLock()
	defer s.mu.RUnlock()
	keys := make([]string, 0, len(s.kv))
	now := time.Now()
	for k, e := range s.kv {
		if e.expiresAt.IsZero() || now.Before(e.expiresAt) {
			keys = append(keys, k)
		}
	}
	return keys
}

func (s *Store) Get(key string) (string, bool) {
	s.mu.RLock()
	e, ok := s.kv[key]
	if !ok {
		s.mu.RUnlock()
		return "", false
	}
	if e.expiresAt.IsZero() || time.Now().Before(e.expiresAt) {
		v := e.value
		s.mu.RUnlock()
		return v, true
	}
	s.mu.RUnlock()

	// Expired: delete under write lock (re-check to avoid races).
	s.mu.Lock()
	defer s.mu.Unlock()
	e, ok = s.kv[key]
	if !ok {
		return "", false
	}
	if !e.expiresAt.IsZero() && !time.Now().Before(e.expiresAt) {
		delete(s.kv, key)
		return "", false
	}
	return e.value, true
}
