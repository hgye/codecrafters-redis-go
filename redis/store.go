package redis

import "sync"

type Store struct {
	mu sync.RWMutex
	kv map[string]string
}

func NewStore() *Store {
	return &Store{kv: make(map[string]string)}
}

func (s *Store) Set(key, value string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.kv[key] = value
}

func (s *Store) Get(key string) (string, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	v, ok := s.kv[key]
	return v, ok
}
