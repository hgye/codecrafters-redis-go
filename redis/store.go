package redis

import (
	"fmt"
	"sync"
	"time"
)

type Store struct {
	mu   sync.RWMutex
	data map[string]Value
}

func NewStore() *Store {
	return &Store{data: make(map[string]Value)}
}

func (s *Store) bindValue(key string, value Value) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.data[key] = value
}

// activeValue returns the current value for key after lazy expiration cleanup.
func (s *Store) activeValue(key string) (Value, bool) {
	s.mu.RLock()
	v, ok := s.data[key]
	if !ok {
		s.mu.RUnlock()
		return nil, false
	}
	if sv, isString := v.(StringValue); isString {
		if !sv.ExpiresAt.IsZero() && !time.Now().Before(sv.ExpiresAt) {
			s.mu.RUnlock()

			s.mu.Lock()
			defer s.mu.Unlock()
			v2, exists := s.data[key]
			if !exists {
				return nil, false
			}
			if sv2, isString2 := v2.(StringValue); isString2 {
				if !sv2.ExpiresAt.IsZero() && !time.Now().Before(sv2.ExpiresAt) {
					delete(s.data, key)
					return nil, false
				}
			}
			return v2, true
		}
	}
	s.mu.RUnlock()
	return v, true
}

// getOrCreateStreamForWrite resolves a stream value for XADD.
// It creates one when key is missing, removes expired string values,
// and returns WRONGTYPE for active non-stream values.
func (s *Store) getOrCreateStreamForWrite(key string) (StreamValue, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	current, ok := s.data[key]
	if !ok {
		return StreamValue{}, nil
	}

	switch typed := current.(type) {
	case StreamValue:
		return typed, nil
	case StringValue:
		if !typed.ExpiresAt.IsZero() && !time.Now().Before(typed.ExpiresAt) {
			delete(s.data, key)
			return StreamValue{}, nil
		}
		return StreamValue{}, fmt.Errorf("WRONGTYPE Operation against a key holding the wrong kind of value")
	default:
		return StreamValue{}, fmt.Errorf("WRONGTYPE Operation against a key holding the wrong kind of value")
	}
}

func (s *Store) Set(key, value string) {
	s.bindValue(key, StringValue{Value: value})
}

func (s *Store) SetWithExpiry(key, value string, ttl time.Duration) {
	s.bindValue(key, StringValue{Value: value, ExpiresAt: time.Now().Add(ttl)})
}

func (s *Store) setStringAt(key, value string, expiresAt time.Time) {
	s.bindValue(key, StringValue{Value: value, ExpiresAt: expiresAt})
}

func (s *Store) AddStreamEntry(key, id string, fields []string) (string, error) {
	st, err := s.getOrCreateStreamForWrite(key)
	if err != nil {
		return "", err
	}

	ms, seq, generated, err := parseXAddID(id, st.LastMs, st.LastSeq)
	if err != nil {
		return "", err
	}

	if ms == 0 && seq == 0 {
		return "", fmt.Errorf("ERR The ID specified in XADD must be greater than 0-0")
	}
	if ms < st.LastMs || (ms == st.LastMs && seq <= st.LastSeq) {
		return "", fmt.Errorf("ERR The ID specified in XADD is equal or smaller than the target stream top item")
	}

	finalID := id
	if generated {
		finalID = fmt.Sprintf("%d-%d", ms, seq)
	}

	st.Entries = append(st.Entries, StreamEntry{ID: finalID, Fields: append([]string(nil), fields...)})
	st.LastMs = ms
	st.LastSeq = seq
	s.bindValue(key, st)

	return finalID, nil
}

func (s *Store) XRange(key, start, end string) ([]StreamEntry, error) {
	v, ok := s.activeValue(key)
	if !ok {
		return []StreamEntry{}, nil
	}

	st, isStream := v.(StreamValue)
	if !isStream {
		return nil, fmt.Errorf("WRONGTYPE Operation against a key holding the wrong kind of value")
	}

	startMs, startSeq, startLowerUnbounded, _, err := parseRangeBound(start, true)
	if err != nil {
		return nil, err
	}
	endMs, endSeq, _, endUpperUnbounded, err := parseRangeBound(end, false)
	if err != nil {
		return nil, err
	}

	result := make([]StreamEntry, 0)
	for _, entry := range st.Entries {
		ems, eseq, err := parseStreamID(entry.ID)
		if err != nil {
			continue
		}
		if !startLowerUnbounded && compareStreamID(ems, eseq, startMs, startSeq) < 0 {
			continue
		}
		if !endUpperUnbounded && compareStreamID(ems, eseq, endMs, endSeq) > 0 {
			continue
		}
		result = append(result, StreamEntry{ID: entry.ID, Fields: append([]string(nil), entry.Fields...)})
	}

	return result, nil
}

func (s *Store) Keys() []string {
	s.mu.RLock()
	keys := make([]string, 0, len(s.data))
	now := time.Now()
	for k, v := range s.data {
		switch typed := v.(type) {
		case StringValue:
			if typed.ExpiresAt.IsZero() || now.Before(typed.ExpiresAt) {
				keys = append(keys, k)
			}
		case StreamValue:
			keys = append(keys, k)
		}
	}
	s.mu.RUnlock()
	return keys
}

func (s *Store) Get(key string) (string, bool, bool) {
	v, ok := s.activeValue(key)
	if !ok {
		return "", false, false
	}

	switch typed := v.(type) {
	case StreamValue:
		return "", false, true
	case StringValue:
		return typed.Value, true, false
	default:
		return "", false, false
	}
}

func (s *Store) TypeOf(key string) string {
	v, ok := s.activeValue(key)
	if !ok {
		return "none"
	}
	switch v.(type) {
	case StreamValue:
		return "stream"
	case StringValue:
		return "string"
	default:
		return "none"
	}
}

func (s *Store) Delete(key string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.data, key)
}
