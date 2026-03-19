package redis

import (
	"fmt"
	"math"
	"sort"
	"strconv"
	"sync"
	"time"
)

type Store struct {
	mu   sync.RWMutex
	data map[string]Value
}

type XReadStreamResult struct {
	Key     string
	Entries []StreamEntry
}

type ZSetEntry struct {
	Member string
	Score  float64
}

type indexRange struct {
	Start int
	Stop  int
}

func (r indexRange) normalize(length int) (start int, stop int, ok bool) {
	if length == 0 {
		return 0, 0, false
	}

	start = r.Start
	stop = r.Stop

	if start < 0 {
		start = length + start
	}
	if stop < 0 {
		stop = length + stop
	}

	if start < 0 {
		start = 0
	}
	if stop < 0 {
		return 0, 0, false
	}

	if start >= length {
		return 0, 0, false
	}
	if stop >= length {
		stop = length - 1
	}
	if start > stop {
		return 0, 0, false
	}

	return start, stop, true
}

func NewStore() *Store {
	return &Store{data: make(map[string]Value)}
}

func (s *Store) bindValue(key string, value Value) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.data[key] = value
}

func isExpiredString(v StringValue) bool {
	return !v.ExpiresAt.IsZero() && !time.Now().Before(v.ExpiresAt)
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
		if isExpiredString(sv) {
			s.mu.RUnlock()

			s.mu.Lock()
			defer s.mu.Unlock()
			v2, exists := s.data[key]
			if !exists {
				return nil, false
			}
			if sv2, isString2 := v2.(StringValue); isString2 {
				if isExpiredString(sv2) {
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
		if isExpiredString(typed) {
			delete(s.data, key)
			return StreamValue{}, nil
		}
		return StreamValue{}, fmt.Errorf("WRONGTYPE Operation against a key holding the wrong kind of value")
	default:
		return StreamValue{}, fmt.Errorf("WRONGTYPE Operation against a key holding the wrong kind of value")
	}
}

// getOrCreateListForWrite resolves a list value for RPUSH.
// It creates one when key is missing, removes expired string values,
// and returns WRONGTYPE for active non-list values.
func (s *Store) getOrCreateListForWrite(key string) (ListValue, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	current, ok := s.data[key]
	if !ok {
		return ListValue{}, nil
	}

	switch typed := current.(type) {
	case ListValue:
		return typed, nil
	case StringValue:
		if isExpiredString(typed) {
			delete(s.data, key)
			return ListValue{}, nil
		}
		return ListValue{}, fmt.Errorf("WRONGTYPE Operation against a key holding the wrong kind of value")
	default:
		return ListValue{}, fmt.Errorf("WRONGTYPE Operation against a key holding the wrong kind of value")
	}
}

// getOrCreateZSetForWrite resolves a sorted set value for ZADD.
// It creates one when key is missing, removes expired string values,
// and returns WRONGTYPE for active non-zset values.
func (s *Store) getOrCreateZSetForWrite(key string) (ZSetValue, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	current, ok := s.data[key]
	if !ok {
		return ZSetValue{Scores: make(map[string]float64)}, nil
	}

	switch typed := current.(type) {
	case ZSetValue:
		if typed.Scores == nil {
			typed.Scores = make(map[string]float64)
		}
		return typed, nil
	case StringValue:
		if isExpiredString(typed) {
			delete(s.data, key)
			return ZSetValue{Scores: make(map[string]float64)}, nil
		}
		return ZSetValue{}, fmt.Errorf("WRONGTYPE Operation against a key holding the wrong kind of value")
	default:
		return ZSetValue{}, fmt.Errorf("WRONGTYPE Operation against a key holding the wrong kind of value")
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

func (s *Store) ZAdd(key string, entries []ZSetEntry) (int64, error) {
	if len(entries) == 0 {
		return 0, fmt.Errorf("ERR wrong number of arguments for 'zadd' command")
	}

	zv, err := s.getOrCreateZSetForWrite(key)
	if err != nil {
		return 0, err
	}

	added := int64(0)
	for _, entry := range entries {
		if _, exists := zv.Scores[entry.Member]; !exists {
			added++
		}
		zv.Scores[entry.Member] = entry.Score
	}

	s.bindValue(key, zv)
	return added, nil
}

func (s *Store) ZRank(key, member string) (int64, bool, error) {
	v, ok := s.activeValue(key)
	if !ok {
		return 0, false, nil
	}

	zv, isZSet := v.(ZSetValue)
	if !isZSet {
		return 0, false, fmt.Errorf("WRONGTYPE Operation against a key holding the wrong kind of value")
	}

	if _, exists := zv.Scores[member]; !exists {
		return 0, false, nil
	}

	entries := make([]ZSetEntry, 0, len(zv.Scores))
	for m, score := range zv.Scores {
		entries = append(entries, ZSetEntry{Member: m, Score: score})
	}

	sort.Slice(entries, func(i, j int) bool {
		if entries[i].Score == entries[j].Score {
			return entries[i].Member < entries[j].Member
		}
		return entries[i].Score < entries[j].Score
	})

	for i, entry := range entries {
		if entry.Member == member {
			return int64(i), true, nil
		}
	}

	return 0, false, nil
}

func (s *Store) ZRange(key string, start, stop int, withScores bool) ([]string, error) {
	v, ok := s.activeValue(key)
	if !ok {
		return []string{}, nil
	}

	zv, isZSet := v.(ZSetValue)
	if !isZSet {
		return nil, fmt.Errorf("WRONGTYPE Operation against a key holding the wrong kind of value")
	}

	entries := make([]ZSetEntry, 0, len(zv.Scores))
	for m, score := range zv.Scores {
		entries = append(entries, ZSetEntry{Member: m, Score: score})
	}

	sort.Slice(entries, func(i, j int) bool {
		if entries[i].Score == entries[j].Score {
			return entries[i].Member < entries[j].Member
		}
		return entries[i].Score < entries[j].Score
	})

	n := len(entries)
	r := indexRange{Start: start, Stop: stop}
	from, to, ok := r.normalize(n)
	if !ok {
		return []string{}, nil
	}

	selected := entries[from : to+1]
	if !withScores {
		members := make([]string, 0, len(selected))
		for _, entry := range selected {
			members = append(members, entry.Member)
		}
		return members, nil
	}

	out := make([]string, 0, len(selected)*2)
	for _, entry := range selected {
		out = append(out, entry.Member)
		out = append(out, strconv.FormatFloat(entry.Score, 'g', -1, 64))
	}
	return out, nil
}

func (s *Store) ZCard(key string) (int64, error) {
	v, ok := s.activeValue(key)
	if !ok {
		return 0, nil
	}

	zv, isZSet := v.(ZSetValue)
	if !isZSet {
		return 0, fmt.Errorf("WRONGTYPE Operation against a key holding the wrong kind of value")
	}

	return int64(len(zv.Scores)), nil
}

func (s *Store) ZScore(key, member string) (float64, bool, error) {
	v, ok := s.activeValue(key)
	if !ok {
		return 0, false, nil
	}

	zv, isZSet := v.(ZSetValue)
	if !isZSet {
		return 0, false, fmt.Errorf("WRONGTYPE Operation against a key holding the wrong kind of value")
	}

	score, exists := zv.Scores[member]
	if !exists {
		return 0, false, nil
	}

	return score, true, nil
}

func (s *Store) ZRem(key string, members []string) (int64, error) {
	if len(members) == 0 {
		return 0, fmt.Errorf("ERR wrong number of arguments for 'zrem' command")
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	current, ok := s.data[key]
	if !ok {
		return 0, nil
	}

	zv, isZSet := current.(ZSetValue)
	if !isZSet {
		if sv, isString := current.(StringValue); isString {
			if isExpiredString(sv) {
				delete(s.data, key)
				return 0, nil
			}
		}
		return 0, fmt.Errorf("WRONGTYPE Operation against a key holding the wrong kind of value")
	}

	removed := int64(0)
	for _, member := range members {
		if _, exists := zv.Scores[member]; exists {
			delete(zv.Scores, member)
			removed++
		}
	}

	if len(zv.Scores) == 0 {
		delete(s.data, key)
	} else {
		s.data[key] = zv
	}

	return removed, nil
}

func (s *Store) RPush(key string, values []string) (int64, error) {
	if len(values) == 0 {
		return 0, fmt.Errorf("ERR wrong number of arguments for 'rpush' command")
	}

	lv, err := s.getOrCreateListForWrite(key)
	if err != nil {
		return 0, err
	}

	lv.Items = append(lv.Items, values...)
	s.bindValue(key, lv)
	return int64(len(lv.Items)), nil
}

func (s *Store) LPush(key string, values []string) (int64, error) {
	if len(values) == 0 {
		return 0, fmt.Errorf("ERR wrong number of arguments for 'lpush' command")
	}

	lv, err := s.getOrCreateListForWrite(key)
	if err != nil {
		return 0, err
	}

	// Redis LPUSH pushes values from left to right to the head.
	newItems := make([]string, 0, len(values)+len(lv.Items))
	for i := len(values) - 1; i >= 0; i-- {
		newItems = append(newItems, values[i])
	}
	newItems = append(newItems, lv.Items...)
	lv.Items = newItems
	s.bindValue(key, lv)
	return int64(len(lv.Items)), nil
}

func (s *Store) LRange(key string, start, stop int) ([]string, error) {
	v, ok := s.activeValue(key)
	if !ok {
		return []string{}, nil
	}

	lv, isList := v.(ListValue)
	if !isList {
		return nil, fmt.Errorf("WRONGTYPE Operation against a key holding the wrong kind of value")
	}

	n := len(lv.Items)
	r := indexRange{Start: start, Stop: stop}
	from, to, ok := r.normalize(n)
	if !ok {
		return []string{}, nil
	}

	res := append([]string(nil), lv.Items[from:to+1]...)
	return res, nil
}

func (s *Store) LLen(key string) (int64, error) {
	v, ok := s.activeValue(key)
	if !ok {
		return 0, nil
	}

	lv, isList := v.(ListValue)
	if !isList {
		return 0, fmt.Errorf("WRONGTYPE Operation against a key holding the wrong kind of value")
	}

	return int64(len(lv.Items)), nil
}

func (s *Store) LPopCount(key string, count int) ([]string, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	current, ok := s.data[key]
	if !ok {
		return []string{}, nil
	}

	switch typed := current.(type) {
	case ListValue:
		if len(typed.Items) == 0 {
			delete(s.data, key)
			return []string{}, nil
		}

		n := count
		if n > len(typed.Items) {
			n = len(typed.Items)
		}

		popped := append([]string(nil), typed.Items[:n]...)
		typed.Items = typed.Items[n:]
		if len(typed.Items) == 0 {
			delete(s.data, key)
		} else {
			s.data[key] = typed
		}
		return popped, nil
	case StringValue:
		if isExpiredString(typed) {
			delete(s.data, key)
			return []string{}, nil
		}
		return nil, fmt.Errorf("WRONGTYPE Operation against a key holding the wrong kind of value")
	default:
		return nil, fmt.Errorf("WRONGTYPE Operation against a key holding the wrong kind of value")
	}
}

func (s *Store) LPop(key string) (string, bool, error) {
	popped, err := s.LPopCount(key, 1)
	if err != nil {
		return "", false, err
	}
	if len(popped) == 0 {
		return "", false, nil
	}
	return popped[0], true, nil
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

func (s *Store) ResolveXReadStartIDs(keys []string, rawIDs []string) ([]string, error) {
	if len(keys) != len(rawIDs) {
		return nil, fmt.Errorf("ERR Unbalanced XREAD list of streams")
	}

	resolved := make([]string, len(rawIDs))
	for i := range keys {
		if rawIDs[i] != "$" {
			resolved[i] = rawIDs[i]
			continue
		}

		v, ok := s.activeValue(keys[i])
		if !ok {
			resolved[i] = "0-0"
			continue
		}

		st, isStream := v.(StreamValue)
		if !isStream {
			return nil, fmt.Errorf("WRONGTYPE Operation against a key holding the wrong kind of value")
		}
		resolved[i] = fmt.Sprintf("%d-%d", st.LastMs, st.LastSeq)
	}

	return resolved, nil
}

func (s *Store) XRead(keys []string, startIDs []string, count int) ([]XReadStreamResult, error) {
	if len(keys) != len(startIDs) {
		return nil, fmt.Errorf("ERR Unbalanced XREAD list of streams")
	}

	results := make([]XReadStreamResult, 0, len(keys))
	for i := range keys {
		v, ok := s.activeValue(keys[i])
		if !ok {
			continue
		}
		st, isStream := v.(StreamValue)
		if !isStream {
			return nil, fmt.Errorf("WRONGTYPE Operation against a key holding the wrong kind of value")
		}

		startMs, startSeq, err := parseXReadStartID(startIDs[i])
		if err != nil {
			return nil, err
		}

		entries := make([]StreamEntry, 0)
		for _, entry := range st.Entries {
			ems, eseq, err := parseStreamID(entry.ID)
			if err != nil {
				continue
			}
			if compareStreamID(ems, eseq, startMs, startSeq) <= 0 {
				continue
			}
			entries = append(entries, StreamEntry{ID: entry.ID, Fields: append([]string(nil), entry.Fields...)})
			if count > 0 && len(entries) >= count {
				break
			}
		}

		if len(entries) > 0 {
			results = append(results, XReadStreamResult{Key: keys[i], Entries: entries})
		}
	}

	return results, nil
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
		case ListValue:
			keys = append(keys, k)
		case ZSetValue:
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

	if typed, isString := v.(StringValue); isString {
		return typed.Value, true, false
	}

	// Any existing non-string type is a WRONGTYPE for GET.
	return "", false, true
}

func (s *Store) TypeOf(key string) string {
	v, ok := s.activeValue(key)
	if !ok {
		return "none"
	}
	return v.Kind()
}

func (s *Store) Delete(key string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.data, key)
}

func (s *Store) Incr(key string) (int64, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	current, ok := s.data[key]
	if !ok {
		s.data[key] = StringValue{Value: "1"}
		return 1, nil
	}

	typed, isString := current.(StringValue)
	if !isString {
		return 0, fmt.Errorf("WRONGTYPE Operation against a key holding the wrong kind of value")
	}

	if isExpiredString(typed) {
		s.data[key] = StringValue{Value: "1"}
		return 1, nil
	}

	n, err := strconv.ParseInt(typed.Value, 10, 64)
	if err != nil {
		return 0, fmt.Errorf("ERR value is not an integer or out of range")
	}
	if n == math.MaxInt64 {
		return 0, fmt.Errorf("ERR increment or decrement would overflow")
	}

	n++
	typed.Value = strconv.FormatInt(n, 10)
	s.data[key] = typed
	return n, nil
}
