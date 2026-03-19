package redis

import (
	"fmt"
	"math"
	"strconv"
	"strings"
	"time"
)

type Value interface {
	Kind() string
}

type StringValue struct {
	Value     string
	ExpiresAt time.Time // zero value means no expiry
}

func (StringValue) Kind() string { return "string" }

type StreamEntry struct {
	ID     string
	Fields []string // alternating field/value pairs
}

type StreamValue struct {
	Entries []StreamEntry
	LastMs  uint64
	LastSeq uint64
}

func (StreamValue) Kind() string { return "stream" }

type ListValue struct {
	Items []string
}

func (ListValue) Kind() string { return "list" }

func parseXAddID(raw string, lastMs, lastSeq uint64) (ms uint64, seq uint64, generated bool, err error) {
	if raw == "*" {
		nowMs := uint64(time.Now().UnixMilli())
		if nowMs > lastMs {
			return nowMs, 0, true, nil
		}
		return lastMs, lastSeq + 1, true, nil
	}

	parts := strings.Split(raw, "-")
	if len(parts) != 2 {
		return 0, 0, false, fmt.Errorf("ERR Invalid stream ID specified as stream command argument")
	}

	parsedMs, err := strconv.ParseUint(parts[0], 10, 64)
	if err != nil {
		return 0, 0, false, fmt.Errorf("ERR Invalid stream ID specified as stream command argument")
	}

	if parts[1] == "*" {
		if parsedMs < lastMs {
			return 0, 0, false, fmt.Errorf("ERR The ID specified in XADD is equal or smaller than the target stream top item")
		}
		if parsedMs == lastMs {
			return parsedMs, lastSeq + 1, true, nil
		}
		return parsedMs, 0, true, nil
	}

	parsedSeq, err := strconv.ParseUint(parts[1], 10, 64)
	if err != nil {
		return 0, 0, false, fmt.Errorf("ERR Invalid stream ID specified as stream command argument")
	}

	return parsedMs, parsedSeq, false, nil
}

func parseStreamID(raw string) (ms uint64, seq uint64, err error) {
	parts := strings.Split(raw, "-")
	if len(parts) != 2 {
		return 0, 0, fmt.Errorf("ERR Invalid stream ID specified as stream command argument")
	}
	ms, err = strconv.ParseUint(parts[0], 10, 64)
	if err != nil {
		return 0, 0, fmt.Errorf("ERR Invalid stream ID specified as stream command argument")
	}
	seq, err = strconv.ParseUint(parts[1], 10, 64)
	if err != nil {
		return 0, 0, fmt.Errorf("ERR Invalid stream ID specified as stream command argument")
	}
	return ms, seq, nil
}

func parseRangeBound(raw string, isStart bool) (ms uint64, seq uint64, lowerUnbounded bool, upperUnbounded bool, err error) {
	switch raw {
	case "-":
		return 0, 0, true, false, nil
	case "+":
		return 0, 0, false, true, nil
	}

	if !strings.Contains(raw, "-") {
		ms, err = strconv.ParseUint(raw, 10, 64)
		if err != nil {
			return 0, 0, false, false, fmt.Errorf("ERR Invalid stream ID specified as stream command argument")
		}
		if isStart {
			return ms, 0, false, false, nil
		}
		return ms, math.MaxUint64, false, false, nil
	}

	ms, seq, err = parseStreamID(raw)
	if err != nil {
		return 0, 0, false, false, err
	}
	return ms, seq, false, false, nil
}

func compareStreamID(msA, seqA, msB, seqB uint64) int {
	if msA < msB {
		return -1
	}
	if msA > msB {
		return 1
	}
	if seqA < seqB {
		return -1
	}
	if seqA > seqB {
		return 1
	}
	return 0
}

func parseXReadStartID(raw string) (ms uint64, seq uint64, err error) {
	if !strings.Contains(raw, "-") {
		ms, err = strconv.ParseUint(raw, 10, 64)
		if err != nil {
			return 0, 0, fmt.Errorf("ERR Invalid stream ID specified as stream command argument")
		}
		return ms, 0, nil
	}
	return parseStreamID(raw)
}
