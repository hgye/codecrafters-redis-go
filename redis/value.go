package redis

import (
	"fmt"
	"strconv"
	"strings"
	"time"
)

type Value interface {
	isValue()
}

type StringValue struct {
	Value     string
	ExpiresAt time.Time // zero value means no expiry
}

func (StringValue) isValue() {}

type StreamEntry struct {
	ID     string
	Fields []string // alternating field/value pairs
}

type StreamValue struct {
	Entries []StreamEntry
	LastMs  uint64
	LastSeq uint64
}

func (StreamValue) isValue() {}

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
