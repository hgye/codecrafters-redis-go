package redis

import (
	"encoding/binary"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"time"
)

// RDB opcodes
const (
	opAux          = 0xFA
	opSelectDB     = 0xFE
	opResizeDB     = 0xFB
	opExpireTimeMS = 0xFC
	opExpireTime   = 0xFD
	opEOF          = 0xFF
)

// LoadRDB reads an RDB file from the configured dir/dbfilename and populates
// the store with the key-value pairs found inside it.
func LoadRDB(cfg Config, store *Store) error {
	if cfg.Dir == "" || cfg.DBFilename == "" {
		return nil
	}
	path := filepath.Join(cfg.Dir, cfg.DBFilename)

	f, err := os.Open(path)
	if err != nil {
		if os.IsNotExist(err) {
			return nil // no RDB file is fine
		}
		return fmt.Errorf("open rdb: %w", err)
	}
	defer f.Close()

	return parseRDB(f, store)
}

func parseRDB(r io.Reader, store *Store) error {
	// --- Header: "REDIS0011" (9 bytes) ---
	header := make([]byte, 9)
	if _, err := io.ReadFull(r, header); err != nil {
		return fmt.Errorf("read header: %w", err)
	}
	if string(header[:5]) != "REDIS" {
		return fmt.Errorf("invalid RDB magic: %q", header[:5])
	}

	// --- Body ---
	for {
		typeByte, err := readByte(r)
		if err != nil {
			return fmt.Errorf("read opcode: %w", err)
		}

		switch typeByte {
		case opAux:
			// Auxiliary field: skip key and value
			if _, err := readRDBString(r); err != nil {
				return err
			}
			if _, err := readRDBString(r); err != nil {
				return err
			}

		case opSelectDB:
			// Database number (length-encoded)
			if _, err := readLength(r); err != nil {
				return err
			}

		case opResizeDB:
			// Hash table sizes: db size + expires size
			if _, err := readLength(r); err != nil {
				return err
			}
			if _, err := readLength(r); err != nil {
				return err
			}

		case opExpireTime:
			// 4-byte unsigned int (seconds since epoch), little-endian
			var ts uint32
			if err := binary.Read(r, binary.LittleEndian, &ts); err != nil {
				return fmt.Errorf("read expire time: %w", err)
			}
			expiresAt := time.Unix(int64(ts), 0)
			if err := readKeyValue(r, store, expiresAt); err != nil {
				return err
			}

		case opExpireTimeMS:
			// 8-byte unsigned int (milliseconds since epoch), little-endian
			var tsMs uint64
			if err := binary.Read(r, binary.LittleEndian, &tsMs); err != nil {
				return fmt.Errorf("read expire time ms: %w", err)
			}
			expiresAt := time.UnixMilli(int64(tsMs))
			if err := readKeyValue(r, store, expiresAt); err != nil {
				return err
			}

		case opEOF:
			// Done; ignore trailing CRC
			return nil

		default:
			// This byte is a value-type indicator; read key + value directly
			if err := readKeyValueWithType(typeByte, r, store, time.Time{}); err != nil {
				return err
			}
		}
	}
}

// readKeyValue reads the value-type byte followed by key and value, then stores the entry.
func readKeyValue(r io.Reader, store *Store, expiresAt time.Time) error {
	typeByte, err := readByte(r)
	if err != nil {
		return fmt.Errorf("read value type: %w", err)
	}
	return readKeyValueWithType(typeByte, r, store, expiresAt)
}

func readKeyValueWithType(valueType byte, r io.Reader, store *Store, expiresAt time.Time) error {
	if valueType != 0 {
		return fmt.Errorf("unsupported value type: %d", valueType)
	}
	key, err := readRDBString(r)
	if err != nil {
		return fmt.Errorf("read key: %w", err)
	}
	value, err := readRDBString(r)
	if err != nil {
		return fmt.Errorf("read value: %w", err)
	}

	store.mu.Lock()
	store.kv[key] = storeEntry{value: value, expiresAt: expiresAt}
	store.mu.Unlock()
	return nil
}

// --- low-level helpers ---

func readByte(r io.Reader) (byte, error) {
	buf := make([]byte, 1)
	if _, err := io.ReadFull(r, buf); err != nil {
		return 0, err
	}
	return buf[0], nil
}

// readLength reads a length-encoded integer.
// It also handles special encodings (integers stored as strings).
// Returns (value, error). For special int encodings, returns the integer value.
func readLength(r io.Reader) (int, error) {
	first, err := readByte(r)
	if err != nil {
		return 0, err
	}

	switch (first & 0xC0) >> 6 {
	case 0: // 00xxxxxx — 6-bit length
		return int(first & 0x3F), nil
	case 1: // 01xxxxxx — 14-bit length
		next, err := readByte(r)
		if err != nil {
			return 0, err
		}
		return int(first&0x3F)<<8 | int(next), nil
	case 2: // 10xxxxxx — 32-bit length (big-endian)
		buf := make([]byte, 4)
		if _, err := io.ReadFull(r, buf); err != nil {
			return 0, err
		}
		return int(binary.BigEndian.Uint32(buf)), nil
	case 3: // 11xxxxxx — special encoding
		return int(first & 0x3F), nil
	}
	return 0, fmt.Errorf("unexpected length encoding")
}

// readRDBString reads a length-prefixed string or a special-encoded integer string.
func readRDBString(r io.Reader) (string, error) {
	first, err := readByte(r)
	if err != nil {
		return "", err
	}

	encType := (first & 0xC0) >> 6
	if encType == 3 {
		return readIntegerString(first, r)
	}
	return readLengthPrefixedString(first, r)
}

// readIntegerString decodes a special-encoded integer (0xC0..0xC2) as a string.
func readIntegerString(first byte, r io.Reader) (string, error) {
	format := first & 0x3F
	switch format {
	case 0: // 8-bit integer
		b, err := readByte(r)
		if err != nil {
			return "", err
		}
		return fmt.Sprintf("%d", int8(b)), nil
	case 1: // 16-bit little-endian integer
		buf := make([]byte, 2)
		if _, err := io.ReadFull(r, buf); err != nil {
			return "", err
		}
		return fmt.Sprintf("%d", int16(binary.LittleEndian.Uint16(buf))), nil
	case 2: // 32-bit little-endian integer
		buf := make([]byte, 4)
		if _, err := io.ReadFull(r, buf); err != nil {
			return "", err
		}
		return fmt.Sprintf("%d", int32(binary.LittleEndian.Uint32(buf))), nil
	default:
		return "", fmt.Errorf("unsupported special string encoding: %d", format)
	}
}

// readLengthPrefixedString reads a regular length-prefixed string given the first byte already consumed.
func readLengthPrefixedString(first byte, r io.Reader) (string, error) {
	encType := (first & 0xC0) >> 6
	var length int
	switch encType {
	case 0:
		length = int(first & 0x3F)
	case 1:
		next, err := readByte(r)
		if err != nil {
			return "", err
		}
		length = int(first&0x3F)<<8 | int(next)
	case 2:
		buf := make([]byte, 4)
		if _, err := io.ReadFull(r, buf); err != nil {
			return "", err
		}
		length = int(binary.BigEndian.Uint32(buf))
	}

	data := make([]byte, length)
	if _, err := io.ReadFull(r, data); err != nil {
		return "", err
	}
	return string(data), nil
}
