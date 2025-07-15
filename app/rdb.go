package main

import (
	"encoding/binary"
	"fmt"
	"io"
	"os"
	"time"
)

// LoadRDBFile loads an RDB file and returns the key-value and expiry maps.
func LoadRDBFile(path string) (map[string]string, map[string]time.Time, error) {
	file, err := os.Open(path)
	if err != nil {
		return map[string]string{}, map[string]time.Time{}, err
	}
	defer file.Close()

	kv := make(map[string]string)
	expiry := make(map[string]time.Time)
	var (
		rdbVersion int
	)

	// 1. Header: REDISxxxx
	header := make([]byte, 9)
	if _, err := io.ReadFull(file, header); err != nil {
		return kv, expiry, fmt.Errorf("failed to read header: %w", err)
	}
	if string(header[:5]) != "REDIS" {
		return kv, expiry, fmt.Errorf("invalid RDB header")
	}
	rdbVersion = int(binary.BigEndian.Uint32(header[5:9]))
	_ = rdbVersion // For future use

	// 2. Metadata section (skip for now, not needed for single key)
	for {
		b, err := readByte(file)
		if err != nil {
			return kv, expiry, err
		}
		if b == 0xFA { // metadata section
			_, err := readRDBString(file) // name
			if err != nil {
				return kv, expiry, err
			}
			_, err = readRDBString(file) // value
			if err != nil {
				return kv, expiry, err
			}
			continue
		}
		if b == 0xFE { // database section
			// 3. Database index (size encoded)
			if _, err := readRDBLength(file); err != nil {
				return kv, expiry, fmt.Errorf("skipping DB index: %w", err)
			}

			// 4. Expect the 0xFB hash‐table marker
			marker, err := readByte(file)
			if err != nil {
				return kv, expiry, fmt.Errorf("reading hash‐table marker: %w", err)
			}
			if marker != 0xFB {
				return kv, expiry, fmt.Errorf("expected 0xFB, got 0x%X", marker)
			}

			// 5. Skip the two size‐encoded lengths: main table and expire table
			if _, err := readRDBLength(file); err != nil {
				return kv, expiry, fmt.Errorf("skipping main table size: %w", err)
			}
			if _, err := readRDBLength(file); err != nil {
				return kv, expiry, fmt.Errorf("skipping expire table size: %w", err)
			}
			// 6. Now read key-value pairs
			var (
				expireAt time.Time
			)
			for {
				b, err := readByte(file)
				if err != nil {
					return kv, expiry, err
				}
				if b == 0xFF {
					return kv, expiry, nil // End of file section
				}
				if b == 0xFC { // expire in ms
					t, err := readUint64LE(file)
					if err != nil {
						return kv, expiry, err
					}
					expireAt = time.Unix(0, int64(t)*int64(time.Millisecond))
					continue
				}
				if b == 0xFD { // expire in s
					t, err := readUint32LE(file)
					if err != nil {
						return kv, expiry, err
					}
					expireAt = time.Unix(int64(t), 0)
					continue
				}
				if b == 0x00 { // string type
					key, err := readRDBString(file)
					if err != nil {
						return kv, expiry, err
					}
					val, err := readRDBString(file)
					if err != nil {
						return kv, expiry, err
					}
					kv[key] = val
					if !expireAt.IsZero() {
						expiry[key] = expireAt
						expireAt = time.Time{}
					}
					continue
				}
				// Ignore other types for now
			}
		}
		if b == 0xFF {
			break
		}
	}
	return kv, expiry, nil
}

// --- Helper functions ---

func readByte(r io.Reader) (byte, error) {
	var b [1]byte
	_, err := io.ReadFull(r, b[:])
	return b[0], err
}

func readUint32LE(r io.Reader) (uint32, error) {
	var b [4]byte
	_, err := io.ReadFull(r, b[:])
	return binary.LittleEndian.Uint32(b[:]), err
}

func readUint64LE(r io.Reader) (uint64, error) {
	var b [8]byte
	_, err := io.ReadFull(r, b[:])
	return binary.LittleEndian.Uint64(b[:]), err
}

// Reads a size-encoded value (see RDB spec)
func readRDBLength(r io.Reader) (int, error) {
	b, err := readByte(r)
	if err != nil {
		return 0, err
	}
	typeBits := b >> 6
	if typeBits == 0 {
		return int(b & 0x3F), nil
	} else if typeBits == 1 {
		b2, err := readByte(r)
		if err != nil {
			return 0, err
		}
		return int(((uint16(b & 0x3F)) << 8) | uint16(b2)), nil
	} else if typeBits == 2 {
		var buf [4]byte
		_, err := io.ReadFull(r, buf[:])
		if err != nil {
			return 0, err
		}
		return int(binary.BigEndian.Uint32(buf[:])), nil
	} else {
		// 0b11: string encoding, not supported for length
		return 0, fmt.Errorf("unsupported length encoding")
	}
}

// Reads a string-encoded value (see RDB spec)
func readRDBString(r io.Reader) (string, error) {
	b, err := readByte(r)
	if err != nil {
		return "", err
	}
	typeBits := b >> 6
	if typeBits == 0 {
		length := int(b & 0x3F)
		buf := make([]byte, length)
		_, err := io.ReadFull(r, buf)
		return string(buf), err
	} else if typeBits == 1 {
		b2, err := readByte(r)
		if err != nil {
			return "", err
		}
		length := int(((uint16(b & 0x3F)) << 8) | uint16(b2))
		buf := make([]byte, length)
		_, err = io.ReadFull(r, buf)
		return string(buf), err
	} else if typeBits == 2 {
		var buf [4]byte
		_, err := io.ReadFull(r, buf[:])
		if err != nil {
			return "", err
		}
		length := int(binary.BigEndian.Uint32(buf[:]))
		data := make([]byte, length)
		_, err = io.ReadFull(r, data)
		return string(data), err
	} else {
		// 0b11: integer or compressed string encoding
		encType := b & 0x3F
		if encType == 0 { // 8-bit int
			v, err := readByte(r)
			return fmt.Sprintf("%d", v), err
		} else if encType == 1 { // 16-bit int
			var buf [2]byte
			_, err := io.ReadFull(r, buf[:])
			v := binary.LittleEndian.Uint16(buf[:])
			return fmt.Sprintf("%d", v), err
		} else if encType == 2 { // 32-bit int
			var buf [4]byte
			_, err := io.ReadFull(r, buf[:])
			v := binary.LittleEndian.Uint32(buf[:])
			return fmt.Sprintf("%d", v), err
		} else {
			return "", fmt.Errorf("unsupported string encoding type: %d", encType)
		}
	}
}
