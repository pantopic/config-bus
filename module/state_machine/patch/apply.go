package patch

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
)

var ErrCorrupt = fmt.Errorf("corrupt patch")

// Apply applies a patch to a to recreate b
func Apply(a, patch, b []byte) ([]byte, error) {
	var r = bytes.NewBuffer(patch)
	var err error
	var (
		size  int
		count int
		start int
		end   int
		diff  int
		prev  int
	)
	if size, err = readUvarint(r); err != nil {
		return b, ErrCorrupt
	}
	if count, err = readUvarint(r); err != nil {
		return b, ErrCorrupt
	}
	b = b[:0]
	for range count {
		if start, err = readUvarint(r); err != nil || start > len(a) {
			return b, ErrCorrupt
		}
		if end, err = readUvarint(r); err != nil || end > len(a) {
			return b, ErrCorrupt
		}
		if diff, err = readUvarint(r); err != nil {
			return b, ErrCorrupt
		}
		b = append(b, a[prev:start]...)
		b = append(b, r.Next(diff)...)
		prev = end
	}
	b = append(b, a[prev:]...)
	if size != len(b) {
		return b, ErrCorrupt
	}
	return b, nil
}

func readUvarint(r io.ByteReader) (int, error) {
	i64, err := binary.ReadUvarint(r)
	return int(i64), err
}
