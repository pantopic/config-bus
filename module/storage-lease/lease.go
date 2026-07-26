package main

import (
	"bytes"
	"encoding/binary"
)

type lease struct {
	id      uint64
	expires uint64
	renewed uint64
}

func (item lease) FromBytes(key, buf []byte) (lease, error) {
	var err error
	if len(buf) < 6 {
		return item, ErrChecksumMissing
	}
	if binary.BigEndian.Uint32(buf[len(buf)-4:]) != crc(key, buf[:len(buf)-4]) {
		return item, ErrChecksumInvalid
	}
	var n int
	if item.id, n = binary.Uvarint(key); n == 0 {
		return item, ErrLeaseKeyInvalid
	}
	r := bytes.NewBuffer(buf[:len(buf)-4])
	if item.expires, err = binary.ReadUvarint(r); err != nil {
		return item, err
	}
	if item.renewed, err = binary.ReadUvarint(r); err != nil {
		return item, err
	}
	return item, nil
}

func (item lease) Bytes(buf []byte) []byte {
	buf = binary.AppendUvarint(buf, item.expires)
	buf = binary.AppendUvarint(buf, item.renewed)
	buf = binary.BigEndian.AppendUint32(buf, crc(binary.AppendUvarint(nil, item.id), buf))
	return buf
}
