package krv

import (
	"bytes"
	"encoding/binary"
	// "log"
	"math"

	"github.com/golang/snappy"

	"github.com/pantopic/krv/internal"
	"github.com/pantopic/krv/internal/patch"
)

type kv struct {
	rev     keyrev
	version uint64
	created uint64
	lease   uint64
	flags   uint8
	key     []byte
	val     []byte
}

func (kv kv) Bytes(next, buf []byte) []byte {
	buf = binary.AppendUvarint(buf, uint64(len(kv.key)))
	buf = append(buf, kv.key...)
	if !kv.rev.isdel() {
		kv.flags = 0
		buf = binary.AppendUvarint(buf, kv.created)
		buf = binary.AppendUvarint(buf, kv.version)
		buf = binary.AppendUvarint(buf, kv.lease)
		if next != nil && KRV_PATCH_ENABLED {
			p := patch.Generate(next, kv.val, nil)
			if len(p) < len(kv.val) {
				kv.val = p
				kv.flags |= KV_FLAG_PATCH
			}
		}
		if KRV_COMPRESSION_ENABLED && len(kv.val) > 16 {
			p := snappy.Encode(nil, kv.val)
			if len(p) < len(kv.val) {
				kv.val = p
				kv.flags |= KV_FLAG_COMPRESSED
			}
		}
		buf = append(buf, kv.flags)
		buf = append(buf, kv.val...)
	}
	buf = binary.BigEndian.AppendUint32(buf, crc(kv.rev.key(), buf))
	return buf
}

func (kv kv) FromBytes(revKey, buf, next []byte, noval bool) (kv, error) {
	var err error
	if len(buf) < 12 {
		return kv, ErrChecksumMissing
	}
	if binary.BigEndian.Uint32(buf[len(buf)-4:]) != crc(revKey, buf[:len(buf)-4]) {
		return kv, ErrChecksumInvalid
	}
	kv.rev = keyrev(binary.BigEndian.Uint64(revKey))
	if kv.rev.isdel() {
		return kv, err
	}
	keylen, n := binary.Uvarint(buf)
	kv.key = buf[n : n+int(keylen)]
	r := bytes.NewBuffer(buf[n+int(keylen) : len(buf)-4])
	kv.created, err = binary.ReadUvarint(r)
	if err != nil {
		return kv, err
	}
	kv.version, err = binary.ReadUvarint(r)
	if err != nil {
		return kv, err
	}
	kv.lease, err = binary.ReadUvarint(r)
	if err != nil {
		return kv, err
	}
	kv.flags, err = r.ReadByte()
	if err != nil {
		return kv, err
	}
	if !noval {
		kv.val = r.Bytes()
		if kv.flags&KV_FLAG_COMPRESSED > 0 {
			kv.val, err = snappy.Decode(nil, kv.val)
			if err != nil {
				return kv, err
			}
		}
		if kv.flags&KV_FLAG_PATCH > 0 {
			if next == nil {
				return kv, ErrPatchInvalid
			}
			kv.val, err = patch.Apply(next, kv.val, nil)
			if err != nil {
				return kv, err
			}
		}
	}
	return kv, err
}

func (kv kv) ToProto() *internal.KeyValue {
	return &internal.KeyValue{
		CreateRevision: int64(kv.created),
		ModRevision:    int64(kv.rev.upper()),
		Version:        int64(kv.version),
		Lease:          int64(kv.lease),
		Key:            kv.key,
		Value:          kv.val,
	}
}

const (
	revMaskLower = math.MaxUint64 >> 54

	revMaskDelete = 1 << iota
	revMaskReserved
)

type keyrev uint64

func newkeyrev(upper, lower uint64, isdel bool) keyrev {
	a := upper<<12 + lower<<2
	if isdel {
		a |= revMaskDelete
	}
	return keyrev(a)
}

func (kr keyrev) invert() keyrev {
	return math.MaxUint64 - kr
}

func (kr keyrev) upper() uint64 {
	return uint64(kr >> 12)
}

func (kr keyrev) lower() uint64 {
	return uint64(kr >> 2 & revMaskLower)
}

func (kr keyrev) isdel() bool {
	return kr&revMaskDelete > 0
}

func (kr keyrev) key() []byte {
	return kr.appendbytes(nil)
}

func (kr keyrev) appendbytes(buf []byte) []byte {
	return binary.BigEndian.AppendUint64(buf, uint64(kr))
}

func (kr keyrev) FromBytes(key, buf []byte) (keyrev, error) {
	if len(buf) < 12 {
		return kr, ErrChecksumMissing
	}
	if binary.BigEndian.Uint32(buf[len(buf)-4:]) != crc(key, buf[:len(buf)-4]) {
		return kr, ErrChecksumInvalid
	}
	kr = keyrev(binary.BigEndian.Uint64(buf[:8])).invert()
	return kr, nil
}

func (kr keyrev) FromKey(key, buf []byte) (keyrev, error) {
	if len(buf) < 4 {
		return kr, ErrChecksumMissing
	}
	if binary.BigEndian.Uint32(buf[len(buf)-4:]) != crc(key, buf[:len(buf)-4]) {
		return kr, ErrChecksumInvalid
	}
	kr = keyrev(binary.BigEndian.Uint64(key))
	return kr, nil
}

func (kr keyrev) Bytes(key, buf []byte) []byte {
	buf = kr.invert().appendbytes(buf)
	buf = binary.BigEndian.AppendUint32(buf, crc(key, buf))
	return buf
}

type keyrecord struct {
	key   []byte
	rev   keyrev
	lease uint64
}

func (kr keyrecord) FromBytes(key, buf []byte) (keyrecord, error) {
	if len(buf) < 12 {
		return kr, ErrChecksumMissing
	}
	if binary.BigEndian.Uint32(buf[len(buf)-4:]) != crc(key, buf[:len(buf)-4]) {
		return kr, ErrChecksumInvalid
	}
	kr.rev = keyrev(binary.BigEndian.Uint64(buf[:8])).invert()
	kr.key = key
	if !kr.rev.isdel() {
		kr.lease, _ = binary.Uvarint(buf[8:])
	}
	return kr, nil
}

func (kr keyrecord) Bytes(buf []byte) []byte {
	buf = kr.rev.invert().appendbytes(buf)
	if !kr.rev.isdel() {
		buf = binary.AppendUvarint(buf, kr.lease)
	}
	buf = binary.BigEndian.AppendUint32(buf, crc(kr.key, buf))
	return buf
}

type kvEvent struct {
	epoch uint64
	key   []byte
	rev   keyrev
}

func (kr kvEvent) etype() uint8 {
	if kr.rev.isdel() {
		return uint8(internal.Event_DELETE)
	}
	return uint8(internal.Event_PUT)
}
