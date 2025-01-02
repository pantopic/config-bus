package icarus

import (
	"bytes"
	"encoding/binary"

	"github.com/golang/snappy"
	// "github.com/klauspost/compress/snappy"

	"github.com/logbn/icarus/internal"
	"github.com/logbn/icarus/internal/patch"
)

const (
	KV_FLAG_PATCH uint8 = 1 << iota
	KV_FLAG_COMPRESSED
)

var (
	ICARUS_FLAG_PATCH_ENABLED       = true
	ICARUS_FLAG_COMPRESSION_ENABLED = true
)

type kv struct {
	revision uint64
	version  uint64
	created  uint64
	lease    uint64
	flags    uint8
	key      []byte
	val      []byte
}

func (kv kv) Bytes(next, buf []byte) []byte {
	buf = binary.AppendUvarint(buf, kv.revision)
	buf = binary.AppendUvarint(buf, kv.created)
	if kv.version != 0 {
		buf = binary.AppendUvarint(buf, kv.version)
		buf = binary.AppendUvarint(buf, kv.lease)
		kv.flags = 0
		if next != nil && ICARUS_FLAG_PATCH_ENABLED {
			p := patch.Generate(next, kv.val, nil)
			if len(p) < len(kv.val) {
				kv.val = p
				kv.flags |= KV_FLAG_PATCH
			}
		}
		if ICARUS_FLAG_COMPRESSION_ENABLED && len(kv.val) > 16 {
			p := snappy.Encode(nil, kv.val)
			if len(p) < len(kv.val) {
				kv.val = p
				kv.flags |= KV_FLAG_COMPRESSED
			}
		}
		buf = append(buf, kv.flags)
		buf = append(buf, kv.val...)
		buf = binary.BigEndian.AppendUint32(buf, crc(kv.key, buf))
	}
	return buf
}

func (kv kv) FromBytes(key, buf, next []byte, noval bool) (kv, error) {
	var err error
	if len(buf) < 4 {
		return kv, ErrChecksumMissing
	}
	if binary.BigEndian.Uint32(buf[len(buf)-4:]) != crc(key, buf[:len(buf)-4]) {
		return kv, ErrChecksumInvalid
	}
	r := bytes.NewBuffer(buf[:len(buf)-4])
	kv.key = key
	kv.revision, err = binary.ReadUvarint(r)
	if err != nil {
		return kv, err
	}
	kv.created, err = binary.ReadUvarint(r)
	if err != nil {
		return kv, err
	}
	if kv.created == 0 {
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
		ModRevision:    int64(kv.revision),
		Version:        int64(kv.version),
		Lease:          int64(kv.lease),
		Key:            kv.key,
		Value:          kv.val,
	}
}
