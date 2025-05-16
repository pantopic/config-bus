package kvr

import (
	"bytes"
	"encoding/binary"

	"github.com/PowerDNS/lmdb-go/lmdb"
)

type dbLease struct {
	db
}

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

func newDbLease(txn *lmdb.Txn) (db dbLease, err error) {
	db.i, err = txn.OpenDBI("lease", uint(lmdb.Create))
	return
}

func (db dbLease) get(txn *lmdb.Txn, id uint64) (item lease, err error) {
	k := binary.AppendUvarint(nil, id)
	v, err := txn.Get(db.i, k)
	if lmdb.IsNotFound(err) {
		err = nil
		return
	}
	if err != nil {
		return
	}
	return item.FromBytes(k, v)
}

func (db dbLease) put(txn *lmdb.Txn, item lease) error {
	return txn.Put(db.i, binary.AppendUvarint(nil, item.id), item.Bytes(nil), 0)
}

func (db dbLease) all(txn *lmdb.Txn) (items []lease, err error) {
	cur, err := txn.OpenCursor(db.i)
	if err != nil {
		return nil, err
	}
	defer cur.Close()
	var item lease
	k, v, err := cur.Get(nil, nil, lmdb.Next)
	for !lmdb.IsNotFound(err) && len(k) > 0 {
		if err != nil {
			return nil, err
		}
		item, err := item.FromBytes(k, v)
		if err != nil {
			return nil, err
		}
		items = append(items, item)
		k, v, err = cur.Get(nil, nil, lmdb.Next)
	}
	if lmdb.IsNotFound(err) {
		err = nil
	}
	return
}

func (db dbLease) del(txn *lmdb.Txn, id uint64) error {
	return txn.Del(db.i, binary.AppendUvarint(nil, id), nil)
}
