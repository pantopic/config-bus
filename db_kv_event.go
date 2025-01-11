package icarus

import (
	"bytes"
	"encoding/binary"

	"github.com/PowerDNS/lmdb-go/lmdb"
	"github.com/elliotchance/orderedmap/v3"
)

type dbKvEvent struct {
	db
}

func newDbKvEvent(txn *lmdb.Txn) (db dbKvEvent, err error) {
	db.i, err = txn.OpenDBI("kv_event", uint(lmdb.Create|lmdbDupFlags))
	return
}

func (db dbKvEvent) put(txn *lmdb.Txn, revision, timestamp uint64, key []byte) (err error) {
	k := binary.AppendUvarint(nil, revision)
	buf := binary.AppendUvarint(nil, timestamp)
	buf = append(buf, KV_EVENT_TYPE_PUT)
	buf = append(buf, key...)
	buf = db.addChecksum(k, buf)
	return txn.Put(db.i, k, buf, 0)
}

func (db dbKvEvent) delete(txn *lmdb.Txn, revision, timestamp uint64, keys [][]byte) (err error) {
	k := binary.AppendUvarint(nil, revision)
	buf := binary.AppendUvarint(nil, timestamp)
	buf = append(buf, KV_EVENT_TYPE_DELETE)
	for _, key := range keys {
		if err = txn.Put(db.i, k, db.addChecksum(k, append(buf, key...)), 0); err != nil {
			return
		}
	}
	return
}

func (db dbKvEvent) compact(txn *lmdb.Txn, max uint64) (batch *orderedmap.OrderedMap[string, uint64], index uint64, err error) {
	var buf []byte
	batch = orderedmap.NewOrderedMap[string, uint64]()
	cur, err := txn.OpenCursor(db.i)
	if err != nil {
		return
	}
	defer cur.Close()
	k, v, err := cur.Get(nil, nil, lmdb.Next)
	for !lmdb.IsNotFound(err) {
		if err != nil {
			break
		}
		if len(v) < 4 {
			err = ErrValueInvalid
			return
		}
		rev, n := binary.Uvarint(k)
		if n < 0 {
			err = ErrKeyInvalid
			return
		}
		if max > 0 && rev >= max {
			break
		}
		for !lmdb.IsNotFound(err) {
			if err != nil {
				return
			}
			buf, err = db.trimChecksum(k, v)
			if err != nil {
				return
			}
			r := bytes.NewBuffer(buf)
			binary.ReadVarint(r)
			r.ReadByte()
			key := string(r.Bytes())
			if len(key) == 0 {
				break
			}
			batch.Set(key, rev)
			if err = cur.Del(lmdb.Current); err != nil {
				return
			}
			_, v, err = cur.Get(k, nil, lmdb.NextDup)
		}
		index = rev
		k, v, err = cur.Get(nil, nil, lmdb.Next)
	}
	if lmdb.IsNotFound(err) {
		err = nil
	}
	return
}
