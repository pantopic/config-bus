package kvr

import (
	"encoding/binary"
	"iter"

	"github.com/PowerDNS/lmdb-go/lmdb"
)

type dbLeaseExp struct {
	db
}

func newDbLeaseExp(txn *lmdb.Txn) (db dbLeaseExp, err error) {
	db.i, err = txn.OpenDBI("lease_exp", uint(lmdb.Create))
	return
}

func (db dbLeaseExp) put(txn *lmdb.Txn, item lease) error {
	k := binary.BigEndian.AppendUint64(nil, item.expires)
	k = binary.AppendUvarint(k, item.id)
	return txn.Put(db.i, k, db.addChecksum(k, nil), 0)
}

func (db dbLeaseExp) del(txn *lmdb.Txn, item lease) (err error) {
	key := binary.BigEndian.AppendUint64(nil, item.expires)
	key = binary.AppendUvarint(key, item.id)
	return txn.Del(db.i, key, nil)
}

func (db dbLeaseExp) scan(txn *lmdb.Txn, expires uint64) iter.Seq[uint64] {
	cur, err := txn.OpenCursor(db.i)
	if err != nil {
		return nil
	}
	k, v, err := cur.Get(nil, nil, lmdb.Next)
	return func(yield func(uint64) bool) {
		defer cur.Close()
		for !lmdb.IsNotFound(err) {
			if err != nil {
				break
			}
			_, err = db.trimChecksum(k, v)
			if err != nil {
				break
			}
			if len(k) < 9 {
				break
			}
			if binary.BigEndian.Uint64(k[:8]) > expires {
				break
			}
			id, _ := binary.Uvarint(k[8:])
			if !yield(id) {
				break
			}
			k, v, err = cur.Get(nil, nil, lmdb.Next)
		}
	}
}
