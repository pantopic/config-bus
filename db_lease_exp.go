package icarus

import (
	"bytes"
	"encoding/binary"

	"github.com/PowerDNS/lmdb-go/lmdb"
)

type dbLeaseExp struct {
	db
}

func newDbLeaseExp(txn *lmdb.Txn) (db dbLeaseExp, err error) {
	db.i, err = txn.OpenDBI("lease_exp", uint(lmdb.Create|lmdbDupFlags))
	return
}

func (db dbLeaseExp) put(txn *lmdb.Txn, item lease) error {
	k := binary.AppendUvarint(nil, item.expires)
	v := db.addChecksum(k, binary.AppendUvarint(nil, item.id))
	return txn.Put(db.i, k, v, 0)
}

func (db dbLeaseExp) del(txn *lmdb.Txn, item lease) (err error) {
	cur, err := txn.OpenCursor(db.i)
	if err != nil {
		return
	}
	defer cur.Close()
	key := binary.AppendUvarint(nil, item.expires)
	val := db.addChecksum(key, binary.AppendUvarint(nil, item.id))
	k, v, err := cur.Get(key, val, lmdb.GetBoth)
	if lmdb.IsNotFound(err) {
		err = nil
		return
	}
	if err != nil {
		return
	}
	if !bytes.Equal(k, key) || !bytes.Equal(v, val) {
		return ErrNotFound
	}
	return cur.Del(lmdb.Current)
}
