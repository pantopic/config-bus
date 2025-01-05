package icarus

import (
	"encoding/binary"

	"github.com/PowerDNS/lmdb-go/lmdb"
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
