package icarus

import (
	"github.com/PowerDNS/lmdb-go/lmdb"
)

type dbKvEvent struct {
	db
}

func newDbKvEvent(txn *lmdb.Txn) (db dbKvEvent, err error) {
	db.i, err = txn.OpenDBI("kv_event", uint(lmdb.Create|lmdbDupFlags))
	return
}
