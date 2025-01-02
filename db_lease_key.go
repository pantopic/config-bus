package icarus

import (
	"github.com/PowerDNS/lmdb-go/lmdb"
)

type dbLeaseKey struct {
	db
}

func newDbLeaseKey(txn *lmdb.Txn) (db dbLeaseKey, err error) {
	db.i, err = txn.OpenDBI("lease_key", uint(lmdb.Create|lmdbDupFlags))
	return
}
