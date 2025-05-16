package kvr

import (
	"github.com/PowerDNS/lmdb-go/lmdb"
)

type dbStats struct {
	db
}

func newDbStats(txn *lmdb.Txn) (db dbStats, err error) {
	db.i, err = txn.OpenDBI("stats", uint(lmdb.Create))
	return
}
