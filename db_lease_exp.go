package icarus

import (
	"github.com/PowerDNS/lmdb-go/lmdb"
)

type dbLeaseExp struct {
	db
}

func newDbLeaseExp(txn *lmdb.Txn) (db dbLeaseExp, err error) {
	db.i, err = txn.OpenDBI("lease_exp", uint(lmdb.Create))
	return
}
