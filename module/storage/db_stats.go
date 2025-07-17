package main

import (
	"github.com/pantopic/wazero-lmdb/lmdb-go"
)

type dbStatsImpl struct {
	db
}

func (db dbStatsImpl) init(txn *lmdb.Txn) {
	db.open(txn)
}
