package main

import (
	"github.com/pantopic/wazero-lmdb/sdk-go"
)

type dbStatsImpl struct {
	db
}

func (db dbStatsImpl) init(txn *lmdb.Txn) {
	db.open(txn)
}
