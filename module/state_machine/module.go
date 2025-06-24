package main

import (
	// sm "github.com/pantopic/cluster/module/state_machine_persistent/guest"

	"github.com/pantopic/wazero-lmdb/lmdb-go"
)

func main() {}

func openDb(txn *lmdb.Txn, db db) {
}

//export Open
func Open() (index uint64) {
	lmdb.Update(func(txn *lmdb.Txn) (err error) {
		index = dbMeta.init(txn)
		dbStats.init(txn)
		kvStore.init(txn)
		dbLease.init(txn)
		dbLeaseExp.init(txn)
		dbLeaseKey.init(txn)
		return nil
	})
	return
}

//export Update
func Update() {
	txn, err := lmdb.BeginTxn(nil, 0)
	if err != nil {
		panic(err)
	}
}
