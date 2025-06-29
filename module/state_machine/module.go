package main

import (
	sm "github.com/pantopic/cluster/guest/state_machine"

	"github.com/pantopic/wazero-lmdb/lmdb-go"
)

var (
	txn *lmdb.Txn
)

func main() {
	sm.RegisterPersistent(
		open,
		update,
		finish,
		read,
	)
}

func open() (index uint64) {
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

func update(index uint64, cmd []byte) (value uint64, data []byte) {
	var err error
	if txn == nil {
		txn, err = lmdb.BeginTxn(nil, 0)
		if err != nil {
			panic(err)
		}
	}
	return
}

func finish() {
	if err := txn.Commit(); err != nil {
		panic(err)
	}
}

func read(query []byte) (code uint64, res []byte) {
	// txn, err := lmdb.BeginTxn(nil, 0)
	// if err != nil {
	// 	panic(err)
	// }
	return
}
