package main

import (
	"encoding/binary"

	"github.com/pantopic/wazero-lmdb/sdk-go"
)

type dbLeaseImpl struct {
	db
}

func (db dbLeaseImpl) init(txn *lmdb.Txn) {
	db.open(txn)
}

func (db dbLeaseImpl) get(txn *lmdb.Txn, id uint64) (item lease, err error) {
	k := binary.AppendUvarint(nil, id)
	v, err := txn.Get(db.i, k)
	if lmdb.IsNotFound(err) {
		err = nil
		return
	}
	if err != nil {
		return
	}
	return item.FromBytes(k, v)
}

func (db dbLeaseImpl) put(txn *lmdb.Txn, item lease) error {
	return txn.Put(db.i, binary.AppendUvarint(nil, item.id), item.Bytes(nil), 0)
}

func (db dbLeaseImpl) all(txn *lmdb.Txn) (items []lease, err error) {
	cur, err := txn.OpenCursor(db.i)
	if err != nil {
		return nil, err
	}
	defer cur.Close()
	var item lease
	k, v, err := cur.Get(nil, nil, lmdb.Next)
	for !lmdb.IsNotFound(err) && len(k) > 0 {
		if err != nil {
			return nil, err
		}
		item, err := item.FromBytes(k, v)
		if err != nil {
			return nil, err
		}
		items = append(items, item)
		k, v, err = cur.Get(nil, nil, lmdb.Next)
	}
	if lmdb.IsNotFound(err) {
		err = nil
	}
	return
}

func (db dbLeaseImpl) del(txn *lmdb.Txn, id uint64) error {
	return txn.Del(db.i, binary.AppendUvarint(nil, id), nil)
}
