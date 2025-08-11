package main

import (
	"bytes"
	"encoding/binary"
	"io"

	"github.com/pantopic/wazero-lmdb/sdk-go"
)

type dbLeaseKeyImpl struct {
	db
}

func (db dbLeaseKeyImpl) init(txn *lmdb.Txn) {
	db.open(txn)
}

func (db dbLeaseKeyImpl) put(txn *lmdb.Txn, id uint64, key []byte) error {
	k := append(binary.AppendUvarint(nil, id), key...)
	v := db.addChecksum(k, nil)
	return txn.Put(db.i, k, v, 0)
}

func (db dbLeaseKeyImpl) sweep(txn *lmdb.Txn, id uint64, batch [][]byte) ([][]byte, error) {
	cur, err := txn.OpenCursor(db.i)
	if err != nil {
		return nil, err
	}
	defer cur.Close()
	var found uint64
	var r = bytes.NewReader(nil)
	k, v, err := cur.Get(binary.AppendUvarint(nil, id), nil, lmdb.SetRange)
	for range cap(batch) {
		if lmdb.IsNotFound(err) || len(k) == 0 {
			err = nil
			break
		}
		if err != nil {
			return nil, err
		}
		if _, err = db.trimChecksum(k, v); err != nil {
			return nil, err
		}
		r.Reset(k)
		if found, err = binary.ReadUvarint(r); err != nil {
			return nil, err
		}
		if found != id {
			break
		}
		key, err := io.ReadAll(r)
		if err != nil {
			return nil, err
		}
		if err = cur.Del(lmdb.Current); err != nil {
			return nil, err
		}
		batch = append(batch, key)
		k, v, err = cur.Get(nil, nil, lmdb.Next)
	}
	return batch, nil
}

func (db dbLeaseKeyImpl) del(txn *lmdb.Txn, id uint64, key []byte) (err error) {
	return txn.Del(db.i, append(binary.AppendUvarint(nil, id), key...), nil)
}
