package main

import (
	"encoding/binary"
	"strconv"

	"github.com/pantopic/wazero-lmdb/lmdb-go"
)

var (
	dbMeta  = dbMetaImpl{db{`meta`, 1}}
	dbStats = dbStatsImpl{db{`stats`, 2}}
	kvStore = kvStoreImpl{
		rev: db{`revision`, 3},
		evt: db{`event`, 4},
		val: db{`value`, 5},
	}
	dbLease    = dbLeaseImpl{db{`lease`, 6}}
	dbLeaseExp = dbLeaseExpImpl{db{`lease_exp`, 7}}
	dbLeaseKey = dbLeaseKeyImpl{db{`lease_key`, 8}}
)

type db struct {
	name string
	i    lmdb.DBI
}

func (db db) open(txn *lmdb.Txn) {
	i, err := txn.OpenDBI(db.name, lmdb.Create)
	if err != nil {
		panic(err)
	}
	if i != db.i {
		panic("Incorrect DBI: " + db.name + "(" + strconv.Itoa(int(i)) + ")")
	}
}

func (db db) trimChecksum(key, val []byte) ([]byte, error) {
	if len(val) < 4 {
		return nil, ErrChecksumInvalid
	}
	chk := binary.BigEndian.Uint32(val[len(val)-4:])
	val = val[:len(val)-4]
	if chk != crc(key, val) {
		return nil, ErrChecksumInvalid
	}
	return val, nil
}

func (db db) addChecksum(key, val []byte) []byte {
	return binary.BigEndian.AppendUint32(val, crc(key, val))
}

func (db db) getUint64(txn *lmdb.Txn, key []byte) (i uint64, err error) {
	val, err := txn.Get(db.i, key)
	if err != nil {
		return
	}
	val, err = db.trimChecksum(key, val)
	if err != nil {
		return
	}
	if len(val) < 8 {
		err = ErrValueInvalid
	}
	i = binary.BigEndian.Uint64(val[:8])
	return
}

func (db db) putUint64(txn *lmdb.Txn, key []byte, val uint64) (err error) {
	return txn.Put(db.i, key, db.addChecksum(key, binary.BigEndian.AppendUint64(nil, val)), 0)
}
