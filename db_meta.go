package icarus

import (
	"github.com/PowerDNS/lmdb-go/lmdb"
)

type dbMeta struct {
	db
}

var (
	metaKeyRevision    = []byte(`rev`)
	metaKeyRevisionMin = []byte(`rev_min`)
)

func newDbMeta(txn *lmdb.Txn) (db dbMeta, index uint64, err error) {
	db.i, err = txn.OpenDBI("meta", uint(lmdb.Create))
	index, err = db.getRevision(txn)
	if lmdb.IsNotFound(err) {
		err = db.setRevision(txn, 0)
	}
	_, err = db.getRevisionMin(txn)
	if lmdb.IsNotFound(err) {
		err = db.setRevisionMin(txn, 0)
	}
	return
}

func (db dbMeta) getRevision(txn *lmdb.Txn) (index uint64, err error) {
	return db.getUint64(txn, metaKeyRevision)
}

func (db dbMeta) setRevision(txn *lmdb.Txn, index uint64) (err error) {
	return db.putUint64(txn, metaKeyRevision, index)
}

func (db dbMeta) getRevisionMin(txn *lmdb.Txn) (index uint64, err error) {
	return db.getUint64(txn, metaKeyRevisionMin)
}

func (db dbMeta) setRevisionMin(txn *lmdb.Txn, index uint64) (err error) {
	return db.putUint64(txn, metaKeyRevisionMin, index)
}
