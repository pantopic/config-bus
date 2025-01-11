package icarus

import (
	"github.com/PowerDNS/lmdb-go/lmdb"
)

type dbMeta struct {
	db
}

var (
	metaKeyRevision          = []byte(`rev`)
	metaKeyRevisionMin       = []byte(`rev_min`)
	metaKeyRevisionCompacted = []byte(`rev_compacted`)
)

func newDbMeta(txn *lmdb.Txn) (db dbMeta, index uint64, err error) {
	db.i, err = txn.OpenDBI("meta", uint(lmdb.Create))
	if index, err = db.getRevision(txn); lmdb.IsNotFound(err) {
		err = db.setRevision(txn, 0)
	}
	if err != nil {
		return
	}
	if _, err = db.getRevisionMin(txn); lmdb.IsNotFound(err) {
		err = db.setRevisionMin(txn, 0)
	}
	if err != nil {
		return
	}
	if _, err = db.getRevisionCompacted(txn); lmdb.IsNotFound(err) {
		err = db.setRevisionCompacted(txn, 0)
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

func (db dbMeta) getRevisionCompacted(txn *lmdb.Txn) (index uint64, err error) {
	return db.getUint64(txn, metaKeyRevisionCompacted)
}

func (db dbMeta) setRevisionCompacted(txn *lmdb.Txn, index uint64) (err error) {
	return db.putUint64(txn, metaKeyRevisionCompacted, index)
}
