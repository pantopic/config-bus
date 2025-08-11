package main

import (
	"github.com/pantopic/wazero-lmdb/sdk-go"
)

var (
	// Logical clock representing seconds of uptime since shard creation
	metaKeyEpoch = []byte(`epoch`)
	// Index of last applied raft log entry
	metaKeyIndex = []byte(`index`)
	// Autoincrement cursor for generating lease ids
	metaKeyLeaseID = []byte(`lease_id`)
	// Last applied data revision
	metaKeyRevision = []byte(`rev`)
	// Compaction cursor - Keys up to this revision have been compacted (always <= rev_min)
	metaKeyRevisionCompacted = []byte(`rev_compacted`)
	// Compaction target - Keys up to this revision are no longer visible
	metaKeyRevisionMin = []byte(`rev_min`)
	// Shard raft term - Prevents duplicate controllers
	metaKeyTerm = []byte(`term`)
)

type dbMetaImpl struct {
	db
}

func (db dbMetaImpl) init(txn *lmdb.Txn) (index uint64) {
	var err error
	db.open(txn)
	for _, k := range [][]byte{
		metaKeyEpoch,
		metaKeyIndex,
		metaKeyLeaseID,
		metaKeyRevision,
		metaKeyRevisionCompacted,
		metaKeyRevisionMin,
		metaKeyTerm,
	} {
		if _, err = db.db.getUint64(txn, k); lmdb.IsNotFound(err) {
			err = db.putUint64(txn, k, 0)
		}
		if err != nil {
			panic(err)
		}
	}
	if index, err = db.getIndex(txn); err != nil {
		panic(err)
	}
	return
}

func (db dbMetaImpl) getEpoch(txn *lmdb.Txn) (index uint64, err error) {
	return db.getUint64(txn, metaKeyEpoch)
}

func (db dbMetaImpl) setEpoch(txn *lmdb.Txn, index uint64) (err error) {
	return db.putUint64(txn, metaKeyEpoch, index)
}

func (db dbMetaImpl) getIndex(txn *lmdb.Txn) (index uint64, err error) {
	return db.getUint64(txn, metaKeyIndex)
}

func (db dbMetaImpl) setIndex(txn *lmdb.Txn, index uint64) (err error) {
	return db.putUint64(txn, metaKeyIndex, index)
}

func (db dbMetaImpl) getLeaseID(txn *lmdb.Txn) (index uint64, err error) {
	return db.getUint64(txn, metaKeyLeaseID)
}

func (db dbMetaImpl) setLeaseID(txn *lmdb.Txn, index uint64) (err error) {
	return db.putUint64(txn, metaKeyLeaseID, index)
}

func (db dbMetaImpl) getRevision(txn *lmdb.Txn) (index uint64, err error) {
	return db.getUint64(txn, metaKeyRevision)
}

func (db dbMetaImpl) setRevision(txn *lmdb.Txn, index uint64) (err error) {
	return db.putUint64(txn, metaKeyRevision, index)
}

func (db dbMetaImpl) getRevisionCompacted(txn *lmdb.Txn) (index uint64, err error) {
	return db.getUint64(txn, metaKeyRevisionCompacted)
}

func (db dbMetaImpl) setRevisionCompacted(txn *lmdb.Txn, index uint64) (err error) {
	return db.putUint64(txn, metaKeyRevisionCompacted, index)
}

func (db dbMetaImpl) getRevisionMin(txn *lmdb.Txn) (index uint64, err error) {
	return db.getUint64(txn, metaKeyRevisionMin)
}

func (db dbMetaImpl) setRevisionMin(txn *lmdb.Txn, index uint64) (err error) {
	return db.putUint64(txn, metaKeyRevisionMin, index)
}

func (db dbMetaImpl) getTerm(txn *lmdb.Txn) (index uint64, err error) {
	return db.getUint64(txn, metaKeyTerm)
}

func (db dbMetaImpl) setTerm(txn *lmdb.Txn, index uint64) (err error) {
	return db.putUint64(txn, metaKeyTerm, index)
}
