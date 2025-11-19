package krv

import (
	"github.com/PowerDNS/lmdb-go/lmdb"
)

type dbMeta struct {
	db
}

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

func newDbMeta(txn *lmdb.Txn) (db dbMeta, index uint64, err error) {
	db.i, err = txn.OpenDBI("meta", uint(lmdb.Create))
	for _, k := range [][]byte{
		metaKeyEpoch,
		metaKeyIndex,
		metaKeyLeaseID,
		metaKeyRevisionCompacted,
		metaKeyTerm,
	} {
		if _, err = db.db.getUint64(txn, k); lmdb.IsNotFound(err) {
			err = db.putUint64(txn, k, 0)
		}
		if err != nil {
			return
		}
	}
	for _, k := range [][]byte{
		metaKeyRevision,
		metaKeyRevisionMin,
	} {
		if _, err = db.db.getUint64(txn, k); lmdb.IsNotFound(err) {
			err = db.putUint64(txn, k, 1)
		}
		if err != nil {
			return
		}
	}
	index, err = db.getIndex(txn)
	return
}

func (db dbMeta) getEpoch(txn *lmdb.Txn) (val uint64, err error) {
	return db.getUint64(txn, metaKeyEpoch)
}

func (db dbMeta) setEpoch(txn *lmdb.Txn, val uint64) (err error) {
	return db.putUint64(txn, metaKeyEpoch, val)
}

func (db dbMeta) getIndex(txn *lmdb.Txn) (val uint64, err error) {
	return db.getUint64(txn, metaKeyIndex)
}

func (db dbMeta) setIndex(txn *lmdb.Txn, val uint64) (err error) {
	return db.putUint64(txn, metaKeyIndex, val)
}

func (db dbMeta) getLeaseID(txn *lmdb.Txn) (val uint64, err error) {
	return db.getUint64(txn, metaKeyLeaseID)
}

func (db dbMeta) setLeaseID(txn *lmdb.Txn, val uint64) (err error) {
	return db.putUint64(txn, metaKeyLeaseID, val)
}

func (db dbMeta) getRevision(txn *lmdb.Txn) (val uint64, err error) {
	return db.getUint64(txn, metaKeyRevision)
}

func (db dbMeta) setRevision(txn *lmdb.Txn, val uint64) (err error) {
	return db.putUint64(txn, metaKeyRevision, val)
}

func (db dbMeta) getRevisionCompacted(txn *lmdb.Txn) (val uint64, err error) {
	return db.getUint64(txn, metaKeyRevisionCompacted)
}

func (db dbMeta) setRevisionCompacted(txn *lmdb.Txn, val uint64) (err error) {
	return db.putUint64(txn, metaKeyRevisionCompacted, val)
}

func (db dbMeta) getRevisionMin(txn *lmdb.Txn) (val uint64, err error) {
	return db.getUint64(txn, metaKeyRevisionMin)
}

func (db dbMeta) setRevisionMin(txn *lmdb.Txn, val uint64) (err error) {
	return db.putUint64(txn, metaKeyRevisionMin, val)
}

func (db dbMeta) getTerm(txn *lmdb.Txn) (val uint64, err error) {
	return db.getUint64(txn, metaKeyTerm)
}

func (db dbMeta) setTerm(txn *lmdb.Txn, val uint64) (err error) {
	return db.putUint64(txn, metaKeyTerm, val)
}
