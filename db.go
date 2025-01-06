package icarus

import (
	"encoding/binary"

	"github.com/PowerDNS/lmdb-go/lmdb"
)

var (
	lmdbEnvFlags = lmdb.NoMemInit | lmdb.NoSync | lmdb.NoMetaSync
	lmdbDupFlags = lmdb.DupSort

	ICARUS_FLAG_PATCH_ENABLED       = false
	ICARUS_FLAG_COMPRESSION_ENABLED = false
)

type db struct {
	i lmdb.DBI
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
