package icarus

import (
	"bytes"
	"encoding/binary"

	"github.com/PowerDNS/lmdb-go/lmdb"
)

type dbKv struct {
	db
}

func newDbKv(txn *lmdb.Txn) (db dbKv, err error) {
	db.i, err = txn.OpenDBI("kv", uint(lmdb.Create|lmdbDupFlags))
	return
}

func (db dbKv) first(txn *lmdb.Txn, key []byte) (found kv, err error) {
	buf, err := txn.Get(db.i, key)
	if lmdb.IsNotFound(err) {
		err = nil
		return
	}
	if err != nil {
		return
	}
	return found.FromBytes(key, buf, nil, false)
}

func (db dbKv) put(txn *lmdb.Txn, index, lease uint64, key, val []byte) (prev kv, next kv, err error) {
	prev, err = db.first(txn, key)
	if err != nil {
		return
	}
	if prev.revision == 0 {
		next = kv{
			revision: index,
			version:  1,
			created:  index,
			lease:    lease,
			key:      key,
			val:      val,
		}
		txn.Put(db.i, key, next.Bytes(nil, nil), 0)
		return
	}
	if prev.revision == index {
		prev.val = val
		prev.lease = lease
		txn.Put(db.i, key, prev.Bytes(nil, nil), 0)
		return
	}
	next = kv{
		revision: index,
		version:  prev.version + 1,
		created:  prev.created,
		lease:    lease,
		key:      key,
		val:      val,
	}
	txn.Put(db.i, key, prev.Bytes(val, nil), 0)
	txn.Put(db.i, key, next.Bytes(nil, nil), 0)
	return
}

func (db dbKv) count(txn *lmdb.Txn, key, end []byte, revision, minMod, maxMod, minCreated, maxCreated uint64) (count uint64, err error) {
	txn.RawRead = true
	cur, err := txn.OpenCursor(db.i)
	if err != nil {
		return
	}
	k, v, err := cur.Get(key, nil, 0)
	if err != nil {
		return
	}
	var mod uint64
	var created uint64
	var r = bytes.NewReader(nil)
	for !lmdb.IsNotFound(err) {
		if bytes.Compare(end, key) > 0 {
			return
		}
		r.Reset(v)
		mod, err = binary.ReadUvarint(r)
		if err != nil {
			return
		}
		for mod > revision {
			k, v, err = cur.Get(nil, nil, lmdb.NextDup)
			if lmdb.IsNotFound(err) {
				err = nil
				break
			}
			r.Reset(v)
			mod, err = binary.ReadUvarint(r)
			if err != nil {
				return
			}
			if mod < minMod {
				k = nil
				break
			}
			if mod > maxMod {
				continue
			}
			created, err = binary.ReadUvarint(r)
			if err != nil {
				return
			}
			if created < minCreated {
				k = nil
				break
			}
			if created > maxCreated {
				continue
			}
		}
		if len(k) > 0 {
			count++
		}
		k, v, err = cur.Get(nil, nil, lmdb.NextNoDup)
	}
	return
}

func (db dbKv) getRange(txn *lmdb.Txn, key, end []byte, revision, minMod, maxMod, minCreated, maxCreated, limit uint64, keysOnly bool) (items []kv, more bool, err error) {
	cur, err := txn.OpenCursor(db.i)
	if err != nil {
		return
	}
	k, v, err := cur.Get(key, nil, 0)
	if err != nil {
		return
	}
	var mod uint64
	var created uint64
	var r = bytes.NewReader(nil)
	for !lmdb.IsNotFound(err) {
		if bytes.Compare(end, key) > 0 {
			return
		}
		if len(items) == int(limit) {
			more = true
			return
		}
		r.Reset(v)
		mod, err = binary.ReadUvarint(r)
		if err != nil {
			return
		}
		created, err = binary.ReadUvarint(r)
		if err != nil {
			return
		}
		for mod > revision {
			k, v, err = cur.Get(nil, nil, lmdb.NextDup)
			if lmdb.IsNotFound(err) {
				err = nil
				break
			}
			r.Reset(v)
			mod, err = binary.ReadUvarint(r)
			if err != nil {
				return
			}
			if mod < minMod {
				k = nil
				break
			}
			if mod > maxMod {
				continue
			}
			created, err = binary.ReadUvarint(r)
			if err != nil {
				return
			}
			if created < minCreated {
				k = nil
				break
			}
			if created > maxCreated {
				continue
			}
		}
		if len(k) > 0 && created != 0 {
			var item kv
			item, err = item.FromBytes(k, v, nil, keysOnly)
			items = append(items, item)
		}
		k, v, err = cur.Get(nil, nil, lmdb.NextNoDup)
	}
	return
}
