package icarus

import (
	"bytes"
	"encoding/binary"
	"math"

	"github.com/PowerDNS/lmdb-go/lmdb"
)

type dbKv struct {
	db
}

func newDbKv(txn *lmdb.Txn) (db dbKv, err error) {
	db.i, err = txn.OpenDBI("kv", uint(lmdb.Create|lmdbDupFlags))
	return
}

func (db dbKv) put(txn *lmdb.Txn, index, lease uint64, key, val []byte) (prev, next kv, patched bool, err error) {
	cur, err := txn.OpenCursor(db.i)
	if err != nil {
		return
	}
	defer cur.Close()
	k, v, err := cur.Get(key, nil, 0)
	if err == nil {
		prev, err = prev.FromBytes(k, v, nil, false)
	} else if !lmdb.IsNotFound(err) {
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
		err = txn.Put(db.i, key, next.Bytes(nil, nil), 0)
		return
	}
	if prev.revision == index {
		prev.val = val
		prev.lease = lease
		err = cur.Put(key, prev.Bytes(nil, nil), lmdb.Current)
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
	if ICARUS_FLAG_KV_PATCH_ENABLED {
		buf := prev.Bytes(val, nil)
		patched = len(buf) < len(prev.val)
		if patched {
			if err = cur.Put(key, buf, lmdb.Current); err != nil {
				return
			}
		}
	}
	if err = txn.Put(db.i, key, next.Bytes(nil, nil), 0); err != nil {
		return
	}
	return
}

func (db dbKv) getRange(txn *lmdb.Txn, key, end []byte, revision, minMod, maxMod, minCreated, maxCreated, limit uint64, countOnly, keysOnly bool) (items []kv, count int, more bool, err error) {
	cur, err := txn.OpenCursor(db.i)
	if err != nil {
		return
	}
	defer cur.Close()
	k, v, err := cur.Get(key, nil, 0)
	if lmdb.IsNotFound(err) {
		k, v, err = cur.Get(nil, nil, lmdb.NextNoDup)
	}
	var mod uint64
	var created uint64
	var r = bytes.NewReader(nil)
	for !lmdb.IsNotFound(err) {
		if err != nil {
			return
		}
		if len(v) < 12 {
			err = ErrValueInvalid
			return
		}
		if len(end) > 0 && bytes.Compare(end, key) < 0 {
			return
		}
		if limit > 0 && len(items) == int(limit) {
			more = true
			return
		}
		mod = math.MaxUint64 - binary.BigEndian.Uint64(v[:8])
		for revision > 0 && mod > revision {
			k, v, err = cur.Get(nil, nil, lmdb.NextDup)
			if lmdb.IsNotFound(err) {
				break
			}
			mod = math.MaxUint64 - binary.BigEndian.Uint64(v[:8])
		}
		if err != nil {
			continue
		}
		if minMod > 0 && mod < minMod {
			k = nil
			break
		}
		if maxMod > 0 && mod > maxMod {
			continue
		}
		r.Reset(v[8:])
		created, err = binary.ReadUvarint(r)
		if err != nil {
			return
		}
		if minCreated > 0 && created < minCreated {
			k = nil
			break
		}
		if maxCreated > 0 && created > maxCreated {
			continue
		}
		if len(k) > 0 && created != 0 {
			if countOnly {
				count++
			} else {
				var item kv
				item, err = item.FromBytes(k, v, nil, keysOnly)
				if err != nil {
					return
				}
				items = append(items, item)
			}
		}
		if len(end) == 0 {
			break
		}
		k, v, err = cur.Get(nil, nil, lmdb.NextNoDup)
	}
	if lmdb.IsNotFound(err) {
		err = nil
	}
	return
}
