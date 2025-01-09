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
	k, v, err := cur.Get(key, nil, lmdb.SetRange)
	if err == nil && bytes.Equal(k, key) {
		prev, err = prev.FromBytes(k, v, nil, false)
	} else if err != nil && !lmdb.IsNotFound(err) {
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
		current := prev
		current.val = val
		current.lease = lease
		if err = cur.Put(key, current.Bytes(nil, nil), lmdb.Current); err != nil {
			return
		}
		if k, v, err = cur.Get(nil, nil, lmdb.NextDup); err != nil {
			if lmdb.IsNotFound(err) {
				err = nil
			}
			return
		}
		prev, err = prev.FromBytes(k, v, prev.val, false)
		if err = cur.Put(key, prev.Bytes(current.val, nil), lmdb.Current); err != nil {
			return
		}
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
		patched = len(buf) < len(v)
		if patched {
			if err = cur.Put(key, buf, lmdb.Current); err != nil {
				return
			}
		}
	}
	err = txn.Put(db.i, key, next.Bytes(nil, nil), 0)
	return
}

func (db dbKv) getRange(txn *lmdb.Txn, key, end []byte, revision, minMod, maxMod, minCreated, maxCreated, limit uint64, countOnly, keysOnly bool) (items []kv, count int, more bool, err error) {
	var created uint64
	var r = bytes.NewReader(nil)
	var next [][]byte
	var item kv
	cur, err := txn.OpenCursor(db.i)
	if err != nil {
		return
	}
	defer cur.Close()
	k, v, err := cur.Get(key, nil, lmdb.SetRange)
	for !lmdb.IsNotFound(err) {
		if err != nil {
			return
		}
		if len(v) < 12 {
			err = ErrValueInvalid
			return
		}
		if len(end) == 0 && bytes.Compare(k, key) != 0 {
			break
		}
		if len(end) > 0 && bytes.Compare(k, end) > 0 {
			return
		}
		if limit > 0 && len(items) == int(limit) {
			more = true
			return
		}
		next = next[:0]
		mod := math.MaxUint64 - binary.BigEndian.Uint64(v[:8])
		for revision > 0 && mod > revision {
			next = append(next, v)
			if k, v, err = cur.Get(nil, nil, lmdb.NextDup); err != nil {
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
		r.Reset(v[8 : len(v)-4])
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
		if created > 0 {
			if countOnly {
				count++
			} else {
				next = append(next, v)
				item, err = item.FromBytes(k, next[0], nil, keysOnly)
				if err != nil {
					return
				}
				for _, p := range next[1:] {
					item, err = item.FromBytes(k, p, item.val, keysOnly)
					if err != nil {
						return
					}
				}
				items = append(items, item)
			}
		}
		if len(end) == 0 {
			break
		}
		k, v, err = cur.Get(k, nil, lmdb.NextNoDup)
	}
	if lmdb.IsNotFound(err) {
		err = nil
	}
	return
}
