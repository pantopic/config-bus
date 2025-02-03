package icarus

import (
	"bytes"
	"encoding/binary"
	"math"

	"github.com/PowerDNS/lmdb-go/lmdb"

	"github.com/logbn/icarus/internal"
)

type dbKv struct {
	del db
	key db
	val db
}

func newDbKv(txn *lmdb.Txn) (db dbKv, err error) {
	if db.key.i, err = txn.OpenDBI("key", uint(lmdb.Create|lmdbDupFlags)); err != nil {
		return
	}
	if db.del.i, err = txn.OpenDBI("del", uint(lmdb.Create|lmdbDupFlags)); err != nil {
		return
	}
	db.val.i, err = txn.OpenDBI("val", uint(lmdb.Create))
	return
}

func (db dbKv) put(
	txn *lmdb.Txn,
	revision, subrev, lease uint64,
	key, val []byte,
	ignoreValue, ignoreLease bool,
) (prev, next kv, patched bool, err error) {
	cur, err := txn.OpenCursor(db.key.i)
	if err != nil {
		return
	}
	defer cur.Close()
	var krec keyrecord
	var v []byte
	k, b, err := cur.Get(key, nil, lmdb.SetRange)
	if err == nil && bytes.Equal(k, key) {
		if krec, err = krec.FromBytes(k, b); err != nil {
			return
		}
		if !krec.rev.isdel() {
			if v, err = txn.Get(db.val.i, krec.rev.key()); err != nil {
				return
			}
			if prev, err = prev.FromBytes(krec.rev.key(), v, nil, false); err != nil {
				return
			}
		}
	} else if err != nil && !lmdb.IsNotFound(err) {
		return
	}
	if prev.created == 0 {
		next = kv{
			rev:     newkeyrev(revision, subrev, false),
			version: 1,
			created: revision,
			lease:   lease,
			key:     key,
			val:     val,
		}
		krec.key = key
		krec.rev = next.rev
		krec.lease = lease
		if err = txn.Put(db.key.i, key, krec.Bytes(nil), 0); err != nil {
			return
		}
		err = txn.Put(db.val.i, next.rev.key(), next.Bytes(nil, nil), 0)
		return
	}
	if prev.rev.upper() == revision && !ICARUS_TXN_MULTI_WRITE_ENABLED {
		err = internal.ErrGRPCDuplicateKey
		return
	}
	next = kv{
		rev:     newkeyrev(revision, subrev, false),
		version: prev.version + 1,
		created: prev.created,
		lease:   lease,
		key:     key,
		val:     val,
	}
	if ignoreValue {
		next.val = prev.val
	}
	if ignoreLease {
		next.lease = prev.lease
	}
	if ICARUS_KV_PATCH_ENABLED {
		buf := prev.Bytes(val, nil)
		patched = len(buf) < len(v)
		if patched {
			if err = txn.Put(db.val.i, prev.rev.key(), buf, 0); err != nil {
				return
			}
		}
	}
	krec.key = key
	krec.rev = next.rev
	krec.lease = lease
	if err = txn.Put(db.key.i, key, krec.Bytes(nil), 0); err != nil {
		return
	}
	err = txn.Put(db.val.i, next.rev.key(), next.Bytes(nil, nil), 0)
	return
}

func (db dbKv) getRange(
	txn *lmdb.Txn,
	key, end []byte,
	revision, minMod, maxMod, minCreated, maxCreated, limit uint64,
	countOnly, keysOnly bool,
) (items []kv, count int, more bool, err error) {
	var krec keyrecord
	var next []keyrev
	var item kv
	var rev keyrev
	cur, err := txn.OpenCursor(db.key.i)
	if err != nil {
		return
	}
	defer cur.Close()
	var k, v []byte
	var isFullScan = bytes.Equal(key, []byte{0}) && bytes.Equal(end, []byte{0})
	k, b, err := cur.Get(key, nil, lmdb.SetRange)
	for !lmdb.IsNotFound(err) {
		if err != nil {
			return
		}
		if !isFullScan && len(end) == 0 && !bytes.Equal(k, key) {
			break
		}
		if !isFullScan && len(end) > 0 && bytes.Compare(k, end) >= 0 {
			break
		}
		if krec, err = krec.FromBytes(k, b); err != nil {
			return
		}
		rev = krec.rev
		if !countOnly && limit > 0 && len(items) == int(limit) {
			more = true
			if !ICARUS_KV_RANGE_COUNT_FULL && !ICARUS_KV_RANGE_COUNT_FAKE {
				return
			}
			countOnly = true
		}
		next = next[:0]
		for revision > 0 && rev.upper() > revision {
			if rev.isdel() {
				next = next[:0]
			} else if !countOnly {
				next = append(next, rev)
			}
			if k, b, err = cur.Get(nil, nil, lmdb.NextDup); err != nil {
				break
			}
			if krec, err = krec.FromBytes(k, b); err != nil {
				return
			}
			rev = krec.rev
		}
		if lmdb.IsNotFound(err) {
			err = nil
			goto next
		} else if err != nil {
			return
		}
		if minMod > 0 && rev.upper() < minMod {
			goto next
		}
		if maxMod > 0 && rev.upper() > maxMod {
			goto next
		}
		if !rev.isdel() {
			count++
			if !countOnly {
				next = append(next, rev)
				item.val = nil
				for _, r := range next {
					v, err = txn.Get(db.val.i, r.key())
					if err != nil {
						return
					}
					item, err = item.FromBytes(r.key(), v, item.val, keysOnly)
					if err != nil {
						return
					}
				}
				items = append(items, item)
			} else if ICARUS_KV_RANGE_COUNT_FAKE {
				break
			}
		}
	next:
		if len(end) == 0 {
			break
		}
		k, b, err = cur.Get(k, b, lmdb.NextNoDup)
	}
	if lmdb.IsNotFound(err) {
		err = nil
	}
	return
}

func (db dbKv) deleteRange(txn *lmdb.Txn, index, subrev uint64, key, end []byte) (items []keyrecord, count int64, err error) {
	var prev keyrecord
	var tombstone keyrev
	cur, err := txn.OpenCursor(db.key.i)
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
		if len(end) == 0 && !bytes.Equal(k, key) {
			break
		}
		if len(end) > 0 && bytes.Compare(k, end) > 0 {
			return
		}
		prev, err = prev.FromBytes(k, v)
		if !prev.rev.isdel() {
			tombstone = newkeyrev(index, subrev, true)
			if prev.rev.upper() == index && !ICARUS_TXN_MULTI_WRITE_ENABLED {
				err = internal.ErrGRPCDuplicateKey
				return
			}
			tkrec := keyrecord{rev: tombstone, key: k}
			if err = txn.Put(db.key.i, k, tkrec.Bytes(nil), 0); err != nil {
				return
			}
			tk := tombstone.key()
			if err = txn.Put(db.del.i, tk, db.del.addChecksum(tk, k), 0); err != nil {
				return
			}
			items = append(items, prev)
			count++
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

func (db dbKv) deleteBatch(txn *lmdb.Txn, index, subrev uint64, keys [][]byte) (err error) {
	var prev, tombstone keyrecord
	var k, v []byte
	cur, err := txn.OpenCursor(db.key.i)
	if err != nil {
		return
	}
	defer cur.Close()
	for _, key := range keys {
		k, v, err = cur.Get(key, nil, lmdb.SetRange)
		if lmdb.IsNotFound(err) {
			return ErrNotFound
		}
		if err != nil {
			return
		}
		if len(v) < 12 {
			err = ErrValueInvalid
			return
		}
		if !bytes.Equal(k, key) {
			return ErrNotFound
		}
		prev, err = prev.FromBytes(k, v)
		if !prev.rev.isdel() {
			tombstone = keyrecord{key: key, rev: newkeyrev(index, subrev, true)}
			if err = txn.Put(db.key.i, k, tombstone.Bytes(nil), 0); err != nil {
				return
			}
			tk := tombstone.rev.key()
			if err = txn.Put(db.del.i, tk, db.del.addChecksum(tk, key), 0); err != nil {
				return
			}
		}
	}
	return
}

func (db dbKv) compact(txn *lmdb.Txn, max uint64) (index uint64, err error) {
	var buf []byte
	var rec1 keyrecord
	var rev1, rev2 keyrev
	curKey, err := txn.OpenCursor(db.key.i)
	if err != nil {
		return
	}
	defer curKey.Close()
	curVal, err := txn.OpenCursor(db.val.i)
	if err != nil {
		return
	}
	defer curVal.Close()
	k1, v1, err1 := curVal.Get(nil, nil, lmdb.Next)
	if err1 != nil && !lmdb.IsNotFound(err1) {
		err = err1
		return
	}
	if rec1, err = rec1.FromBytes(k1, v1); err != nil {
		return
	}
	rev1 = rec1.rev
	curDel, err := txn.OpenCursor(db.del.i)
	if err != nil {
		return
	}
	defer curDel.Close()
	k2, v2, err2 := curDel.Get(nil, nil, lmdb.Next)
	if err2 != nil && !lmdb.IsNotFound(err2) {
		err = err2
		return
	}
	if rev2, err = rev1.FromKey(k2, v2); err != nil {
		return
	}
	var keys = map[string]keyrev{}
	var keylen uint64
	var key []byte
	var done bool
	for !done {
		for !lmdb.IsNotFound(err1) && !lmdb.IsNotFound(err2) {
			if (err1 != nil && !lmdb.IsNotFound(err1)) || (err2 != nil && !lmdb.IsNotFound(err2)) {
				done = true
				break
			}
			if max > 0 && rev1.upper() >= max && rev2.upper() >= max {
				done = true
				break
			}
			if len(keys) >= limitCompactionMaxKeys {
				break
			}
			if rev1 < rev2 {
				index = rev1.upper()
				if err = curVal.Del(lmdb.Current); err != nil {
					return
				}
				buf, err = db.key.trimChecksum(k1, v1)
				if err != nil {
					return
				}
				r := bytes.NewBuffer(buf)
				keylen, err = binary.ReadUvarint(r)
				if err != nil {
					return
				}
				key = r.Next(int(keylen))
				if _, ok := keys[string(key)]; !ok {
					keys[string(key)] = rev1
				}
				k1, v1, err1 = curVal.Get(nil, nil, lmdb.NextDup)
				if lmdb.IsNotFound(err1) {
					k1, v1, err1 = curVal.Get(nil, nil, lmdb.Next)
				}
				if err1 == nil {
					rec1, err1 = rec1.FromBytes(k1, v1)
					rev1 = rec1.rev
				} else if lmdb.IsNotFound(err1) {
					rev1 = math.MaxUint64
				}
			} else {
				index = rev2.upper()
				if err = curDel.Del(lmdb.Current); err != nil {
					return
				}
				key, err = db.key.trimChecksum(k2, v2)
				if err != nil {
					return
				}
				if err = txn.Del(db.key.i, key, rev2.Bytes(key, nil)); err != nil {
					return
				}
				k2, v2, err2 = curDel.Get(nil, nil, lmdb.NextDup)
				if lmdb.IsNotFound(err2) {
					k2, v2, err2 = curDel.Get(nil, nil, lmdb.Next)
				}
				if err2 == nil {
					rev2, err2 = rev2.FromBytes(k2, v2)
				} else if lmdb.IsNotFound(err2) {
					rev2 = math.MaxUint64
				}
			}
		}
		if lmdb.IsNotFound(err) {
			err = nil
		}
		var r keyrecord
		var k, v []byte
		var hasNewer bool
		for key, rev := range keys {
			k, v, err = curKey.Get([]byte(key), nil, lmdb.SetRange)
			for !lmdb.IsNotFound(err) {
				if err != nil {
					return
				}
				if !bytes.Equal(k, []byte(key)) {
					break
				}
				if r, err = r.FromBytes(k, v); err != nil {
					return
				}
				if r.rev > rev {
					hasNewer = true
					goto next
				}
				if r.rev == rev && !r.rev.isdel() {
					hasNewer = true
					goto next
				}
				if r.rev.isdel() {
					if err = curKey.Del(lmdb.Current); err != nil {
						return
					}
					hasNewer = true
					goto next
				}
				if hasNewer {
					if err = curKey.Del(lmdb.Current); err != nil {
						return
					}
					if err = txn.Del(db.val.i, r.rev.key(), nil); err != nil {
						return
					}
					hasNewer = true
					goto next
				}
			next:
				k, v, err = curKey.Get(nil, nil, lmdb.NextDup)
			}
		}
		if !done {
			clear(keys)
		}
	}
	return
}

func (db dbKv) get(txn *lmdb.Txn, key []byte) (item kv, err error) {
	item, _, err = db.getRev(txn, key, 0, false)
	return
}

func (db dbKv) getRev(txn *lmdb.Txn, key []byte, revision uint64, withPrev bool) (item, prev kv, err error) {
	cur, err := txn.OpenCursor(db.key.i)
	if err != nil {
		return
	}
	defer cur.Close()
	var krec keyrecord
	var prec keyrecord
	var next []keyrev
	k, v, err := cur.Get(key, nil, lmdb.SetRange)
	if lmdb.IsNotFound(err) {
		err = nil
		return
	}
	if err != nil {
		return
	}
	if !bytes.Equal(k, key) {
		return
	}
	if krec, err = krec.FromBytes(k, v); err != nil {
		return
	}
	for revision > 0 && krec.rev.upper() > revision {
		if krec.rev.isdel() {
			next = next[:0]
		} else {
			next = append(next, krec.rev)
		}
		if k, v, err = cur.Get(nil, nil, lmdb.NextDup); err != nil {
			break
		}
		if krec, err = krec.FromBytes(k, v); err != nil {
			return
		}
	}
	if lmdb.IsNotFound(err) {
		err = nil
		return
	}
	if err != nil {
		return
	}
	if krec.rev.isdel() {
		return
	}
	var p []byte
	next = append(next, krec.rev)
	for _, rev := range next {
		if p, err = txn.Get(db.val.i, rev.key()); err != nil {
			return
		}
		item, err = item.FromBytes(rev.key(), p, item.val, false)
		if err != nil {
			return
		}
	}
	if withPrev {
		k, v, err = cur.Get(nil, nil, lmdb.NextDup)
		if lmdb.IsNotFound(err) {
			err = nil
			return
		}
		if err != nil {
			return
		}
		if prec, err = prec.FromBytes(k, v); err != nil {
			return
		}
		if p, err = txn.Get(db.val.i, prec.rev.key()); err != nil {
			return
		}
		prev, err = prev.FromBytes(prec.rev.key(), p, item.val, false)
	}
	return
}
