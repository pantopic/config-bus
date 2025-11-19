package krv

import (
	"bytes"
	"encoding/binary"
	"iter"

	"github.com/PowerDNS/lmdb-go/lmdb"

	"github.com/pantopic/krv/internal"
)

type dbKv struct {
	evt db
	rev db
	val db
}

func newDbKv(txn *lmdb.Txn) (db dbKv, err error) {
	if db.rev.i, err = txn.OpenDBI("revision", uint(lmdb.Create|lmdbDupFlags)); err != nil {
		return
	}
	if db.evt.i, err = txn.OpenDBI("event", uint(lmdb.Create|lmdbDupFlags)); err != nil {
		return
	}
	db.val.i, err = txn.OpenDBI("value", uint(lmdb.Create))
	return
}

func (db dbKv) put(
	txn *lmdb.Txn,
	revision, subrev, lease, epoch uint64,
	key, val []byte,
	ignoreValue, ignoreLease bool,
) (prev, next kv, patched bool, err error) {
	cur, err := txn.OpenCursor(db.rev.i)
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
	} else if err != nil && !lmdb.IsNotFound(err) {
		return
	}
	if krec.rev == 0 || krec.rev.isdel() {
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
		if err = txn.Put(db.rev.i, key, krec.Bytes(nil), 0); err != nil {
			return
		}
		revkey := krec.rev.key()
		if err = txn.Put(db.val.i, revkey, next.Bytes(nil, nil), 0); err != nil {
			return
		}
		evt := append(binary.AppendUvarint(nil, epoch), key...)
		if err = txn.Put(db.evt.i, revkey, db.evt.addChecksum(revkey, evt), 0); err != nil {
			return
		}
		return
	}
	if krec.rev.upper() == revision && !KRV_TXN_MULTI_WRITE_ENABLED {
		err = internal.ErrGRPCDuplicateKey
		return
	}
	if v, err = txn.Get(db.val.i, krec.rev.key()); err != nil {
		return
	}
	if prev, err = prev.FromBytes(krec.rev.key(), v, nil, false); err != nil {
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
	if KRV_PATCH_ENABLED && !krec.rev.isdel() {
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
	if err = txn.Put(db.rev.i, key, krec.Bytes(nil), 0); err != nil {
		return
	}
	revkey := krec.rev.key()
	if err = txn.Put(db.val.i, revkey, next.Bytes(nil, nil), 0); err != nil {
		return
	}
	evt := append(binary.AppendUvarint(nil, epoch), key...)
	if err = txn.Put(db.evt.i, revkey, db.evt.addChecksum(revkey, evt), 0); err != nil {
		return
	}
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
	cur, err := txn.OpenCursor(db.rev.i)
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
			if !KRV_RANGE_COUNT_FULL && !KRV_RANGE_COUNT_FAKE {
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
				// Filtering on created revision is terribly ineffecient.
				// It is not used by Kubernetes and should not be used by anyone.
				if minCreated > 0 && item.created < minCreated {
					count--
					goto next
				}
				if maxCreated > 0 && item.created > maxCreated {
					count--
					goto next
				}
				items = append(items, item)
			} else if KRV_RANGE_COUNT_FAKE {
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

func (db dbKv) deleteRange(txn *lmdb.Txn, rev, subrev, epoch uint64, key, end []byte) (items []keyrecord, count int64, err error) {
	var prev keyrecord
	var tombstone keyrev
	cur, err := txn.OpenCursor(db.rev.i)
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
			tombstone = newkeyrev(rev, subrev, true)
			if prev.rev.upper() == rev && !KRV_TXN_MULTI_WRITE_ENABLED {
				err = internal.ErrGRPCDuplicateKey
				return
			}
			tkrec := keyrecord{rev: tombstone, key: k}
			if err = txn.Put(db.rev.i, k, tkrec.Bytes(nil), 0); err != nil {
				return
			}
			tk := tombstone.key()
			data := append(binary.AppendUvarint(nil, epoch), k...)
			if err = txn.Put(db.evt.i, tk, db.evt.addChecksum(tk, data), 0); err != nil {
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

func (db dbKv) deleteBatch(txn *lmdb.Txn, rev, subrev, epoch uint64, keys [][]byte) (err error) {
	var prev, tombstone keyrecord
	var k, v []byte
	cur, err := txn.OpenCursor(db.rev.i)
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
			tombstone = keyrecord{key: key, rev: newkeyrev(rev, subrev, true)}
			if err = txn.Put(db.rev.i, k, tombstone.Bytes(nil), 0); err != nil {
				return
			}
			tk := tombstone.rev.key()
			data := append(binary.AppendUvarint(nil, epoch), key...)
			if err = txn.Put(db.evt.i, tk, db.evt.addChecksum(tk, data), 0); err != nil {
				return
			}
		}
	}
	return
}

func (db dbKv) compact(txn *lmdb.Txn, max uint64) (last uint64, err error) {
	curRev, err := txn.OpenCursor(db.rev.i)
	if err != nil {
		return
	}
	defer curRev.Close()
	curEvt, err := txn.OpenCursor(db.evt.i)
	if err != nil {
		return
	}
	defer curEvt.Close()
	k, v, err := curEvt.Get(nil, nil, lmdb.Next)
	if err != nil && !lmdb.IsNotFound(err) {
		return
	}
	var rev keyrev
	if rev, err = rev.FromKey(k, v); err != nil {
		return
	}
	var keys = map[string]keyrev{}
	var done bool
	for !done {
		for !lmdb.IsNotFound(err) {
			if err != nil {
				done = true
				break
			}
			if max > 0 && rev.upper() >= max {
				done = true
				break
			}
			if len(keys) >= limitCompactionMaxKeys {
				break
			}
			last = rev.upper()
			v, err = db.rev.trimChecksum(k, v)
			if err != nil {
				return
			}
			_, n := binary.Uvarint(v)
			keys[string(v[n:])] = rev
			if err = curEvt.Del(lmdb.Current); err != nil {
				return
			}
			k, v, err = curEvt.Get(nil, nil, lmdb.NextDup)
			if lmdb.IsNotFound(err) {
				k, v, err = curEvt.Get(nil, nil, lmdb.Next)
			}
			if err == nil {
				rev, err = rev.FromKey(k, v)
			}
		}
		var rec keyrecord
		var k, v []byte
		var hasNewer bool
		for key, rev := range keys {
			k, v, err = curRev.Get([]byte(key), nil, lmdb.SetRange)
			for !lmdb.IsNotFound(err) {
				if err != nil {
					return
				}
				if !bytes.Equal(k, []byte(key)) {
					break
				}
				if rec, err = rec.FromBytes(k, v); err != nil {
					return
				}
				if rec.rev >= rev {
					hasNewer = true
					goto next
				}
				if hasNewer && !rec.rev.isdel() {
					if err = txn.Del(db.val.i, rec.rev.key(), nil); err != nil {
						return
					}
				}
				if err = curRev.Del(lmdb.Current); err != nil {
					return
				}
				hasNewer = true
				goto next
			next:
				k, v, err = curRev.Get(nil, nil, lmdb.NextDup)
			}
			hasNewer = false
		}
		if lmdb.IsNotFound(err) {
			err = nil
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
	cur, err := txn.OpenCursor(db.rev.i)
	if err != nil {
		return
	}
	defer cur.Close()
	var krec keyrecord
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
	next = append(next, krec.rev)
	for _, rev := range next {
		if v, err = txn.Get(db.val.i, rev.key()); err != nil {
			return
		}
		item, err = item.FromBytes(rev.key(), v, item.val, false)
		if err != nil {
			return
		}
	}
	if withPrev {
		var prec keyrecord
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
		if v, err = txn.Get(db.val.i, prec.rev.key()); err != nil {
			return
		}
		prev, err = prev.FromBytes(prec.rev.key(), v, item.val, false)
	}
	return
}

func (db dbKv) scan(txn *lmdb.Txn, revision uint64) iter.Seq[kvEvent] {
	cur, err := txn.OpenCursor(db.evt.i)
	if err != nil {
		return nil
	}
	var evt kvEvent
	k, v, err := cur.Get(binary.BigEndian.AppendUint64(nil, revision<<12), nil, lmdb.SetRange)
	return func(yield func(kvEvent) bool) {
		defer cur.Close()
		for !lmdb.IsNotFound(err) {
			if err != nil {
				// log err
				break
			}
			evt, err = db.evtFromBytes(k, v)
			if err != nil {
				// log err
				return
			}
			if !yield(evt) {
				break
			}
			k, v, err = cur.Get(nil, nil, lmdb.NextDup)
			if lmdb.IsNotFound(err) {
				k, v, err = cur.Get(nil, nil, lmdb.Next)
			}
		}
	}
}

func (db dbKv) evtFromBytes(k, v []byte) (evt kvEvent, err error) {
	var n int
	if evt.rev, err = evt.rev.FromKey(k, v); err != nil {
		return
	}
	v, err = db.evt.trimChecksum(k, v)
	if err != nil {
		return
	}
	evt.epoch, n = binary.Uvarint(v)
	evt.key = v[n:]
	return
}
