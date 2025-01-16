package icarus

import (
	"bytes"
	"encoding/binary"
	"iter"

	"github.com/PowerDNS/lmdb-go/lmdb"
	"github.com/elliotchance/orderedmap/v3"

	"github.com/logbn/icarus/internal"
)

type dbKvEvent struct {
	db
}

func newDbKvEvent(txn *lmdb.Txn) (db dbKvEvent, err error) {
	db.i, err = txn.OpenDBI("kv_event", uint(lmdb.Create|lmdbDupFlags))
	return
}

func (db dbKvEvent) put(txn *lmdb.Txn, revision, epoch uint64, key []byte) (err error) {
	k := binary.BigEndian.AppendUint64(nil, revision)
	buf := binary.AppendUvarint(nil, epoch)
	buf = append(buf, byte(internal.Event_PUT))
	buf = append(buf, key...)
	buf = db.addChecksum(k, buf)
	return txn.Put(db.i, k, buf, 0)
}

func (db dbKvEvent) delete(txn *lmdb.Txn, revision, epoch uint64, keys [][]byte) (err error) {
	k := binary.BigEndian.AppendUint64(nil, revision)
	buf := binary.AppendUvarint(nil, epoch)
	buf = append(buf, byte(internal.Event_DELETE))
	for _, key := range keys {
		if err = txn.Put(db.i, k, db.addChecksum(k, append(buf, key...)), 0); err != nil {
			return
		}
	}
	return
}

func (db dbKvEvent) compact(txn *lmdb.Txn, max uint64) (batch *orderedmap.OrderedMap[string, uint64], index uint64, err error) {
	var buf []byte
	batch = orderedmap.NewOrderedMap[string, uint64]()
	cur, err := txn.OpenCursor(db.i)
	if err != nil {
		return
	}
	defer cur.Close()
	k, v, err := cur.Get(nil, nil, lmdb.Next)
	for !lmdb.IsNotFound(err) {
		if err != nil {
			break
		}
		if len(v) < 4 {
			err = ErrValueInvalid
			return
		}
		rev := binary.BigEndian.Uint64(k)
		if max > 0 && rev >= max {
			break
		}
		for !lmdb.IsNotFound(err) {
			if err != nil {
				return
			}
			buf, err = db.trimChecksum(k, v)
			if err != nil {
				return
			}
			r := bytes.NewBuffer(buf)
			binary.ReadUvarint(r)
			r.ReadByte()
			key := string(r.Bytes())
			if len(key) == 0 {
				break
			}
			batch.Set(key, rev)
			if err = cur.Del(lmdb.Current); err != nil {
				return
			}
			_, v, err = cur.Get(k, nil, lmdb.NextDup)
		}
		index = rev
		k, v, err = cur.Get(nil, nil, lmdb.Next)
	}
	if lmdb.IsNotFound(err) {
		err = nil
	}
	return
}

func (db dbKvEvent) evtFromBytes(k, v []byte) (evt kvEvent, err error) {
	evt.revision = binary.BigEndian.Uint64(k)
	v, err = db.trimChecksum(k, v)
	if err != nil {
		return
	}
	r := bytes.NewBuffer(v)
	evt.epoch, err = binary.ReadUvarint(r)
	if err != nil {
		return
	}
	evt.etype, err = r.ReadByte()
	if err != nil {
		return
	}
	evt.key = r.Bytes()
	return
}

func (db dbKvEvent) scan(txn *lmdb.Txn, revision uint64) iter.Seq[kvEvent] {
	cur, err := txn.OpenCursor(db.i)
	if err != nil {
		return nil
	}
	var evt kvEvent
	k, v, err := cur.Get(binary.BigEndian.AppendUint64(nil, revision), nil, lmdb.SetRange)
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
			k, v, err = cur.Get(nil, nil, lmdb.Next)
		}
	}
}

type kvEvent struct {
	revision uint64
	epoch    uint64
	etype    uint8
	key      []byte
}
