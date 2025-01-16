package icarus

import (
	"log/slog"
	"os"
	"testing"

	"github.com/PowerDNS/lmdb-go/lmdb"
	"github.com/benbjohnson/clock"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/logbn/icarus/internal"
)

var (
	dir = "/tmp/icarus/test-db"
	sm  = &stateMachine{
		shardID:   1,
		replicaID: 1,
		envPath:   dir,
		log:       slog.Default(),
		clock:     clock.New(),
	}
)

func init() {
	if err = os.RemoveAll(dir); err != nil {
		panic(err)
	}
	_, err = sm.Open(make(chan struct{}))
	if err != nil {
		panic(err)
	}
}

func TestDb(t *testing.T) {
	var err error
	t.Run("checksum", func(t *testing.T) {
		k := []byte(`test-key`)
		v := []byte(`test-val`)
		t.Run("success", func(t *testing.T) {
			chk := sm.dbMeta.addChecksum(k, v)
			assert.Len(t, chk, len(v)+4)
			val, err := sm.dbMeta.trimChecksum(k, chk)
			assert.Nil(t, err)
			assert.Equal(t, val, v)
		})
		t.Run("failure", func(t *testing.T) {
			_, err = sm.dbMeta.trimChecksum(k, []byte(`a`))
			assert.NotNil(t, err)
			chk := sm.dbMeta.addChecksum(k, v)
			_, err = sm.dbMeta.trimChecksum(k, chk[1:])
			assert.NotNil(t, err)
		})
	})
	t.Run("uint64", func(t *testing.T) {
		t.Run("put", func(t *testing.T) {
			err = sm.env.Update(func(txn *lmdb.Txn) (err error) {
				return sm.dbMeta.putUint64(txn, []byte(`test`), 101)
			})
			assert.Nil(t, err)
		})
		t.Run("get", func(t *testing.T) {
			var val uint64
			err = sm.env.View(func(txn *lmdb.Txn) (err error) {
				val, err = sm.dbMeta.getUint64(txn, []byte(`test`))
				return
			})
			assert.Nil(t, err)
			assert.Equal(t, uint64(101), val)
		})
		t.Run("err", func(t *testing.T) {
			err = sm.env.Update(func(txn *lmdb.Txn) (err error) {
				return txn.Put(sm.dbMeta.i, []byte(`test-invalid-1`), []byte(`invalid`), 0)
			})
			assert.Nil(t, err)
			err = sm.env.View(func(txn *lmdb.Txn) (err error) {
				_, err = sm.dbMeta.getUint64(txn, []byte(`test-invalid-1`))
				return
			})
			assert.NotNil(t, err)
			var k2 = []byte(`test-invalid-2`)
			err = sm.env.Update(func(txn *lmdb.Txn) (err error) {
				return txn.Put(sm.dbMeta.i, k2, sm.dbMeta.addChecksum(k2, []byte(`invalid`)), 0)
			})
			assert.Nil(t, err)
			err = sm.env.View(func(txn *lmdb.Txn) (err error) {
				_, err = sm.dbMeta.getUint64(txn, k2)
				return
			})
			assert.NotNil(t, err)
		})
	})
	t.Run("bad-dbi", func(t *testing.T) {
		i := sm.dbKv.i
		defer func() { sm.dbKv.i = i }()
		sm.dbKv.i = 0
		err = sm.env.Update(func(txn *lmdb.Txn) (err error) {
			_, _, _, err = sm.dbKv.put(txn, 0, 0, []byte(`test-key`), []byte(`test-val`), false, false)
			return
		})
		assert.NotNil(t, err)
		sm.dbKv.i = 100000
		err = sm.env.Update(func(txn *lmdb.Txn) (err error) {
			_, _, _, err = sm.dbKv.put(txn, 0, 0, []byte(`test-key`), []byte(`test-val`), false, false)
			return
		})
		assert.NotNil(t, err)
	})
}

func TestDbKv(t *testing.T) {
	t.Run("getRev", func(t *testing.T) {
		err = sm.env.Update(func(txn *lmdb.Txn) (err error) {
			items := []kv{
				{revision: 1, key: []byte(`test-get-rev-00`), val: []byte(`test-get-rev-val-00`)},
				{revision: 2, key: []byte(`test-get-rev-00`), val: []byte(`test-get-rev-val-01`)},
				{revision: 3, key: []byte(`test-get-rev-00`), val: []byte(`test-get-rev-val-02`)},
			}
			for _, item := range items {
				_, _, _, err = sm.dbKv.put(txn, item.revision, 0, item.key, item.val, false, false)
				require.Nil(t, err)
			}
			item, prev, err := sm.dbKv.getRev(txn, items[1].key, items[1].revision, true)
			require.Nil(t, err)
			assert.Equal(t, items[1].val, item.val, string(item.val))
			assert.Equal(t, items[0].val, prev.val, string(prev.val))
			item, prev, err = sm.dbKv.getRev(txn, items[2].key, items[2].revision, true)
			require.Nil(t, err)
			assert.Equal(t, items[2].val, item.val, string(item.val))
			assert.Equal(t, items[1].val, prev.val, string(prev.val))
			item, prev, err = sm.dbKv.getRev(txn, items[1].key, items[1].revision, false)
			require.Nil(t, err)
			assert.Equal(t, items[1].val, item.val, string(item.val))
			assert.Len(t, prev.val, 0, string(prev.val))
			item, prev, err = sm.dbKv.getRev(txn, items[0].key, items[0].revision, true)
			require.Nil(t, err)
			assert.Equal(t, items[0].val, item.val, string(item.val))
			assert.Len(t, prev.val, 0, string(prev.val))
			return
		})
		assert.Nil(t, err)
	})
}

func TestDbKvEvent(t *testing.T) {
	t.Run("scan", func(t *testing.T) {
		err = sm.env.Update(func(txn *lmdb.Txn) (err error) {
			events := []kvEvent{
				{revision: 1, epoch: 1, key: []byte(`test-scan-00`)},
				{revision: 1, epoch: 1, key: []byte(`test-scan-01`)},
				{revision: 2, epoch: 1, key: []byte(`test-scan-01`)},
			}
			for _, evt := range events {
				require.Nil(t, sm.dbKvEvent.put(txn, evt.revision, evt.epoch, evt.key))
			}
			require.Nil(t, sm.dbKvEvent.delete(txn, 3, 2, [][]byte{[]byte(`test-scan-00`)}))
			var items []kvEvent
			for item := range sm.dbKvEvent.scan(txn, 1) {
				items = append(items, item)
			}
			require.Len(t, items, 4)
			for i, evt := range events {
				assert.Equal(t, evt.revision, items[i].revision, "revision")
				assert.Equal(t, evt.epoch, items[i].epoch, "epoch")
				assert.Equal(t, evt.etype, items[i].etype, "etype")
				assert.Equal(t, evt.key, items[i].key, "key")
			}
			assert.Equal(t, uint64(3), items[3].revision, "revision")
			assert.Equal(t, uint64(2), items[3].epoch, "epoch")
			assert.Equal(t, byte(internal.Event_DELETE), items[3].etype, "etype")
			assert.Equal(t, []byte(`test-scan-00`), items[3].key, "key")
			return
		})
		assert.Nil(t, err)
	})
}
