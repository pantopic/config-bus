package icarus

import (
	"log/slog"
	"testing"

	"github.com/PowerDNS/lmdb-go/lmdb"
	"github.com/benbjohnson/clock"
	"github.com/stretchr/testify/assert"
	// "github.com/stretchr/testify/require"
)

func TestDb(t *testing.T) {
	var err error
	var (
		dir = "/tmp/icarus/test-db"
		log = slog.Default()
	)
	sm := &stateMachine{
		shardID:   1,
		replicaID: 1,
		envPath:   dir,
		log:       log,
		clock:     clock.New(),
	}
	_, err = sm.Open(make(chan struct{}))
	if err != nil {
		panic(err)
	}
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
			_, _, _, err = sm.dbKv.put(txn, 0, 0, []byte(`test-key`), []byte(`test-val`))
			return
		})
		assert.NotNil(t, err)
		sm.dbKv.i = 100000
		err = sm.env.Update(func(txn *lmdb.Txn) (err error) {
			_, _, _, err = sm.dbKv.put(txn, 0, 0, []byte(`test-key`), []byte(`test-val`))
			return
		})
		assert.NotNil(t, err)
	})
}
