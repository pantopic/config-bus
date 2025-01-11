package icarus

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestKv(t *testing.T) {
	assert.Equal(t, uint8(1), KV_FLAG_PATCH)
	assert.Equal(t, uint8(2), KV_FLAG_COMPRESSED)
	item := kv{
		revision: 3,
		version:  2,
		created:  1,
		key:      []byte(`test-key`),
		val:      []byte(`----------------------------------------`),
	}
	t.Run(`basic`, func(t *testing.T) {
		buf := item.Bytes(nil, nil)
		var item2 kv
		item2, err := item2.FromBytes(item.key, buf, nil, false)
		require.Nil(t, err)
		assert.Equal(t, item.revision, item2.revision)
		assert.Equal(t, item.version, item2.version)
		assert.Equal(t, item.created, item2.created)
		assert.Equal(t, item.val, item2.val)
		assert.Equal(t, item.key, item2.key)
	})
	t.Run(`patch`, func(t *testing.T) {
		next := []byte(`--------------------0-------------------`)
		withGlobal(&ICARUS_KV_PATCH_ENABLED, true, func() {
			buf := item.Bytes(next, nil)
			t.Run(`enabled`, func(t *testing.T) {
				t.Run(`success`, func(t *testing.T) {
					item2, err := (kv{}).FromBytes(item.key, buf, next, false)
					require.Nil(t, err)
					assert.Equal(t, KV_FLAG_PATCH, item2.flags&KV_FLAG_PATCH)
					assert.Equal(t, item.val, item2.val)
				})
				t.Run(`invalid`, func(t *testing.T) {
					_, err := (kv{}).FromBytes(item.key, buf, nil, false)
					require.Equal(t, ErrPatchInvalid, err)
				})
			})
		})
		withGlobal(&ICARUS_KV_PATCH_ENABLED, false, func() {
			buf := item.Bytes(next, nil)
			t.Run(`disabled`, func(t *testing.T) {
				t.Run(`success`, func(t *testing.T) {
					item2, err := (kv{}).FromBytes(item.key, buf, next, false)
					require.Nil(t, err)
					assert.Equal(t, uint8(0), item2.flags&KV_FLAG_PATCH)
					assert.Equal(t, item.val, item2.val)
				})
				t.Run(`valid`, func(t *testing.T) {
					_, err := (kv{}).FromBytes(item.key, buf, nil, false)
					require.Nil(t, err)
				})
			})
		})
	})
	t.Run(`compress`, func(t *testing.T) {
		next := []byte(`........................................`)
		withGlobal(&ICARUS_KV_COMPRESSION_ENABLED, true, func() {
			buf := item.Bytes(next, nil)
			t.Run(`enabled`, func(t *testing.T) {
				t.Run(`success`, func(t *testing.T) {
					item2, err := (kv{}).FromBytes(item.key, buf, next, false)
					require.Nil(t, err)
					assert.Equal(t, KV_FLAG_COMPRESSED, item2.flags&KV_FLAG_COMPRESSED)
					assert.Equal(t, item.val, item2.val)
				})
			})
		})
		withGlobal(&ICARUS_KV_COMPRESSION_ENABLED, false, func() {
			buf := item.Bytes(next, nil)
			t.Run(`disabled`, func(t *testing.T) {
				t.Run(`success`, func(t *testing.T) {
					item2, err := (kv{}).FromBytes(item.key, buf, next, false)
					require.Nil(t, err)
					assert.Equal(t, uint8(0), item2.flags&KV_FLAG_COMPRESSED)
					assert.Equal(t, item.val, item2.val)
				})
			})
		})
	})
}

func withGlobal(flag *bool, val bool, fn func()) {
	prev := *flag
	*flag = val
	fn()
	*flag = prev
}
