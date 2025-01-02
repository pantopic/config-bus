package icarus

import (
	"testing"
)

func BenchmarkTestKv(b *testing.B) {
	item := kv{
		revision: 3,
		version:  2,
		created:  1,
		key:      []byte(`test-key`),
		val:      []byte(`test-val`),
	}
	itemPatchCompress := kv{
		revision: 3,
		version:  2,
		created:  1,
		key:      []byte(`test-key`),
		val:      []byte(`----------------------------------------`),
	}
	nextPatch := []byte(`--------------------0-------------------`)
	nextCompress := []byte(`........................................`)
	b.Run(`enc`, func(b *testing.B) {
		b.Run(`basic`, func(b *testing.B) {
			var buf []byte
			for range b.N {
				buf = item.Bytes(nil, nil)
			}
			b.SetBytes(int64(len(buf)))
		})
		ICARUS_FLAG_COMPRESSION_ENABLED = false
		b.Run(`patch`, func(b *testing.B) {
			var buf []byte
			for range b.N {
				buf = itemPatchCompress.Bytes(nextPatch, nil)
			}
			b.SetBytes(int64(len(buf)))
		})
		ICARUS_FLAG_COMPRESSION_ENABLED = true
		ICARUS_FLAG_PATCH_ENABLED = false
		b.Run(`compress`, func(b *testing.B) {
			var buf []byte
			for range b.N {
				buf = itemPatchCompress.Bytes(nextCompress, nil)
			}
			b.SetBytes(int64(len(buf)))
		})
		ICARUS_FLAG_PATCH_ENABLED = true
		b.Run(`nopatch-compress`, func(b *testing.B) {
			var buf []byte
			for range b.N {
				buf = itemPatchCompress.Bytes(nextCompress, nil)
			}
			b.SetBytes(int64(len(buf)))
		})
	})
	b.Run(`dec`, func(b *testing.B) {
		b.Run(`basic`, func(b *testing.B) {
			var err error
			var item2 kv
			var buf = item.Bytes(nil, nil)
			for range b.N {
				item2, err = item2.FromBytes(item.key, buf, nil, false)
			}
			b.SetBytes(int64(len(item2.val)))
			if err != nil {
				b.Fatal(err.Error())
			}
		})
		b.Run(`patch`, func(b *testing.B) {
			var err error
			var item2 kv
			var buf = itemPatchCompress.Bytes(nextPatch, nil)
			for range b.N {
				item2, err = item2.FromBytes(item.key, buf, nextPatch, false)
			}
			b.SetBytes(int64(len(item2.val)))
			if err != nil {
				b.Fatal(err.Error())
			}
		})
		b.Run(`compress`, func(b *testing.B) {
			var err error
			var item2 kv
			var buf = itemPatchCompress.Bytes(nextCompress, nil)
			for range b.N {
				item2, err = item2.FromBytes(item.key, buf, nextCompress, false)
			}
			b.SetBytes(int64(len(item2.val)))
			if err != nil {
				b.Fatal(err.Error())
			}
		})
	})
}
