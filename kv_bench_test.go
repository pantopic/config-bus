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
		withGlobal(&ICARUS_FLAG_KV_PATCH_ENABLED, true, func() {
			withGlobal(&ICARUS_FLAG_KV_COMPRESSION_ENABLED, false, func() {
				b.Run(`patch`, func(b *testing.B) {
					var buf []byte
					for range b.N {
						buf = itemPatchCompress.Bytes(nextPatch, nil)
					}
					b.SetBytes(int64(len(buf)))
				})
			})
		})
		withGlobal(&ICARUS_FLAG_KV_COMPRESSION_ENABLED, true, func() {
			withGlobal(&ICARUS_FLAG_KV_PATCH_ENABLED, false, func() {
				b.Run(`compress`, func(b *testing.B) {
					var buf []byte
					for range b.N {
						buf = itemPatchCompress.Bytes(nextCompress, nil)
					}
					b.SetBytes(int64(len(buf)))
				})
			})
		})
		withGlobal(&ICARUS_FLAG_KV_COMPRESSION_ENABLED, true, func() {
			withGlobal(&ICARUS_FLAG_KV_PATCH_ENABLED, true, func() {
				b.Run(`nopatch-compress`, func(b *testing.B) {
					var buf []byte
					for range b.N {
						buf = itemPatchCompress.Bytes(nextCompress, nil)
					}
					b.SetBytes(int64(len(buf)))
				})
			})
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
			var buf []byte
			withGlobal(&ICARUS_FLAG_KV_PATCH_ENABLED, true, func() {
				withGlobal(&ICARUS_FLAG_KV_COMPRESSION_ENABLED, false, func() {
					buf = itemPatchCompress.Bytes(nextPatch, nil)
				})
			})
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
			var buf []byte
			withGlobal(&ICARUS_FLAG_KV_PATCH_ENABLED, false, func() {
				withGlobal(&ICARUS_FLAG_KV_COMPRESSION_ENABLED, true, func() {
					buf = itemPatchCompress.Bytes(nextPatch, nil)
				})
			})
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
