package icarus

import (
	"hash"
	"hash/crc32"
	"sync"
)

var crcPool = sync.Pool{
	New: func() any { return crc32.NewIEEE() },
}

func crc(key, val []byte) (res uint32) {
	h := crcPool.Get().(hash.Hash32)
	h.Write(key)
	h.Write(val)
	res = h.Sum32()
	h.Reset()
	crcPool.Put(h)
	return
}
