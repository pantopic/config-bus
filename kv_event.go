package icarus

type kvEvent struct {
	revision uint64
	epoch    uint64
	etype    uint8
	key      []byte
}
