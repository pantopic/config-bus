package icarus

import (
	"fmt"

	"github.com/logbn/zongzi"
)

const (
	CMD_KV_PUT byte = iota
	CMD_KV_DELETE_RANGE
	CMD_KV_TXN
	CMD_KV_COMPACT

	QUERY_KV_RANGE byte = iota

	KV_EVENT_TYPE_PUT byte = iota
	KV_EVENT_TYPE_DELETE
)

var (
	ErrChecksumInvalid = fmt.Errorf(`Checksum invalid`)
	ErrChecksumMissing = fmt.Errorf(`Checksum missing`)
	ErrValueInvalid    = fmt.Errorf(`Value invalid`)
	ErrPatchInvalid    = fmt.Errorf(`Patch invalid (missing next?)`)
)

type (
	Entry  = zongzi.Entry
	Result = zongzi.Result
)
