package icarus

import (
	"fmt"

	"github.com/logbn/zongzi"
)

const (
	CMD_KV_PUT byte = iota
	CMD_KV_DELETE_RANGE
	CMD_KV_COMPACT
	CMD_KV_TXN
	CMD_LEASE_GRANT
	CMD_LEASE_REVOKE
	CMD_LEASE_KEEP_ALIVE

	QUERY_KV_RANGE byte = iota
	QUERY_LEASE_LEASES
	QUERY_LEASE_TIME_TO_LIVE

	KV_EVENT_TYPE_PUT byte = iota
	KV_EVENT_TYPE_DELETE
)

var (
	ErrChecksumInvalid = fmt.Errorf(`Checksum invalid`)
	ErrChecksumMissing = fmt.Errorf(`Checksum missing`)
	ErrValueInvalid    = fmt.Errorf(`Value invalid`)
	ErrPatchInvalid    = fmt.Errorf(`Patch invalid (missing next?)`)
	ErrKeyInvalid      = fmt.Errorf(`Key invalid`)
	ErrKeyMissing      = fmt.Errorf(`Key missing`)
	ErrLeaseKeyInvalid = fmt.Errorf(`Lease key invalid`)
	ErrNotFound        = fmt.Errorf(`Not found`)
)

type (
	Entry  = zongzi.Entry
	Result = zongzi.Result
)
