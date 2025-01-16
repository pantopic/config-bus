package icarus

import (
	"fmt"

	"github.com/logbn/zongzi"
)

const (
	KV_FLAG_PATCH uint8 = 1 << iota
	KV_FLAG_COMPRESSED

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

	WatchMessageType_UNKNOWN byte = iota
	WatchMessageType_INIT
	WatchMessageType_EVENT
	WatchMessageType_SYNC
	WatchMessageType_NOTIFY

	WatchEventBatchSizeMax = 1000
)

var (
	// ICARUS_KV_FULL_COUNT_ENABLED determines whether to execute a full scan for every range request to generate count.
	// Full count is used by Kubernetes in at least one place (api server storage).
	// Disabling this would certainly improve performance of range requests covering lots of keys.
	// See https://github.com/kubernetes/kubernetes/blob/e85c72d4177fba224cb1baa1b5abfb5980e6d867/staging/src/k8s.io/apiserver/pkg/storage/etcd3/store.go#L762
	ICARUS_KV_FULL_COUNT_ENABLED = true

	// ICARUS_KV_PATCH_ENABLED determines whether to enable patches for non-current key revisions
	// Icarus can use it transparently so it is enabled by default.
	ICARUS_KV_PATCH_ENABLED = true

	// ICARUS_KV_COMPRESSION_ENABLED determines whether to snappy compress values
	// Icarus can use it transparently so it is enabled by default.
	ICARUS_KV_COMPRESSION_ENABLED = true

	// ICARUS_TXN_MULTI_WRITE_ENABLED determines whether to allow multiple writes to a single key during a transaction.
	// Icarus supports it, but etcd does not so it is disabled by default.
	ICARUS_TXN_MULTI_WRITE_ENABLED = false

	// ICARUS_RANGE_CORRECT_FILTER_COUNT_ENABLED determines whether to apply filters to the result count.
	// This is a bug in etcd that they don't intend to fix.
	// Min/max mod/created rev not used by Kubernetes so it is enabled by default since it is the correct behavior.
	ICARUS_CORRECT_RANGE_FILTER_COUNT_ENABLED = true
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
