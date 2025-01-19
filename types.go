package icarus

import (
	"fmt"
	"time"

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

	WATCH_DEBOUNCE = 50 * time.Millisecond
)

var (
	// ICARUS_KV_RANGE_COUNT_FULL determines whether to execute a full scan for every range request to generate count.
	// Disabling this would certainly improve performance of range requests covering lots of keys.
	// Full count is used by Kubernetes in at least one place (api server storage) but only because More is missing.
	// See https://github.com/kubernetes/kubernetes/blob/e85c72d4177fba224cb1baa1b5abfb5980e6d867/staging/src/k8s.io/apiserver/pkg/storage/etcd3/store.go#L762
	ICARUS_KV_RANGE_COUNT_FULL = true

	// ICARUS_KV_RANGE_COUNT_FAKE determines whether to return a count value 1 greater than the number of results
	// when there are more results in a range query. This should be sufficient to trick Kubernetes into functioning
	// correctly without incurring the cost of scanning the entire ky range to generate a count for each range request.
	// Not suitable for parity.
	ICARUS_KV_RANGE_COUNT_FAKE = false

	// ICARUS_RANGE_COUNT_FILTER_CORRECT determines whether to apply filters to the result count.
	// This is a bug in etcd that they don't intend to fix.
	// Min/max mod/created rev are not used by Kubernetes so it is enabled by default since it is the correct behavior.
	ICARUS_RANGE_COUNT_FILTER_CORRECT = true

	// ICARUS_KV_PATCH_ENABLED determines whether to enable patches for non-current key revisions
	// Icarus can use it transparently so it is enabled by default.
	ICARUS_KV_PATCH_ENABLED = true

	// ICARUS_KV_COMPRESSION_ENABLED determines whether to snappy compress values
	// Icarus can use it transparently so it is enabled by default.
	ICARUS_KV_COMPRESSION_ENABLED = true

	// ICARUS_TXN_MULTI_WRITE_ENABLED determines whether to allow multiple writes to a single key during a transaction.
	// Icarus supports it, but etcd does not so it is disabled by default.
	ICARUS_TXN_MULTI_WRITE_ENABLED = false

	// ICARUS_ZERO_INDEX_WATCH_ID determines whether to start watch IDs at 0 rather than 1.
	// This is poor API design because it fails to leverage the zero value as an empty state.
	// Etcd starts watch IDs at zero, so this is enabled by default for parity.
	ICARUS_ZERO_INDEX_WATCH_ID = true
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
