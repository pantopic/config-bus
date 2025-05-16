package kvr

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
	CMD_INTERNAL_TICK
	CMD_INTERNAL_TERM

	QUERY_KV_RANGE byte = iota
	QUERY_LEASE_LEASES
	QUERY_LEASE_TIME_TO_LIVE

	WatchMessageType_UNKNOWN byte = iota
	WatchMessageType_INIT
	WatchMessageType_EVENT
	WatchMessageType_SYNC
	WatchMessageType_NOTIFY
	WatchMessageType_ERR_COMPACTED

	WATCH_DEBOUNCE = 50 * time.Millisecond

	// grpc overhead costs for calculating KVR_RESPONSE_SIZE_MAX
	sizeMetaKeyValue      = 256
	sizeMetaEvent         = 256
	sizeMetaHeader        = 256
	sizeMetaWatchResponse = 256

	limitCompactionMaxKeys = 1000
)

const (
	// KVR_TXN_OPS_LIMIT limits the maximum number of operations per transaction. Hard limit allows use of last
	// 10 bits of revision to represent subrevision. Max txn ops in K8s is set to 1000.
	// Etcd default max is 128 but max can be set as high as MaxInt64. !!! VIOLATES PARITY !!!
	KVR_TXN_OPS_LIMIT = 1024

	// KVR_LIMIT_KEY_LENGTH limits the maximum length of any key.
	// Key length is unlimited in etc. !!! VIOLATES PARITY !!!
	KVR_LIMIT_KEY_LENGTH = 480
)

var (
	// KVR_RANGE_COUNT_FULL determines whether to execute a full scan for every range request to generate count.
	// Disabling this will likely improve performance of range requests covering lots of keys.
	// Full count is used by Kubernetes in at least one place (api server storage) but only because More is missing.
	// See https://github.com/kubernetes/kubernetes/blob/e85c72d4177fba224cb1baa1b5abfb5980e6d867/staging/src/k8s.io/apiserver/pkg/storage/etcd3/store.go#L762
	// Enabled by default for parity.
	KVR_RANGE_COUNT_FULL = true

	// KVR_RANGE_COUNT_FAKE determines whether to return a count value 1 greater than the number of results
	// when there are more results in a range query. This should be sufficient to trick Kubernetes into functioning
	// correctly without incurring the cost of scanning the entire key range to generate a count for each range request.
	// Disabled by default for parity.
	KVR_RANGE_COUNT_FAKE = false

	// KVR_RANGE_COUNT_FILTER_CORRECT determines whether to apply filters to the result count. This is a bug in etcd
	// that they don't intend to fix. Min/max mod/created rev are not used by Kubernetes so parity is unimportant.
	// Enabled by default. !!! VIOLATES PARITY !!!
	KVR_RANGE_COUNT_FILTER_CORRECT = true

	// KVR_PATCH_ENABLED determines whether to enable patches for non-current key revisions
	// Enabled by default due to transparently.
	KVR_PATCH_ENABLED = true

	// KVR_COMPRESSION_ENABLED determines whether to snappy compress values
	// Enabled by default due to transparently.
	KVR_COMPRESSION_ENABLED = true

	// KVR_TXN_MULTI_WRITE_ENABLED determines whether to allow multiple writes to a single key during a transaction.
	// Disabled by default for parity.
	KVR_TXN_MULTI_WRITE_ENABLED = false

	// KVR_WATCH_ID_ZERO_INDEX determines whether to start watch IDs at 0 rather than 1. This is bad API design
	// because it confuses the zero value with the empty state. Sending an explicit watchID in a create request will
	// fail if a watch with that ID already exists for all values of watchID except 0 which will generate a new ID.
	// Disabled by default. !!! VIOLATES PARITY !!!
	KVR_WATCH_ID_ZERO_INDEX = false

	// KVR_WATCH_CREATE_COMPACTED determines whether to create a watch and then immediately cancel it when a client
	// requests a watch with a compacted StartRevision. This is bad API design. Only the cancellation should be sent.
	// Enabled by default for parity.
	KVR_WATCH_CREATE_COMPACTED = true

	// KVR_TXN_OPS_MAX sets the maximum number of operations allowed per transaction.
	// Matches etcd by default. Limited by KVR_TXN_OPS_LIMIT
	KVR_TXN_OPS_MAX = 128

	// KVR_RESPONSE_SIZE_MAX sets the maximum request and response size.
	// Matches etcd by default.
	KVR_RESPONSE_SIZE_MAX = 10 * 1024 * 1024

	// KVR_WATCH_PROGRESS_NOTIFY_INTERVAL sets the duration of periodic watch progress notification.
	// Matches etcd by default.
	KVR_WATCH_PROGRESS_NOTIFY_INTERVAL = 10 * time.Minute
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
	ErrTermExpired     = fmt.Errorf(`Term expired`)
)

type (
	Entry  = zongzi.Entry
	Result = zongzi.Result
)
