package main

import (
	"errors"
	"time"

	"github.com/aperturerobotics/protobuf-go-lite"
	"github.com/pantopic/wazero-global/sdk-go"
)

type (
	Message protobuf_go_lite.Message
)

const (
	KV_FLAG_PATCH uint8 = 1 << iota
	KV_FLAG_COMPRESSED
)

const (
	CMD_INTERNAL_TERM byte = iota
	CMD_INTERNAL_TICK
	CMD_KV_COMPACT
	CMD_KV_DELETE_RANGE
	CMD_KV_PUT
	CMD_KV_TXN
	CMD_LEASE_GRANT
	CMD_LEASE_KEEP_ALIVE
	CMD_LEASE_KEEP_ALIVE_BATCH
	CMD_LEASE_REVOKE
)

const (
	QUERY_HEADER byte = iota
	QUERY_KV_RANGE
	QUERY_LEASE_LEASES
	QUERY_LEASE_TIME_TO_LIVE
	QUERY_WATCH_PROGRESS
)

const (
	WatchMessageType__UNKNOWN byte = iota
	WatchMessageType_CANCELED
	WatchMessageType_ERR_COMPACTED
	WatchMessageType_ERR_EXISTS
	WatchMessageType_EVENT_BATCH
	WatchMessageType_EVENT_SYNC
	WatchMessageType_INIT
	WatchMessageType_NOTIFY
)

const (
	limitCompactionMaxKeys = 1 << 10
)

const (
	// TXN_OPS_LIMIT limits the maximum number of operations per transaction. Hard limit allows use of last
	// 10 bits of revision to represent subrevision. Max txn ops in K8s is set to 1000.
	// Etcd default max is 128 but max can be set as high as MaxInt64. !!! VIOLATES PARITY !!!
	TXN_OPS_LIMIT = 1024

	// LIMIT_KEY_LENGTH limits the maximum length of any key.
	// Key length is unlimited in etc. !!! VIOLATES PARITY !!!
	LIMIT_KEY_LENGTH = 480
)

var (
	// RANGE_COUNT_FULL determines whether to execute a full scan for every range request to generate count.
	// Disabling this will likely improve performance of range requests covering lots of keys.
	// Full count is used by Kubernetes in at least one place (api server storage) but only because More is missing.
	// See https://github.com/kubernetes/kubernetes/blob/e85c72d4177fba224cb1baa1b5abfb5980e6d867/staging/src/k8s.io/apiserver/pkg/storage/etcd3/store.go#L762
	// Enabled by default for parity.
	RANGE_COUNT_FULL = global.NewBool(`RANGE_COUNT_FULL`, true)

	// RANGE_COUNT_FAKE determines whether to return a count value 1 greater than the number of results
	// when there are more results in a range query. This should be sufficient to trick Kubernetes into functioning
	// correctly without incurring the cost of scanning the entire key range to generate a count for each range request.
	// Disabled by default for parity.
	RANGE_COUNT_FAKE = global.NewBool(`RANGE_COUNT_FAKE`, false)

	// RANGE_COUNT_FILTER_CORRECT determines whether to apply filters to the result count. This is a bug in etcd
	// that they don't intend to fix. Min/max mod/created rev are not used by Kubernetes so parity is unimportant.
	// Enabled by default. !!! VIOLATES PARITY !!!
	RANGE_COUNT_FILTER_CORRECT = true

	// PATCH_ENABLED determines whether to enable patches for non-current key revisions
	// Enabled by default.
	PATCH_ENABLED = true

	// COMPRESSION_ENABLED determines whether to snappy compress values
	// Enabled by default.
	COMPRESSION_ENABLED = true

	// TXN_MULTI_WRITE_ENABLED determines whether to allow multiple writes to a single key during a transaction.
	// Disabled by default for parity.
	TXN_MULTI_WRITE_ENABLED = global.NewBool(`TXN_MULTI_WRITE_ENABLED`, false)

	// WATCH_ID_ZERO_INDEX determines whether to start watch IDs at 0 rather than 1. Starting at 0 is bad API design
	// because it confuses the zero value with the empty state. Sending an explicit watchID in a create request will
	// fail if a watch with that ID already exists for all values of watchID except 0 which will generate a new ID.
	// Disabled by default. !!! VIOLATES PARITY !!!
	WATCH_ID_ZERO_INDEX = false

	// TXN_OPS_MAX sets the maximum number of operations allowed per transaction.
	// Matches etcd by default. Limited by [TXN_OPS_LIMIT]
	TXN_OPS_MAX = 128

	// RESPONSE_SIZE_MAX sets the maximum request and response size.
	// Matches etcd by default.
	RESPONSE_SIZE_MAX = 10 << 20

	// WATCH_PROGRESS_NOTIFY_INTERVAL sets the duration of periodic watch progress notification.
	// Matches etcd by default.
	WATCH_PROGRESS_NOTIFY_INTERVAL = 10 * time.Minute

	// READ_LOCAL forces Linearizable range requests to be served as Serializable (stale) if:
	// 1. The client requests a specific revision
	// 2. That revision is available locally
	// This works because revisions are immutable.
	// Enabled by default.
	READ_LOCAL = true
)

var (
	ErrChecksumInvalid = errors.New(`Checksum invalid`)
	ErrChecksumMissing = errors.New(`Checksum missing`)
	ErrValueInvalid    = errors.New(`Value invalid`)
	ErrPatchInvalid    = errors.New(`Patch invalid (missing next?)`)
	ErrKeyInvalid      = errors.New(`Key invalid`)
	ErrKeyMissing      = errors.New(`Key missing`)
	ErrLeaseKeyInvalid = errors.New(`Lease key invalid`)
	ErrNotFound        = errors.New(`Not found`)
	ErrTermExpired     = errors.New(`Term expired`)
)
