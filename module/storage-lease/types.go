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
	CMD_INTERNAL_TICK_LEASE
	CMD_KV_PUT
	CMD_KV_DELETE_RANGE
	CMD_KV_COMPACT
	CMD_KV_TXN
	CMD_LEASE_GRANT
	CMD_LEASE_REVOKE
	CMD_LEASE_KEEP_ALIVE
	CMD_LEASE_KEEP_ALIVE_BATCH
)

const (
	QUERY_HEADER byte = iota
	QUERY_KV_RANGE
	QUERY_LEASE_CHECK_BATCH
	QUERY_LEASE_LEASES
	QUERY_LEASE_TIME_TO_LIVE
	QUERY_WATCH_PROGRESS
)

var (
	// PCB_LEASE_PARTITIONS specifies the number of lease shards to use.
	// Matches etcd by default.
	PCB_LEASE_PARTITIONS = global.NewUint64(`PCB_LEASE_PARTITIONS`, 0)

	// PCB_WATCH_ID_ZERO_INDEX determines whether to start watch IDs at 0 rather than 1. Starting at 0 is bad API design
	// because it confuses the zero value with the empty state. Sending an explicit watchID in a create request will
	// fail if a watch with that ID already exists for all values of watchID except 0 which will generate a new ID.
	// Disabled by default. !!! VIOLATES PARITY !!!
	PCB_WATCH_ID_ZERO_INDEX = false

	// PCB_BATCH_LEASE_RENEWAL specifies whether to introduce artificial latency when batching lease renewals.
	// Reduces total number of raft proposals to improve efficiency at the cost of increased latency for lease renewals.
	// Enabled by default.
	PCB_BATCH_LEASE_RENEWAL          = true
	PCB_BATCH_LEASE_RENEWAL_LIMIT    = 1000
	PCB_BATCH_LEASE_RENEWAL_INTERVAL = 500 * time.Millisecond
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
