package main

import (
	"math"

	"github.com/pantopic/wazero-global/sdk-go"
)

const (
	CMD_INTERNAL_TERM byte = iota
	CMD_INTERNAL_TICK
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
	QUERY_KV_RANGE byte = iota
	QUERY_LEASE_LEASES
	QUERY_LEASE_TIME_TO_LIVE
	QUERY_WATCH_PROGRESS
	QUERY_HEADER
)

var (
	// PCB_WATCH_ID_ZERO_INDEX determines whether to start watch IDs at 0 rather than 1. Starting at 0 is bad API design
	// because it confuses the zero value with the empty state. Sending an explicit watchID in a create request will
	// fail if a watch with that ID already exists for all values of watchID except 0 which will generate a new ID.
	// Disabled by default. !!! VIOLATES PARITY !!!
	PCB_WATCH_ID_ZERO_INDEX = false

	// PCB_RESPONSE_SIZE_MAX sets the maximum request and response size.
	// Matches etcd by default.
	PCB_RESPONSE_SIZE_MAX = global.NewUint64(`PCB_RESPONSE_SIZE_MAX`, 10<<20)
)

const (
	// grpc overhead costs for calculating PCB_RESPONSE_SIZE_MAX
	sizeMetaKeyValue      = 256
	sizeMetaEvent         = 256
	sizeMetaHeader        = 256
	sizeMetaWatchResponse = 256
)

const (
	WatchMessageType_UNKNOWN byte = iota
	WatchMessageType_INIT
	WatchMessageType_EVENT
	WatchMessageType_SYNC
	WatchMessageType_NOTIFY
	WatchMessageType_CANCELED
	WatchMessageType_ERR_COMPACTED
	WatchMessageType_ERR_EXISTS
)

const (
	WATCH_ID_ERROR = (math.MaxUint64 / 2) + 1 // 0x8000000000000000
)
