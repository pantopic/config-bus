package main

import (
	"os"
	"strconv"

	"github.com/pantopic/wazero-global/sdk-go"
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
	CMD_LEASE_LOCK
	CMD_LEASE_REVOKE
	CMD_LEASE_UNLOCK
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
	// PCB_WATCH_ID_ZERO_INDEX determines whether to start watch IDs at 0 rather than 1. Starting at 0 is bad API design
	// because it confuses the zero value with the empty state. Sending an explicit watchID in a create request will
	// fail if a watch with that ID already exists for all values of watchID except 0 which will generate a new ID.
	// Disabled by default. !!! VIOLATES PARITY !!!
	PCB_WATCH_ID_ZERO_INDEX = false

	// PCB_RESPONSE_SIZE_MAX sets the maximum request and response size.
	// Matches etcd by default.
	PCB_RESPONSE_SIZE_MAX = envUint64(`PCB_RESPONSE_SIZE_MAX`, 10<<20) // 10 MiB

	// PCB_LEASE_PARTITIONS specifies the number of lease shards to use.
	// Matches etcd by default.
	PCB_LEASE_PARTITIONS = global.NewUint64(`PCB_LEASE_PARTITIONS`, 0)
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
	WATCH_ID_ERROR uint64 = 1 << 63 // 0x8000000000000000
)

func envUint64(key string, def uint64) uint64 {
	if s := os.Getenv(key); s != "" {
		if v, err := strconv.ParseUint(s, 10, 64); err == nil {
			return v
		}
	}
	return def
}
