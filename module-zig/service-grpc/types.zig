//! Mirrors module/service-grpc/types.go

pub const CMD_INTERNAL_TERM: u8 = 0;
pub const CMD_INTERNAL_TICK: u8 = 1;
pub const CMD_KV_PUT: u8 = 2;
pub const CMD_KV_DELETE_RANGE: u8 = 3;
pub const CMD_KV_COMPACT: u8 = 4;
pub const CMD_KV_TXN: u8 = 5;
pub const CMD_LEASE_GRANT: u8 = 6;
pub const CMD_LEASE_REVOKE: u8 = 7;
pub const CMD_LEASE_KEEP_ALIVE: u8 = 8;
pub const CMD_LEASE_KEEP_ALIVE_BATCH: u8 = 9;

pub const QUERY_KV_RANGE: u8 = 0;
pub const QUERY_LEASE_LEASES: u8 = 1;
pub const QUERY_LEASE_TIME_TO_LIVE: u8 = 2;
pub const QUERY_WATCH_PROGRESS: u8 = 3;
pub const QUERY_HEADER: u8 = 4;

/// PCB_WATCH_ID_ZERO_INDEX determines whether to start watch IDs at 0 rather than 1. Starting at 0 is bad API design
/// because it confuses the zero value with the empty state. Sending an explicit watchID in a create request will
/// fail if a watch with that ID already exists for all values of watchID except 0 which will generate a new ID.
/// Disabled by default. !!! VIOLATES PARITY !!!
pub const PCB_WATCH_ID_ZERO_INDEX = false;

/// PCB_RESPONSE_SIZE_MAX sets the maximum request and response size.
/// Matches etcd by default.
pub const PCB_RESPONSE_SIZE_MAX: u64 = 10 << 20; // 10 MiB

pub const WatchMessageType_UNKNOWN: u8 = 0;
pub const WatchMessageType_INIT: u8 = 1;
pub const WatchMessageType_EVENT: u8 = 2;
pub const WatchMessageType_SYNC: u8 = 3;
pub const WatchMessageType_NOTIFY: u8 = 4;
pub const WatchMessageType_CANCELED: u8 = 5;
pub const WatchMessageType_ERR_COMPACTED: u8 = 6;
pub const WatchMessageType_ERR_EXISTS: u8 = 7;

pub const WATCH_ID_ERROR: u64 = 1 << 63; // 0x8000000000000000
