const global = @import("global");

pub const KV_FLAG_PATCH: u8 = 1 << 0;
pub const KV_FLAG_COMPRESSED: u8 = 1 << 1;

pub const CMD_INTERNAL_TERM: u8 = 0;
pub const CMD_INTERNAL_TICK: u8 = 1;
pub const CMD_KV_COMPACT: u8 = 2;
pub const CMD_KV_DELETE_RANGE: u8 = 3;
pub const CMD_KV_PUT: u8 = 4;
pub const CMD_KV_TXN: u8 = 5;
pub const CMD_LEASE_GRANT: u8 = 6;
pub const CMD_LEASE_KEEP_ALIVE_BATCH: u8 = 7;
pub const CMD_LEASE_KEEP_ALIVE: u8 = 8;
pub const CMD_LEASE_REVOKE: u8 = 9;

pub const QUERY_HEADER: u8 = 0;
pub const QUERY_KV_RANGE: u8 = 1;
pub const QUERY_LEASE_LEASES: u8 = 2;
pub const QUERY_LEASE_TIME_TO_LIVE: u8 = 3;
pub const QUERY_WATCH_PROGRESS: u8 = 4;

pub const WatchMessageType__UNKNOWN: u8 = 0;
pub const WatchMessageType_CANCELED: u8 = 1;
pub const WatchMessageType_ERR_COMPACTED: u8 = 2;
pub const WatchMessageType_ERR_EXISTS: u8 = 3;
pub const WatchMessageType_EVENT_BATCH: u8 = 4;
pub const WatchMessageType_EVENT_SYNC: u8 = 5;
pub const WatchMessageType_INIT: u8 = 6;
pub const WatchMessageType_NOTIFY: u8 = 7;

pub const limitCompactionMaxKeys = 1 << 10;

/// TXN_OPS_LIMIT limits the maximum number of operations per transaction. Hard limit allows use of last
/// 10 bits of revision to represent subrevision. Max txn ops in K8s is set to 1000.
/// Etcd default max is 128 but max can be set as high as MaxInt64. !!! VIOLATES PARITY !!!
pub const TXN_OPS_LIMIT = 1024;

/// LIMIT_KEY_LENGTH limits the maximum length of any key.
/// Key length is unlimited in etc. !!! VIOLATES PARITY !!!
pub const LIMIT_KEY_LENGTH = 480;

/// RANGE_COUNT_FULL determines whether to execute a full scan for every range request to generate count.
/// Enabled by default for parity.
pub const RANGE_COUNT_FULL = global.newBool("RANGE_COUNT_FULL", true);

/// RANGE_COUNT_FAKE determines whether to return a count value 1 greater than the number of results
/// when there are more results in a range query.
/// Disabled by default for parity.
pub const RANGE_COUNT_FAKE = global.newBool("RANGE_COUNT_FAKE", false);

/// RANGE_COUNT_FILTER_CORRECT determines whether to apply filters to the result count.
/// Enabled by default. !!! VIOLATES PARITY !!!
pub const RANGE_COUNT_FILTER_CORRECT = true;

/// PATCH_ENABLED determines whether to enable patches for non-current key revisions
/// Enabled by default.
pub const PATCH_ENABLED = true;

/// COMPRESSION_ENABLED determines whether to snappy compress values
/// Enabled by default.
pub const COMPRESSION_ENABLED = true;

/// TXN_MULTI_WRITE_ENABLED determines whether to allow multiple writes to a single key during a transaction.
/// Disabled by default for parity.
pub const TXN_MULTI_WRITE_ENABLED = global.newBool("TXN_MULTI_WRITE_ENABLED", false);

/// WATCH_ID_ZERO_INDEX determines whether to start watch IDs at 0 rather than 1. Starting at 0 is bad API design
/// because it confuses the zero value with the empty state. Sending an explicit watchID in a create request will
/// fail if a watch with that ID already exists EXCEPT when the ID is 0 which will generate a new ID.
/// Disabled by default. !!! VIOLATES PARITY !!!
pub const WATCH_ID_ZERO_INDEX = false;

/// TXN_OPS_MAX sets the maximum number of operations allowed per transaction.
/// Matches etcd by default. Limited by TXN_OPS_LIMIT
pub const TXN_OPS_MAX = 128;

/// RESPONSE_SIZE_MAX sets the maximum request and response size.
/// Matches etcd by default.
pub const RESPONSE_SIZE_MAX: u64 = 10 << 20;

/// WATCH_PROGRESS_NOTIFY_INTERVAL sets the duration of periodic watch progress notification (ns).
/// Matches etcd by default.
pub const WATCH_PROGRESS_NOTIFY_INTERVAL_NS: u64 = 10 * 60 * 1000 * 1000 * 1000;

/// READ_LOCAL forces Linearizable range requests to be served as Serializable (stale) if:
/// 1. The client requests a specific revision
/// 2. That revision is available locally
/// This works because revisions are immutable.
/// Enabled by default.
pub const READ_LOCAL = true;

/// BATCH_LEASE_RENEWAL specifies whether to introduce artificial latency when batching lease renewals.
/// Enabled by default.
pub const BATCH_LEASE_RENEWAL = true;
pub const BATCH_LEASE_RENEWAL_LIMIT = 1000;
pub const BATCH_LEASE_RENEWAL_INTERVAL_NS: u64 = 500 * 1000 * 1000;
