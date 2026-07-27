//! Mirrors module/service-grpc/error.go
//!
//! Maps etcd server error message strings to gRPC status codes. The storage
//! module returns bare error message strings; this table recovers the code.

const std = @import("std");

pub const code_canceled: u32 = 1;
pub const code_unknown: u32 = 2;
pub const code_invalid_argument: u32 = 3;
pub const code_deadline_exceeded: u32 = 4;
pub const code_not_found: u32 = 5;
pub const code_already_exists: u32 = 6;
pub const code_permission_denied: u32 = 7;
pub const code_resource_exhausted: u32 = 8;
pub const code_failed_precondition: u32 = 9;
pub const code_aborted: u32 = 10;
pub const code_out_of_range: u32 = 11;
pub const code_unimplemented: u32 = 12;
pub const code_internal: u32 = 13;
pub const code_unavailable: u32 = 14;
pub const code_data_loss: u32 = 15;
pub const code_unauthenticated: u32 = 16;

pub const Status = struct {
    code: u32,
    msg: []const u8,
};

// mvcc
pub const err_watcher_not_exist = "mvcc: watcher does not exist";
pub const err_empty_watcher_range = "mvcc: watcher range is empty";
pub const err_watcher_duplicate_id = "mvcc: duplicate watch ID provided on the WatchStream";

// Mirrors errStringToError: every gRPC status error keyed by its message.
pub const statuses = [_]Status{
    .{ .code = code_invalid_argument, .msg = "etcdserver: key too long" },

    .{ .code = code_invalid_argument, .msg = "etcdserver: key is not provided" },
    .{ .code = code_invalid_argument, .msg = "etcdserver: key not found" },
    .{ .code = code_invalid_argument, .msg = "etcdserver: value is provided" },
    .{ .code = code_invalid_argument, .msg = "etcdserver: lease is provided" },

    .{ .code = code_invalid_argument, .msg = "etcdserver: too many operations in txn request" },
    .{ .code = code_invalid_argument, .msg = "etcdserver: duplicate key given in txn request" },
    .{ .code = code_out_of_range, .msg = "etcdserver: mvcc: required revision has been compacted" },
    .{ .code = code_out_of_range, .msg = "etcdserver: mvcc: required revision is a future revision" },
    .{ .code = code_resource_exhausted, .msg = "etcdserver: mvcc: database space exceeded" },

    .{ .code = code_not_found, .msg = "etcdserver: requested lease not found" },
    .{ .code = code_failed_precondition, .msg = "etcdserver: lease already exists" },
    .{ .code = code_out_of_range, .msg = "etcdserver: too large lease TTL" },

    .{ .code = code_canceled, .msg = "etcdserver: watch canceled" },

    .{ .code = code_failed_precondition, .msg = "etcdserver: member ID already exist" },
    .{ .code = code_failed_precondition, .msg = "etcdserver: Peer URLs already exists" },
    .{ .code = code_failed_precondition, .msg = "etcdserver: re-configuration failed due to not enough started members" },
    .{ .code = code_invalid_argument, .msg = "etcdserver: given member URLs are invalid" },
    .{ .code = code_not_found, .msg = "etcdserver: member not found" },
    .{ .code = code_failed_precondition, .msg = "etcdserver: can only promote a learner member" },
    .{ .code = code_failed_precondition, .msg = "etcdserver: can only promote a learner member which is in sync with leader" },
    .{ .code = code_failed_precondition, .msg = "etcdserver: too many learner members in cluster" },
    .{ .code = code_failed_precondition, .msg = "etcdserver: cluster ID mismatch" },

    .{ .code = code_invalid_argument, .msg = "etcdserver: request is too large" },
    .{ .code = code_resource_exhausted, .msg = "etcdserver: too many requests" },

    .{ .code = code_failed_precondition, .msg = "etcdserver: root user does not exist" },
    .{ .code = code_failed_precondition, .msg = "etcdserver: root user does not have root role" },
    .{ .code = code_failed_precondition, .msg = "etcdserver: user name already exists" },
    .{ .code = code_invalid_argument, .msg = "etcdserver: user name is empty" },
    .{ .code = code_failed_precondition, .msg = "etcdserver: user name not found" },
    .{ .code = code_failed_precondition, .msg = "etcdserver: role name already exists" },
    .{ .code = code_failed_precondition, .msg = "etcdserver: role name not found" },
    .{ .code = code_invalid_argument, .msg = "etcdserver: role name is empty" },
    .{ .code = code_invalid_argument, .msg = "etcdserver: authentication failed, invalid user ID or password" },
    .{ .code = code_permission_denied, .msg = "etcdserver: permission denied" },
    .{ .code = code_failed_precondition, .msg = "etcdserver: role is not granted to the user" },
    .{ .code = code_failed_precondition, .msg = "etcdserver: permission is not granted to the role" },
    .{ .code = code_failed_precondition, .msg = "etcdserver: authentication is not enabled" },
    .{ .code = code_unauthenticated, .msg = "etcdserver: invalid auth token" },
    .{ .code = code_invalid_argument, .msg = "etcdserver: invalid auth management" },
    .{ .code = code_invalid_argument, .msg = "etcdserver: revision of auth store is old" },

    .{ .code = code_unavailable, .msg = "etcdserver: no leader" },
    .{ .code = code_failed_precondition, .msg = "etcdserver: not leader" },
    .{ .code = code_unavailable, .msg = "etcdserver: leader changed" },
    .{ .code = code_unavailable, .msg = "etcdserver: not capable" },
    .{ .code = code_unavailable, .msg = "etcdserver: server stopped" },
    .{ .code = code_unavailable, .msg = "etcdserver: request timed out" },
    .{ .code = code_unavailable, .msg = "etcdserver: request timed out, possibly due to previous leader failure" },
    .{ .code = code_unavailable, .msg = "etcdserver: request timed out, possibly due to connection lost" },
    .{ .code = code_unavailable, .msg = "etcdserver: request timed out, waiting for the applied index took too long" },
    .{ .code = code_unavailable, .msg = "etcdserver: unhealthy cluster" },
    .{ .code = code_data_loss, .msg = "etcdserver: corrupt cluster" },
    .{ .code = code_unavailable, .msg = "etcdserver: rpc not supported for learner" },
    .{ .code = code_failed_precondition, .msg = "etcdserver: bad leader transferee" },

    .{ .code = code_unavailable, .msg = "etcdserver: cluster version not found during downgrade" },
    .{ .code = code_invalid_argument, .msg = "etcdserver: wrong downgrade target version format" },
    .{ .code = code_invalid_argument, .msg = "etcdserver: invalid downgrade target version" },
    .{ .code = code_failed_precondition, .msg = "etcdserver: cluster has a downgrade job in progress" },
    .{ .code = code_failed_precondition, .msg = "etcdserver: no inflight downgrade job" },

    .{ .code = code_canceled, .msg = "etcdserver: request canceled" },
    .{ .code = code_deadline_exceeded, .msg = "etcdserver: context deadline exceeded" },
};

/// Returns the gRPC status code for a known etcd error message,
/// or Unknown when the message is not recognized (mirrors grpcError).
pub fn grpcCode(msg: []const u8) u32 {
    for (statuses) |s| {
        if (std.mem.eql(u8, s.msg, msg)) return s.code;
    }
    return code_unknown;
}
