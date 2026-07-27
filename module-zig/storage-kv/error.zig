//! Mirrors module/storage-kv/error.go and the module-local errors in types.go
//!
//! Error identity carries the tag; `msg` recovers the exact etcd error message
//! string written into responses (Go's err.Error()).

const std = @import("std");

pub const Error = error{
    // module-local (types.go)
    ChecksumInvalid,
    ChecksumMissing,
    ValueInvalid,
    PatchInvalid,
    KeyInvalid,
    KeyMissing,
    LeaseKeyInvalid,
    ModuleNotFound,
    TermExpired,

    // server-side gRPC errors (error.go)
    GRPCKeyTooLong,
    GRPCEmptyKey,
    GRPCKeyNotFound,
    GRPCValueProvided,
    GRPCLeaseProvided,
    GRPCTooManyOps,
    GRPCDuplicateKey,
    GRPCCompacted,
    GRPCFutureRev,
    GRPCNoSpace,
    GRPCLeaseNotFound,
    GRPCLeaseExist,
    GRPCLeaseTTLTooLarge,
    GRPCWatchCanceled,
};

/// Returns the Go error message string for known errors,
/// falling back to the zig error name.
pub fn msg(err: anyerror) []const u8 {
    return switch (err) {
        error.ChecksumInvalid => "Checksum invalid",
        error.ChecksumMissing => "Checksum missing",
        error.ValueInvalid => "Value invalid",
        error.PatchInvalid => "Patch invalid (missing next?)",
        error.KeyInvalid => "Key invalid",
        error.KeyMissing => "Key missing",
        error.LeaseKeyInvalid => "Lease key invalid",
        error.ModuleNotFound => "Not found",
        error.TermExpired => "Term expired",

        error.GRPCKeyTooLong => "etcdserver: key too long",
        error.GRPCEmptyKey => "etcdserver: key is not provided",
        error.GRPCKeyNotFound => "etcdserver: key not found",
        error.GRPCValueProvided => "etcdserver: value is provided",
        error.GRPCLeaseProvided => "etcdserver: lease is provided",
        error.GRPCTooManyOps => "etcdserver: too many operations in txn request",
        error.GRPCDuplicateKey => "etcdserver: duplicate key given in txn request",
        error.GRPCCompacted => "etcdserver: mvcc: required revision has been compacted",
        error.GRPCFutureRev => "etcdserver: mvcc: required revision is a future revision",
        error.GRPCNoSpace => "etcdserver: mvcc: database space exceeded",
        error.GRPCLeaseNotFound => "etcdserver: requested lease not found",
        error.GRPCLeaseExist => "etcdserver: lease already exists",
        error.GRPCLeaseTTLTooLarge => "etcdserver: too large lease TTL",
        error.GRPCWatchCanceled => "etcdserver: watch canceled",

        else => @errorName(err),
    };
}
