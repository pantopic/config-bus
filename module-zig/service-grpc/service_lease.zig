//! Mirrors module/service-grpc/service_lease.go

const types = @import("types.zig");
const util = @import("util.zig");

pub fn grant(in: []const u8) anyerror!void {
    return util.kvShard().AsyncApply(util.withSuffix(in, types.CMD_LEASE_GRANT), "leaseGrant");
}

pub fn revoke(in: []const u8) anyerror!void {
    return util.kvShard().AsyncApply(util.withSuffix(in, types.CMD_LEASE_REVOKE), "leaseRevoke");
}

pub fn keepaliveOpen() anyerror!void {}

pub fn keepaliveRecv(in: []const u8) anyerror!void {
    return util.kvShard().AsyncApply(util.withSuffix(in, types.CMD_LEASE_KEEP_ALIVE), "leaseKeepAlive");
}

pub fn keepaliveClose() anyerror!void {}

pub fn leases(in: []const u8) anyerror!void {
    return util.kvShard().AsyncRead(util.withSuffix(in, types.QUERY_LEASE_LEASES), "leaseLeases", true);
}

pub fn timeToLive(in: []const u8) anyerror!void {
    return util.kvShard().AsyncRead(util.withSuffix(in, types.QUERY_LEASE_TIME_TO_LIVE), "leaseTimeToLive", true);
}
