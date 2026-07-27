//! Mirrors module/service-grpc/service_lease.go

const types = @import("types.zig");
const util = @import("util.zig");

pub fn grant(in: []const u8) anyerror!void {
    const r = util.kvShard().Apply(util.withSuffix(in, types.CMD_LEASE_GRANT));
    return util.autoSend(r.val, r.res, r.err);
}

pub fn revoke(in: []const u8) anyerror!void {
    const r = util.kvShard().Apply(util.withSuffix(in, types.CMD_LEASE_REVOKE));
    return util.autoSend(r.val, r.res, r.err);
}

pub fn keepaliveOpen() anyerror!void {}

pub fn keepaliveRecv(item: []const u8) anyerror!void {
    const r = util.kvShard().Apply(util.withSuffix(item, types.CMD_LEASE_KEEP_ALIVE));
    return util.autoSend(r.val, r.res, r.err);
}

pub fn keepaliveClose() anyerror!void {}

pub fn leases(in: []const u8) anyerror!void {
    const r = util.kvShard().Read(util.withSuffix(in, types.QUERY_LEASE_LEASES), true);
    return util.autoSend(r.val, r.res, r.err);
}

pub fn timeToLive(in: []const u8) anyerror!void {
    const r = util.kvShard().Read(util.withSuffix(in, types.QUERY_LEASE_TIME_TO_LIVE), true);
    return util.autoSend(r.val, r.res, r.err);
}
