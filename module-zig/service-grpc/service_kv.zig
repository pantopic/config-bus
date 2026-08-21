//! Mirrors module/service-grpc/service_kv.go

const std = @import("std");
const pb = @import("pb/etcdserverpb.pb.zig");

const errors = @import("error.zig");
const module = @import("module.zig");
const types = @import("types.zig");
const util = @import("util.zig");

var scratch: [64 * 1024]u8 = undefined;
var fba = std.heap.FixedBufferAllocator.init(&scratch);

pub fn range(in: []const u8) anyerror!void {
    fba.reset();
    var reader = std.Io.Reader.fixed(in);
    const req = pb.RangeRequest.decode(&reader, fba.allocator()) catch |err| {
        try module.server.sendErr(errors.code_invalid_argument, @errorName(err));
        return;
    };
    return util.kvShard().AsyncRead(util.withSuffix(in, types.QUERY_KV_RANGE), "kvRange", req.serializable);
}

pub fn put(in: []const u8) anyerror!void {
    return util.kvShard().AsyncApply(util.withSuffix(in, types.CMD_KV_PUT), "kvPut");
}

pub fn deleteRange(in: []const u8) anyerror!void {
    return util.kvShard().AsyncApply(util.withSuffix(in, types.CMD_KV_DELETE_RANGE), "kvDeleteRange");
}

pub fn txn(in: []const u8) anyerror!void {
    return util.kvShard().AsyncApply(util.withSuffix(in, types.CMD_KV_TXN), "kvTxn");
}

pub fn compact(in: []const u8) anyerror!void {
    return util.kvShard().AsyncApply(util.withSuffix(in, types.CMD_KV_COMPACT), "kvCompact");
}
