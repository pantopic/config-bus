//! Mirrors module/service-grpc/service_maintenance.go

const std = @import("std");
const pb = @import("pb/etcdserverpb.pb.zig");

const errors = @import("error.zig");
const module = @import("module.zig");
const types = @import("types.zig");
const util = @import("util.zig");

var scratch: [16 * 1024]u8 = undefined;
var fba = std.heap.FixedBufferAllocator.init(&scratch);
var out: [16 * 1024]u8 = undefined;

pub fn alarm(in: []const u8) anyerror!void {
    _ = in;
    return module.server.send("");
}

pub fn status(in: []const u8) anyerror!void {
    const r = util.kvShard().Read(util.withSuffix(in, types.QUERY_HEADER), false);
    if (r.err) |e| std.debug.panic("{s}", .{e});
    if (r.val != 1) {
        return module.server.sendErr(errors.grpcCode(r.res), r.res);
    }
    fba.reset();
    var reader = std.Io.Reader.fixed(r.res);
    const header = pb.ResponseHeader.decode(&reader, fba.allocator()) catch |err| {
        return module.server.sendErr(errors.code_unknown, @errorName(err));
    };
    const resp = pb.StatusResponse{
        .header = header,
        .version = "3.6.5",
        .dbSize = 28672,
        .dbSizeInUse = 28672,
        .isLearner = false,
        .leader = header.member_id,
        .raftIndex = @intCast(header.revision),
        .raftTerm = 1,
        .raftAppliedIndex = @intCast(header.revision),
    };
    var writer = std.Io.Writer.fixed(&out);
    try resp.encode(&writer, fba.allocator());
    return module.server.send(writer.buffered());
}

pub fn defragment(in: []const u8) anyerror!void {
    _ = in;
    return module.server.send("");
}

pub fn hash(in: []const u8) anyerror!void {
    _ = in;
    return module.server.send("");
}

pub fn hashKV(in: []const u8) anyerror!void {
    _ = in;
    return module.server.send("");
}

pub fn snapshotOpen(in: []const u8) anyerror!void {
    _ = in;
    return module.server.send("");
}

pub fn snapshotClose() anyerror!void {
    return module.server.send("");
}

pub fn moveLeader(in: []const u8) anyerror!void {
    _ = in;
    return module.server.send("");
}

pub fn downgrade(in: []const u8) anyerror!void {
    _ = in;
    return module.server.send("");
}
