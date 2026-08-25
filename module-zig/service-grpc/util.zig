//! Mirrors module/service-grpc/util.go

const std = @import("std");
const shard_client = @import("shard_client");

const errors = @import("error.zig");
const module = @import("module.zig");

pub const component_name = "default";
pub const shard_name_kv = "kv";

var suffix_buf: [(2 << 20) + 1]u8 = undefined;

pub fn withSuffix(in: []const u8, c: u8) []const u8 {
    @memcpy(suffix_buf[0..in.len], in);
    suffix_buf[in.len] = c;
    return suffix_buf[0 .. in.len + 1];
}

pub fn autoSend(val: u64, res: []const u8, err: ?[]const u8) anyerror!void {
    if (err) |e| {
        std.debug.panic("{s}", .{e});
    }
    if (val != 1) {
        return module.server.sendErr(errors.grpcCode(res), res);
    }
    return module.server.send(res);
}

pub fn kvShard() shard_client.Client {
    return shard_client.New(component_name, shard_name_kv);
}
