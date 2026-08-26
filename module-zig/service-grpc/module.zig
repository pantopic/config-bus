//! Mirrors module/service-grpc/main.go

const std = @import("std");
const buffer = @import("buffer");
const grpc_server = @import("grpc_server");
const shard_client = @import("shard_client");

const cluster = @import("service_cluster.zig");
const kv = @import("service_kv.zig");
const lease = @import("service_lease.zig");
const maintenance = @import("service_maintenance.zig");
const types = @import("types.zig");
const watch = @import("service_watch.zig");
const util = @import("util.zig");

pub const BUFFER_POOL_WATCH_EVENT: u64 = 0;

pub var bufferPoolWatchEvent: buffer.MultiValueSet = undefined;

pub const server = grpc_server.Server(.{
    .method_cap = 256,
    .msg_cap = 1536 * 1024,
    .services = &.{
        .{
            .name = "etcdserverpb.Cluster",
            .methods = &.{
                .{ .name = "MemberAdd", .handler = .{ .unary = &cluster.memberAdd } },
                .{ .name = "MemberRemove", .handler = .{ .unary = &cluster.memberRemove } },
                .{ .name = "MemberUpdate", .handler = .{ .unary = &cluster.memberUpdate } },
                .{ .name = "MemberList", .handler = .{ .unary = &cluster.memberList } },
                .{ .name = "MemberPromote", .handler = .{ .unary = &cluster.memberPromote } },
            },
        },
        .{
            .name = "etcdserverpb.KV",
            .methods = &.{
                .{ .name = "Range", .handler = .{ .unary = &kv.range } },
                .{ .name = "Put", .handler = .{ .unary = &kv.put } },
                .{ .name = "DeleteRange", .handler = .{ .unary = &kv.deleteRange } },
                .{ .name = "Txn", .handler = .{ .unary = &kv.txn } },
                .{ .name = "Compact", .handler = .{ .unary = &kv.compact } },
            },
        },
        .{
            .name = "etcdserverpb.Lease",
            .methods = &.{
                .{ .name = "LeaseGrant", .handler = .{ .unary = &lease.grant } },
                .{ .name = "LeaseRevoke", .handler = .{ .unary = &lease.revoke } },
                .{ .name = "LeaseKeepAlive", .handler = .{ .bidirectional = .{
                    .open = &lease.keepaliveOpen,
                    .recv = &lease.keepaliveRecv,
                    .close = &lease.keepaliveClose,
                } } },
                .{ .name = "LeaseLeases", .handler = .{ .unary = &lease.leases } },
                .{ .name = "LeaseTimeToLive", .handler = .{ .unary = &lease.timeToLive } },
            },
        },
        .{
            .name = "etcdserverpb.Maintenance",
            .methods = &.{
                .{ .name = "Alarm", .handler = .{ .unary = &maintenance.alarm } },
                .{ .name = "Status", .handler = .{ .unary = &maintenance.status } },
                .{ .name = "Defragment", .handler = .{ .unary = &maintenance.defragment } },
                .{ .name = "Hash", .handler = .{ .unary = &maintenance.hash } },
                .{ .name = "HashKV", .handler = .{ .unary = &maintenance.hashKV } },
                .{ .name = "Snapshot", .handler = .{ .server_stream = .{
                    .open = &maintenance.snapshotOpen,
                    .close = &maintenance.snapshotClose,
                } } },
                .{ .name = "MoveLeader", .handler = .{ .unary = &maintenance.moveLeader } },
                .{ .name = "Downgrade", .handler = .{ .unary = &maintenance.downgrade } },
            },
        },
        .{
            .name = "etcdserverpb.Watch",
            .methods = &.{
                .{ .name = "Watch", .handler = .{ .bidirectional = .{
                    .open = &watch.open,
                    .recv = &watch.recv,
                    .close = &watch.close,
                } } },
            },
        },
    },
    .http = &httpHandler,
});

comptime {
    server.register();
    _ = shard_client.abi; // emit the SDK's __shard_client* exports
}

export fn _start() void {
    shard_client.RegisterStreamRecv(&watch.shardRecv) catch |err| {
        std.debug.panic("RegisterStreamRecv failed: {s}", .{@errorName(err)});
    };
    shard_client.RegisterAsyncRecv(&asyncRecv) catch |err| {
        std.debug.panic("RegisterAsyncRecv failed: {s}", .{@errorName(err)});
    };
    bufferPoolWatchEvent = buffer.MultiValueSet.init(
        BUFFER_POOL_WATCH_EVENT,
        .{ .size_limit = types.PCB_RESPONSE_SIZE_MAX },
    );
}

pub fn asyncRecv(name: []const u8, data: []const u8, val: u64, err: ?[]const u8) void {
    _ = name;
    util.autoSend(val, data, err) catch |e| {
        std.debug.panic("autoSend failed: {s}", .{@errorName(e)});
    };
}

fn httpHandler(method: []const u8, path: []const u8, body: []const u8) grpc_server.HttpResponse {
    _ = method;
    _ = body;
    if (std.mem.eql(u8, path, "/metrics")) {
        return .{ .body = "pantopic_power_level 9001" };
    }
    if (std.mem.eql(u8, path, "/health")) {
        return .{ .body = "{\"health\":\"true\",\"reason\":\"\"}" };
    }
    if (std.mem.eql(u8, path, "/version")) {
        return .{ .body = "{\"etcdserver\":\"3.5.25\",\"etcdcluster\":\"3.5.0\"}" };
    }
    return .{ .code = 405 };
}
