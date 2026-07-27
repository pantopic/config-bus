//! Mirrors module/service-grpc/service_watch.go

const std = @import("std");
const pb = @import("pb/etcdserverpb.pb.zig");

const errors = @import("error.zig");
const module = @import("module.zig");
const types = @import("types.zig");
const util = @import("util.zig");

// Backed by the wasm page allocator so it grows on demand (mirrors the Go
// module's GC), retaining capacity across per-message resets.
var arena_state = std.heap.ArenaAllocator.init(std.heap.wasm_allocator);
var out: [1536 * 1024]u8 = undefined;

fn arena() std.mem.Allocator {
    return arena_state.allocator();
}

fn decode(comptime T: type, b: []const u8) !T {
    var reader = std.Io.Reader.fixed(b);
    return T.decode(&reader, arena());
}

fn encode(msg: anytype) ![]const u8 {
    var writer = std.Io.Writer.fixed(&out);
    try msg.encode(&writer, arena());
    return writer.buffered();
}

pub fn shardRecv(name: []const u8, data: []const u8, id: u64) void {
    _ = name;
    if (id == types.WATCH_ID_ERROR) {
        std.debug.print("watch err {s}\n", .{data});
        return;
    }
    _ = arena_state.reset(.retain_capacity);
    switch (data[0]) {
        types.WatchMessageType_INIT => {
            const header = decode(pb.ResponseHeader, data[1..]) catch |err|
                std.debug.panic("Unable to unmarshal response header: {s}", .{@errorName(err)});
            const resp = pb.WatchResponse{
                .header = header,
                .watch_id = @bitCast(id),
                .created = true,
            };
            const b = encode(resp) catch |err|
                std.debug.panic("Unable to marshal watch response: {s}", .{@errorName(err)});
            module.server.send(b) catch {};
        },
        types.WatchMessageType_EVENT => {
            const events = module.bufferPoolWatchEvent.find(id);
            if (events.append(data[1..])) {
                return;
            }
            var last_rev: u64 = 0;
            var resp = pb.WatchResponse{
                .header = .{},
                .watch_id = @bitCast(id),
            };
            var it = events.iterator();
            while (it.next()) |b| {
                last_rev = std.mem.readInt(u64, b[0..8], .big);
                const evt = decode(pb.Event, b[8..]) catch |err|
                    std.debug.panic("Unable to unmarshal event: {s}", .{@errorName(err)});
                resp.events.append(arena(), evt) catch |err|
                    std.debug.panic("{s}", .{@errorName(err)});
            }
            const current_rev = std.mem.readInt(u64, data[1..9], .big);
            if (last_rev == current_rev) {
                resp.fragment = true;
            }
            resp.header.?.revision = @bitCast(last_rev);
            const res = encode(resp) catch |err|
                std.debug.panic("Unable to marshal watch response: {s}", .{@errorName(err)});
            module.server.send(res) catch {};
            events.reset();
            if (!events.append(data[1..])) {
                @panic("Failed to append watch event after reset");
            }
        },
        types.WatchMessageType_SYNC => {
            const events = module.bufferPoolWatchEvent.find(id);
            var resp = pb.WatchResponse{
                .watch_id = @bitCast(id),
            };
            resp.header = decode(pb.ResponseHeader, data[1..]) catch |err|
                std.debug.panic("Unable to unmarshal response header: {s}", .{@errorName(err)});
            var it = events.iterator();
            while (it.next()) |b| {
                const evt = decode(pb.Event, b[8..]) catch |err| {
                    std.debug.print("{d} {s}\n", .{ b.len, b });
                    events.reset();
                    std.debug.panic("Unable to unmarshal event in sync: {s}", .{@errorName(err)});
                };
                resp.events.append(arena(), evt) catch |err|
                    std.debug.panic("{s}", .{@errorName(err)});
            }
            const b = encode(resp) catch |err|
                std.debug.panic("Unable to marshal watch response: {s}", .{@errorName(err)});
            module.server.send(b) catch {};
            events.reset();
        },
        types.WatchMessageType_NOTIFY => {
            const header = decode(pb.ResponseHeader, data[1..]) catch |err|
                std.debug.panic("Unable to unmarshal response header: {s}", .{@errorName(err)});
            const resp = pb.WatchResponse{
                .header = header,
                .watch_id = @bitCast(id),
            };
            const b = encode(resp) catch |err|
                std.debug.panic("Unable to marshal watch response: {s}", .{@errorName(err)});
            module.server.send(b) catch {};
        },
        types.WatchMessageType_CANCELED => {
            const resp = pb.WatchResponse{
                .watch_id = @bitCast(id),
                .canceled = true,
            };
            const b = encode(resp) catch |err|
                std.debug.panic("Unable to marshal watch response: {s}", .{@errorName(err)});
            module.server.send(b) catch {};
            // TODO: reset watch buffer pool
        },
        types.WatchMessageType_ERR_EXISTS => {
            const resp = pb.WatchResponse{
                .watch_id = -1,
                .created = true,
                .canceled = true,
                .cancel_reason = errors.err_watcher_duplicate_id,
            };
            const b = encode(resp) catch |err|
                std.debug.panic("Unable to marshal watch response: {s}", .{@errorName(err)});
            module.server.send(b) catch {};
        },
        types.WatchMessageType_ERR_COMPACTED => {
            const header = decode(pb.ResponseHeader, data[1..]) catch |err|
                std.debug.panic("Unable to unmarshal response header: {s}", .{@errorName(err)});
            var resp = pb.WatchResponse{
                .header = header,
                .watch_id = @bitCast(id),
                .created = true,
            };
            var b = encode(resp) catch |err|
                std.debug.panic("Unable to marshal watch response: {s}", .{@errorName(err)});
            module.server.send(b) catch {};
            resp = pb.WatchResponse{
                .header = header,
                .watch_id = @bitCast(id),
                .canceled = true,
                .compact_revision = header.revision,
            };
            b = encode(resp) catch |err|
                std.debug.panic("Unable to marshal watch response: {s}", .{@errorName(err)});
            module.server.send(b) catch {};
        },
        else => @panic("Unrecognized"),
    }
}

pub fn open() anyerror!void {
    return util.kvShard().StreamOpen("watch");
}

pub fn recv(data: []const u8) anyerror!void {
    return util.kvShard().StreamSend("watch", data);
}

pub fn close() anyerror!void {}
