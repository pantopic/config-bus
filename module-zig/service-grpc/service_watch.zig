//! Mirrors module/service-grpc/service_watch.go

const std = @import("std");
const pb = @import("pb/etcdserverpb.pb.zig");
const buffer_pool = @import("buffer_pool");

const errors = @import("error.zig");
const module = @import("module.zig");
const types = @import("types.zig");
const util = @import("util.zig");

var arena_state = std.heap.ArenaAllocator.init(std.heap.wasm_allocator);
var out: [1536 * 1024 + 8]u8 = undefined;

fn decode(comptime T: type, b: []const u8) !T {
    var reader = std.Io.Reader.fixed(b);
    return T.decode(&reader, arena_state.allocator());
}

fn encode(msg: anytype) ![]const u8 {
    var writer = std.Io.Writer.fixed(&out);
    try msg.encode(&writer, arena_state.allocator());
    return writer.buffered();
}

pub fn shardRecv(name: []const u8, data: []const u8, id: u64) void {
    _ = name;
    if (id == types.WATCH_ID_ERROR) {
        std.debug.print("watch err {s}\n", .{data});
        return;
    }
    defer _ = arena_state.reset(.retain_capacity);
    switch (data[0]) {
        types.WatchMessageType_INIT => {
            const header = decode(pb.ResponseHeader, data[1..]) catch |err| {
                std.debug.panic("Unable to unmarshal response header: {s}", .{@errorName(err)});
            };
            const resp = pb.WatchResponse{
                .header = header,
                .watch_id = @bitCast(id),
                .created = true,
            };
            const b = encode(resp) catch |err| {
                std.debug.panic("Unable to marshal watch response: {s}", .{@errorName(err)});
            };
            module.server.send(b) catch unreachable;
        },
        types.WatchMessageType_EVENT_BATCH => {
            var watchEventBatch = decode(pb.WatchEventBatch, data[1..]) catch |err| {
                std.debug.panic("Unable to unmarshal watch event batch: {s}", .{@errorName(err)});
            };
            if (watchEventBatch.watch_ids_prev.items.len > 0) {
                const b = encode(watchEventBatch.event.?) catch |err| {
                    std.debug.panic("Unable to marshal watch event batch event: {s}", .{@errorName(err)});
                };
                for (watchEventBatch.watch_ids_prev.items) |watch_id| {
                    sendEvent(watch_id, watchEventBatch.revision, b) catch |err| {
                        std.debug.panic("Unable to send watch event batch event: {s}", .{@errorName(err)});
                    };
                }
            }
            if (watchEventBatch.watch_ids.items.len > 0) {
                watchEventBatch.event.?.prev_kv = null;
                const b = encode(watchEventBatch.event.?) catch |err| {
                    std.debug.panic("Unable to marshal watch event batch event: {s}", .{@errorName(err)});
                };
                for (watchEventBatch.watch_ids.items) |watch_id| {
                    sendEvent(watch_id, watchEventBatch.revision, b) catch |err| {
                        std.debug.panic("Unable to send watch event batch event: {s}", .{@errorName(err)});
                    };
                }
            }
        },
        types.WatchMessageType_EVENT_SYNC => {
            const watchEventSync = decode(pb.WatchEventSync, data[1..]) catch |err| {
                std.debug.panic("Unable to unmarshal watch event sync: {s}", .{@errorName(err)});
            };
            for (watchEventSync.IDs.items) |watch_id| {
                const events = module.bufferPoolWatchEvent.find(@bitCast(watch_id));
                clearEvents(events, watch_id, watchEventSync.revision, true) catch |err| {
                    std.debug.panic("Unable to clear watch events: {s}", .{@errorName(err)});
                };
            }
        },
        types.WatchMessageType_NOTIFY => {
            const header = decode(pb.ResponseHeader, data[1..]) catch |err| {
                std.debug.panic("Unable to unmarshal response header: {s}", .{@errorName(err)});
            };
            const resp = pb.WatchResponse{
                .header = header,
                .watch_id = @bitCast(id),
            };
            const b = encode(resp) catch |err| {
                std.debug.panic("Unable to marshal watch response: {s}", .{@errorName(err)});
            };
            module.server.send(b) catch {};
        },
        types.WatchMessageType_CANCELED => {
            const resp = pb.WatchResponse{
                .watch_id = @bitCast(id),
                .canceled = true,
            };
            const b = encode(resp) catch |err| {
                std.debug.panic("Unable to marshal watch response: {s}", .{@errorName(err)});
            };
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
            const b = encode(resp) catch |err| {
                std.debug.panic("Unable to marshal watch response: {s}", .{@errorName(err)});
            };
            module.server.send(b) catch {};
        },
        types.WatchMessageType_ERR_COMPACTED => {
            const header = decode(pb.ResponseHeader, data[1..]) catch |err| {
                std.debug.panic("Unable to unmarshal response header: {s}", .{@errorName(err)});
            };
            var resp = pb.WatchResponse{
                .header = header,
                .watch_id = @bitCast(id),
                .created = true,
            };
            var b = encode(resp) catch |err| {
                std.debug.panic("Unable to marshal watch response: {s}", .{@errorName(err)});
            };
            module.server.send(b) catch {};
            resp = pb.WatchResponse{
                .header = header,
                .watch_id = @bitCast(id),
                .canceled = true,
                .compact_revision = header.revision,
            };
            b = encode(resp) catch |err| {
                std.debug.panic("Unable to marshal watch response: {s}", .{@errorName(err)});
            };
            module.server.send(b) catch {};
        },
        else => @panic("Unrecognized"),
    }
}

fn sendEvent(watch_id: i64, rev: u64, data: []const u8) !void {
    var rev_buf: [8]u8 = undefined;
    std.mem.writeInt(u64, &rev_buf, rev, .big);
    const data2: []const u8 = try std.mem.concat(arena_state.allocator(), u8, &.{ data, &rev_buf });
    const events = module.bufferPoolWatchEvent.find(@bitCast(watch_id));
    if (events.append(data2)) {
        return;
    }
    clearEvents(events, watch_id, rev, false) catch |err| {
        std.debug.panic("Unable to clear watch events: {s}", .{@errorName(err)});
    };
    if (!events.append(data2)) {
        @panic("Failed to append watch event after reset");
    }
}

fn clearEvents(events: buffer_pool.MultiValue, watch_id: i64, rev: u64, sync: bool) !void {
    var resp = pb.WatchResponse{
        .header = .{},
        .watch_id = @bitCast(watch_id),
    };
    var last_rev: u64 = 0;
    var it = events.iterator();
    while (it.next()) |b| {
        const rev_bytes: *const [8]u8 = @ptrCast(b[b.len - 8 ..].ptr);
        last_rev = std.mem.readInt(u64, rev_bytes, .big);
        const evt = decode(pb.Event, b[0 .. b.len - 8]) catch |err|
            std.debug.panic("Unable to unmarshal event: {s}", .{@errorName(err)});
        resp.events.append(arena_state.allocator(), evt) catch |err|
            std.debug.panic("{s}", .{@errorName(err)});
    }
    if (resp.events.items.len == 0) {
        return;
    }
    resp.fragment = !sync and last_rev == rev;
    resp.header.?.revision = @bitCast(last_rev);
    const res = encode(resp) catch |err|
        std.debug.panic("Unable to marshal watch response: {s}", .{@errorName(err)});
    module.server.send(res) catch {};
    events.reset();
}

pub fn open() anyerror!void {
    return util.kvShard().StreamOpen("watch");
}

pub fn recv(data: []const u8) anyerror!void {
    return util.kvShard().StreamSend("watch", data);
}

pub fn close() anyerror!void {}
