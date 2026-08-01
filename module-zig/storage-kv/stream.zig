//! Mirrors module/storage-kv/stream.go

const std = @import("std");
const atomic = @import("atomic");
const lmdb = @import("lmdb");
const range_watch = @import("range_watch");
const small_cache = @import("small_cache");
const statemachine = @import("statemachine");
const pb = @import("pb/etcdserverpb.pb.zig");

const dbs = @import("db.zig");
const errors = @import("error.zig");
const kvpkg = @import("kv.zig");
const module = @import("module.zig");
const types = @import("types.zig");
const util = @import("util.zig");

const dbMeta = dbs.db_meta;
const kvStore = dbs.kv_store;

var arena_state = std.heap.ArenaAllocator.init(std.heap.wasm_allocator);
var scan_arena_state = std.heap.ArenaAllocator.init(std.heap.wasm_allocator);
var out: [2 * 1024 * 1024]u8 = undefined;

fn arena() std.mem.Allocator {
    return arena_state.allocator();
}

fn decode(comptime T: type, b: []const u8) !T {
    var reader = std.Io.Reader.fixed(b);
    return T.decode(&reader, arena());
}

var filtered = [_]bool{false} ** 256;

pub fn rangeWatchRecv(watch_id_bytes: []const u8, revisions: []u64) void {
    _ = arena_state.reset(.retain_capacity);
    const watch_id = std.mem.readInt(u64, watch_id_bytes[0..8], .big);
    const last = module.watchRev.load(watch_id);
    const b = module.watchCache.get(watch_id_bytes);
    if (b.len == 0) {
        std.debug.print("watchCache not found: {d}\n", .{watch_id});
        return;
    }
    var i: u32 = 0;
    while (i < revisions.len and revisions[i] <= last) {
        i += 1;
    }
    if (i < revisions.len) {
        const req = decode(pb.WatchCreateRequest, b) catch @panic("Watch request malformed");
        const r = watchScanRevs(&req, revisions[i..]) catch |err| {
            std.debug.panic("Error reading events: {s}", .{@errorName(err)});
        };
        module.watchRev.store(watch_id, r.rev);
        if (r.sent == 0 and req.progress_notify) {
            sendCodeHeader(util.u64Of(req.watch_id), types.WatchMessageType_NOTIFY, r.rev);
        }
    }
}

pub fn streamOpen() void {
    // std.debug.print("wasm stream open\n", .{});
}

pub fn streamRecv(data: []u8) void {
    _ = arena_state.reset(.retain_capacity);
    const req = decode(pb.WatchRequest, data) catch
        std.debug.panic("Invalid command: {s}", .{data});
    if (req.request_union) |ut| switch (ut) {
        .create_request => |cr| {
            var create = cr;
            watchStart(&create);
        },
        .cancel_request => |cr| {
            var watch_id_bytes: [8]u8 = undefined;
            std.mem.writeInt(u64, &watch_id_bytes, util.u64Of(cr.watch_id), .big);
            range_watch.stop(&watch_id_bytes) catch {};
            module.watchCache.del(&watch_id_bytes);
            module.watchRev.del(util.u64Of(cr.watch_id));
            statemachine.streamSend(util.u64Of(cr.watch_id), &[_]u8{types.WatchMessageType_CANCELED});
        },
        .progress_request => {
            var rev: u64 = 0;
            {
                const txn = lmdb.begin(lmdb.readonly) catch |err| {
                    std.debug.panic("Unable to retrieve database revision: {s}", .{@errorName(err)});
                };
                defer txn.abort();
                rev = dbMeta.getRevision(txn) catch |err| {
                    std.debug.panic("Unable to retrieve database revision: {s}", .{@errorName(err)});
                };
            }
            var min_watch_id: u64 = 0;
            const min_watch_id_bytes = module.watchCache.min();
            if (min_watch_id_bytes.len == 8) {
                min_watch_id = std.mem.readInt(u64, min_watch_id_bytes[0..8], .big);
            }
            sendCodeHeader(min_watch_id, types.WatchMessageType_NOTIFY, rev);
        },
    };
}

const WatchScanResult = struct {
    rev: u64 = 0,
    sent: usize = 0,
};

fn watchScan(req: *const pb.WatchCreateRequest, since: u64) !WatchScanResult {
    defer @memset(&filtered, false);
    for (req.filters.items) |f| {
        const v: u8 = @truncate(@as(u32, @bitCast(@intFromEnum(f))));
        filtered[v] = true;
    }
    var res = WatchScanResult{};
    {
        const txn = try lmdb.begin(lmdb.readonly);
        defer txn.abort();
        res.rev = try dbMeta.getRevision(txn);
        if (since != 0) {
            var it = kvStore.scan(txn, scan_arena_state.allocator(), since);
            defer it.close();
            while (true) {
                _ = scan_arena_state.reset(.retain_capacity);
                const evt = it.next() orelse break;
                if (!std.mem.eql(u8, evt.key, req.key)) {
                    if (req.range_end.len == 0 or std.mem.eql(u8, req.key, req.range_end)) {
                        continue;
                    }
                    if (std.mem.order(u8, evt.key, req.key) == .lt) {
                        continue;
                    }
                    if (std.mem.order(u8, evt.key, req.range_end) != .lt) {
                        continue;
                    }
                }
                if (filtered[evt.etype()]) {
                    continue;
                }
                var current = kvpkg.Kv{};
                var prev = kvpkg.Kv{};
                if (evt.rev.isdel()) {
                    current = .{ .key = evt.key, .rev = evt.rev };
                    if (req.prev_kv) {
                        const g = try kvStore.getRev(txn, scan_arena_state.allocator(), evt.key, evt.rev.upper(), req.prev_kv);
                        prev = g.prev;
                    }
                } else {
                    const g = kvStore.getRev(txn, scan_arena_state.allocator(), evt.key, evt.rev.upper(), req.prev_kv) catch {
                        std.debug.panic("Error getting event kv: {s}", .{evt.key});
                    };
                    current = g.item;
                    prev = g.prev;
                }
                var event = pb.Event{
                    .type = @enumFromInt(evt.etype()),
                };
                if (current.rev.upper() > 0) {
                    event.kv = current.toProto();
                }
                if (prev.rev.upper() > 0) {
                    event.prev_kv = prev.toProto();
                }
                sendCodeRevMsg(util.u64Of(req.watch_id), types.WatchMessageType_EVENT, res.rev, event);
                res.sent += 1;
            }
        }
    }
    if (res.sent > 0) {
        sendCodeHeader(util.u64Of(req.watch_id), types.WatchMessageType_SYNC, res.rev);
    }
    return res;
}

fn watchScanRevs(req: *const pb.WatchCreateRequest, revisions: []u64) !WatchScanResult {
    defer @memset(&filtered, false);
    for (req.filters.items) |f| {
        const v: u8 = @truncate(@as(u32, @bitCast(@intFromEnum(f))));
        filtered[v] = true;
    }
    var res = WatchScanResult{ .rev = revisions[revisions.len - 1] };
    const txn = try lmdb.begin(lmdb.readonly);
    defer txn.abort();
    const rev: u64 = try dbMeta.getRevision(txn);
    var it = kvStore.revScan(txn, scan_arena_state.allocator(), revisions);
    defer it.close();
    while (true) {
        _ = scan_arena_state.reset(.retain_capacity);
        const evt = it.next() orelse break;
        if (filtered[evt.etype()]) {
            continue;
        }
        var current = kvpkg.Kv{};
        var prev = kvpkg.Kv{};
        if (evt.rev.isdel()) {
            current = .{ .key = evt.key, .rev = evt.rev };
            if (req.prev_kv) {
                const g = try kvStore.getRev(txn, scan_arena_state.allocator(), evt.key, evt.rev.upper(), req.prev_kv);
                prev = g.prev;
            }
        } else {
            const g = kvStore.getRev(txn, scan_arena_state.allocator(), evt.key, evt.rev.upper(), req.prev_kv) catch {
                std.debug.panic("Error getting event kv: {s}", .{evt.key});
            };
            current = g.item;
            prev = g.prev;
        }
        var event = pb.Event{
            .type = @enumFromInt(evt.etype()),
        };
        if (current.rev.upper() > 0) {
            event.kv = current.toProto();
        }
        if (prev.rev.upper() > 0) {
            event.prev_kv = prev.toProto();
        }
        sendCodeRevMsg(util.u64Of(req.watch_id), types.WatchMessageType_EVENT, rev, event);
        res.sent += 1;
    }
    if (res.sent > 0) {
        sendCodeHeader(util.u64Of(req.watch_id), types.WatchMessageType_SYNC, rev);
    }
    return res;
}

fn watchStart(req: *pb.WatchCreateRequest) void {
    const since = util.u64Of(req.start_revision);
    var min: u64 = 0;
    var watch_id_bytes: [8]u8 = undefined;
    if (req.watch_id == 0) {
        while (true) {
            req.watch_id = util.i64Of(module.watchID.add(1));
            std.mem.writeInt(u64, &watch_id_bytes, util.u64Of(req.watch_id), .big);
            range_watch.reserve(&watch_id_bytes) catch continue;
            break;
        }
    } else {
        std.mem.writeInt(u64, &watch_id_bytes, util.u64Of(req.watch_id), .big);
        range_watch.reserve(&watch_id_bytes) catch {
            statemachine.streamSend(1, &[_]u8{types.WatchMessageType_ERR_EXISTS});
            return;
        };
    }
    var compacted = false;
    {
        const txn = lmdb.begin(lmdb.readonly) catch |err| {
            std.debug.panic("Error checking min revision: {s}", .{@errorName(err)});
        };
        defer txn.abort();
        min = dbMeta.getRevisionMin(txn) catch |err| {
            std.debug.panic("Error checking min revision: {s}", .{@errorName(err)});
        };
        if (since > 0 and min > since) {
            compacted = true;
        }
    }
    if (compacted) {
        sendCodeHeader(util.u64Of(req.watch_id), types.WatchMessageType_ERR_COMPACTED, min);
        return;
    }
    sendCodeHeader(util.u64Of(req.watch_id), types.WatchMessageType_INIT, 0);
    var r = watchScan(req, since) catch |err| {
        std.debug.panic("Error in event scan 1: {s}", .{@errorName(err)});
    };
    range_watch.open(&watch_id_bytes, req.key, req.range_end) catch |err| {
        std.debug.panic("Error starting range watch: {s}", .{@errorName(err)});
    };
    r = watchScan(req, r.rev + 1) catch |err| {
        std.debug.panic("Error in event scan 2: {s}", .{@errorName(err)});
    };
    var writer = std.Io.Writer.fixed(&out);
    req.encode(&writer, arena()) catch |err| {
        std.debug.panic("Error marshaling watch create request: {s}", .{@errorName(err)});
    };
    module.watchRev.store(util.u64Of(req.watch_id), r.rev);
    module.watchCache.put(&watch_id_bytes, writer.buffered());
    range_watch.start(&watch_id_bytes) catch |err| {
        std.debug.panic("Error starting range watch: {s}", .{@errorName(err)});
    };
}

fn sendCodeHeader(val: u64, code: u8, rev: u64) void {
    const h = module.responseHeader(rev);
    var buf: [128]u8 = undefined;
    buf[0] = code;
    var writer = std.Io.Writer.fixed(buf[1..]);
    h.encode(&writer, arena()) catch |err| {
        std.debug.panic("Error marshaling header: {s}", .{@errorName(err)});
    };
    statemachine.streamSend(val, buf[0 .. 1 + writer.buffered().len]);
}

fn sendCodeRevMsg(val: u64, code: u8, rev: u64, msg: anytype) void {
    out[0] = code;
    std.mem.writeInt(u64, out[1..9], rev, .big);
    var writer = std.Io.Writer.fixed(out[9..]);
    msg.encode(&writer, scan_arena_state.allocator()) catch |err| {
        std.debug.panic("Error serializing event kv: {s}", .{@errorName(err)});
    };
    statemachine.streamSend(val, out[0 .. 9 + writer.buffered().len]);
}

pub fn streamClosed() void {
    // std.debug.print("wasm stream closed\n", .{});
}
