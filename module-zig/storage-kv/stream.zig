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

pub fn printStdout(comptime fmt: []const u8, args: anytype) void {
    var buf: [64]u8 = undefined;
    const msg = std.fmt.bufPrint(&buf, fmt, args) catch return;
    const iovs = [_]std.os.wasi.ciovec_t{.{ .base = msg.ptr, .len = msg.len }};
    var nwritten: usize = undefined;
    _ = std.os.wasi.fd_write(1, &iovs, iovs.len, &nwritten);
}

pub fn open() void {
    range_watch.groupStart();
}

pub fn close() void {
    // TODO: Clean up watchCache: range_watch.Each(func(w *range_watch.Watch) { watchCache.Del(w.id) })
    range_watch.groupStop();
}

pub fn recv(data: []u8) void {
    defer _ = arena_state.reset(.retain_capacity);
    const req = decode(pb.WatchRequest, data) catch {
        std.debug.panic("Invalid command: {s}", .{data});
    };
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
            printStdout("progress request {d} {d}\n", .{ min_watch_id, rev });
            sendCodeHeader(min_watch_id, types.WatchMessageType_NOTIFY, rev);
        },
    };
}

const WatchScanResult = struct {
    rev: u64 = 0,
    sent: usize = 0,
};

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
    var res = watchScan(req, since, true) catch |err| {
        std.debug.panic("Error in event scan 1: {s}", .{@errorName(err)});
    };
    var writer = std.Io.Writer.fixed(&out);
    req.encode(&writer, arena()) catch |err| {
        std.debug.panic("Error marshaling watch create request: {s}", .{@errorName(err)});
    };
    if (since == 0) {
        module.watchRev.store(util.u64Of(req.watch_id), res.rev);
        module.watchCache.put(&watch_id_bytes, writer.buffered());
        range_watch.openstart(&watch_id_bytes, req.key, req.range_end) catch |err| {
            std.debug.panic("Error starting range watch: {s}", .{@errorName(err)});
        };
    } else {
        range_watch.open(&watch_id_bytes, req.key, req.range_end) catch |err| {
            std.debug.panic("Error starting range watch: {s}", .{@errorName(err)});
        };
        res = watchScan(req, res.rev + 1, false) catch |err| {
            std.debug.panic("Error in event scan 2: {s}", .{@errorName(err)});
        };
        module.watchRev.store(util.u64Of(req.watch_id), res.rev);
        module.watchCache.put(&watch_id_bytes, writer.buffered());
        range_watch.start(&watch_id_bytes) catch |err| {
            std.debug.panic("Error starting range watch: {s}", .{@errorName(err)});
        };
    }
    if (req.progress_notify) {
        printStdout("progress watchStart notify {d} {d}\n", .{ req.watch_id, res.rev });
        sendCodeHeader(util.u64Of(req.watch_id), types.WatchMessageType_NOTIFY, res.rev);
    }
}

fn watchScan(req: *const pb.WatchCreateRequest, since: u64, start: bool) !WatchScanResult {
    var res = WatchScanResult{};
    {
        const txn = try lmdb.begin(lmdb.readonly);
        defer txn.abort();
        res.rev = try dbMeta.getRevision(txn);
        if (start) {
            sendCodeHeader(util.u64Of(req.watch_id), types.WatchMessageType_INIT, res.rev);
        }
        if (since == 0) {
            return res;
        }
        var it = kvStore.scan(txn, scan_arena_state.allocator(), since);
        defer it.close();
        scan: while (true) {
            _ = scan_arena_state.reset(.retain_capacity);
            const evt = it.next() orelse break;
            const evt_type: pb.Event.EventType = @enumFromInt(evt.etype());
            for (req.filters.items) |f| {
                if (evt.etype() == @as(u8, @intCast(@intFromEnum(f)))) continue :scan;
            }
            switch (std.mem.order(u8, evt.key, req.key)) {
                .lt => continue :scan,
                .gt => if (std.mem.order(u8, evt.key, req.range_end) != .lt) continue :scan,
                .eq => {},
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
            var event = pb.Event{ .type = evt_type };
            if (current.rev.upper() > 0) {
                event.kv = current.toProto();
            }
            if (prev.rev.upper() > 0) {
                event.prev_kv = prev.toProto();
            }
            var watchEventBatch = pb.WatchEventBatch{ .event = event };
            watchEventBatch.revision = res.rev;
            watchEventBatch.watch_ids = try std.ArrayList(i64).initCapacity(scan_arena_state.allocator(), 1);
            watchEventBatch.watch_ids.appendAssumeCapacity(req.watch_id);
            sendCodeMsg(util.u64Of(req.watch_id), types.WatchMessageType_EVENT_BATCH, watchEventBatch);
            res.sent += 1;
        }
    }
    if (res.sent > 0) {
        var watchEventSync = pb.WatchEventSync{ .revision = res.rev };
        watchEventSync.IDs = try std.ArrayList(i64).initCapacity(scan_arena_state.allocator(), 1);
        watchEventSync.IDs.appendAssumeCapacity(req.watch_id);
        sendCodeMsg(0, types.WatchMessageType_EVENT_SYNC, watchEventSync);
    }
    return res;
}

pub fn rangeWatchRecv(notices: []range_watch.Notice) void {
    defer _ = arena_state.reset(.retain_capacity);
    var reqs = std.AutoHashMap(u64, pb.WatchCreateRequest).init(arena());
    var mins = std.AutoHashMap(u64, u64).init(arena());
    var sent = std.AutoHashMap(u64, u32).init(arena());
    var watch_event_sync = pb.WatchEventSync{};
    const revs = arena().alloc(u64, notices.len) catch |err| {
        std.debug.panic("Error allocating revisions: {s}", .{@errorName(err)});
    };
    const prev_counts = arena().alloc(u32, notices.len) catch |err| {
        std.debug.panic("Error allocating prev counts: {s}", .{@errorName(err)});
    };
    for (notices, 0..) |notice, ni| {
        revs[ni] = notice.val;
        prev_counts[ni] = 0;
        for (notice.ids) |watch_id_bytes| {
            const watch_id = std.mem.readInt(u64, watch_id_bytes[0..8], .big);
            watch_event_sync.IDs.append(arena(), util.i64Of(watch_id)) catch |err| {
                std.debug.panic("Error appending sync watch id: {s}", .{@errorName(err)});
            };
            if (!reqs.contains(watch_id)) {
                const b = module.watchCache.get(watch_id_bytes);
                if (b.len == 0) {
                    std.debug.panic("Watch request not found: {d}", .{watch_id});
                }
                const req = decode(pb.WatchCreateRequest, b) catch |err| {
                    std.debug.panic("Watch request malformed: {s}", .{@errorName(err)});
                };
                reqs.put(watch_id, req) catch |err| {
                    std.debug.panic("Error tracking watch request: {s}", .{@errorName(err)});
                };
                mins.put(watch_id, module.watchRev.load(watch_id)) catch |err| {
                    std.debug.panic("Error tracking watch min revision: {s}", .{@errorName(err)});
                };
            }
            if (reqs.get(watch_id).?.prev_kv) prev_counts[ni] += 1;
        }
    }
    var watch_event_batch = pb.WatchEventBatch{};
    {
        const txn = lmdb.begin(lmdb.readonly) catch |err| {
            std.debug.panic("Error reading events: {s}", .{@errorName(err)});
        };
        defer txn.abort();
        var it = kvStore.revScan(txn, scan_arena_state.allocator(), revs);
        defer it.close();
        var n: u64 = 0;
        var idx: usize = 0;
        while (true) {
            _ = scan_arena_state.reset(.retain_capacity);
            const evt = it.next() orelse break;
            if (n != evt.rev.upper()) {
                if (n > 0) idx += 1;
                n = evt.rev.upper();
            }
            const evt_type: pb.Event.EventType = @enumFromInt(evt.etype());
            var current = kvpkg.Kv{};
            var previous = kvpkg.Kv{};
            if (evt.rev.isdel()) {
                current = .{ .key = evt.key, .rev = evt.rev };
                if (prev_counts[idx] > 0) {
                    const g = kvStore.getRev(txn, scan_arena_state.allocator(), evt.key, evt.rev.upper(), true) catch |err| {
                        std.debug.panic("Error getting event kv: {s}", .{@errorName(err)});
                    };
                    previous = g.prev;
                }
            } else {
                const g = kvStore.getRev(txn, scan_arena_state.allocator(), evt.key, evt.rev.upper(), prev_counts[idx] > 0) catch {
                    std.debug.panic("Error getting event kv: {s}", .{evt.key});
                };
                current = g.item;
                previous = g.prev;
            }
            var event = pb.Event{ .type = evt_type };
            if (current.rev.upper() > 0) {
                event.kv = current.toProto();
            }
            if (prev_counts[idx] > 0 and previous.rev.upper() > 0) {
                event.prev_kv = previous.toProto();
            }
            watch_event_batch.watch_ids.clearRetainingCapacity();
            watch_event_batch.watch_ids_prev.clearRetainingCapacity();
            watch_event_batch.event = event;
            watch_event_batch.revision = revs[revs.len - 1];
            const notice = notices[idx];
            watches: for (notice.ids) |watch_id_bytes| {
                const watch_id = std.mem.readInt(u64, watch_id_bytes[0..8], .big);
                const req = reqs.get(watch_id) orelse continue :watches;
                const min = mins.get(watch_id) orelse 0;
                if (min > 0 and evt.rev.upper() <= min) continue :watches;
                for (req.filters.items) |f| {
                    if (evt.etype() == @as(u8, @intCast(@intFromEnum(f)))) continue :watches;
                }
                switch (std.mem.order(u8, evt.key, req.key)) {
                    .lt => continue :watches,
                    .gt => if (std.mem.order(u8, evt.key, req.range_end) != .lt) continue :watches,
                    .eq => {},
                }
                if (!req.prev_kv) {
                    watch_event_batch.watch_ids.append(arena(), util.i64Of(watch_id)) catch |err| {
                        std.debug.panic("Error appending watch id: {s}", .{@errorName(err)});
                    };
                } else {
                    watch_event_batch.watch_ids_prev.append(arena(), util.i64Of(watch_id)) catch |err| {
                        std.debug.panic("Error appending watch id: {s}", .{@errorName(err)});
                    };
                }
                const gop = sent.getOrPut(watch_id) catch |err| {
                    std.debug.panic("Error tracking sent count: {s}", .{@errorName(err)});
                };
                if (!gop.found_existing) gop.value_ptr.* = 0;
                gop.value_ptr.* += 1;
            }
            if (watch_event_batch.watch_ids.items.len > 0 or watch_event_batch.watch_ids_prev.items.len > 0) {
                sendCodeMsg(0, types.WatchMessageType_EVENT_BATCH, watch_event_batch);
            }
        }
    }
    if (watch_event_sync.IDs.items.len > 0) {
        watch_event_sync.revision = revs[revs.len - 1];
        sendCodeMsg(0, types.WatchMessageType_EVENT_SYNC, watch_event_sync);
    }
    var reqs_it = reqs.iterator();
    while (reqs_it.next()) |entry| {
        const watch_id = entry.key_ptr.*;
        if ((sent.get(watch_id) orelse 0) == 0 and entry.value_ptr.progress_notify) {
            printStdout("progress rangeWatchRecv notify {d} {d}\n", .{ watch_id, revs[revs.len - 1] });
            sendCodeHeader(watch_id, types.WatchMessageType_NOTIFY, revs[revs.len - 1]);
        }
    }
    module.watchProgress.store(revs[revs.len - 1]);
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

fn sendCodeMsg(val: u64, code: u8, msg: anytype) void {
    out[0] = code;
    var writer = std.Io.Writer.fixed(out[1..]);
    msg.encode(&writer, arena()) catch |err| {
        std.debug.panic("Error serializing message: {s}", .{@errorName(err)});
    };
    statemachine.streamSend(val, out[0 .. 1 + writer.buffered().len]);
}
