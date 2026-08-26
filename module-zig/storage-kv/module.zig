const std = @import("std");
const atomic = @import("atomic");
const global = @import("global");
const lmdb = @import("lmdb");
const range_watch = @import("range_watch");
const small_cache = @import("small_cache");
const statemachine = @import("statemachine");
const pb = @import("pb/etcdserverpb.pb.zig");

const dbs = @import("db.zig");
const errors = @import("error.zig");
const kvpkg = @import("kv.zig");
const stream = @import("stream.zig");
const types = @import("types.zig");
const util = @import("util.zig");

const Kv = kvpkg.Kv;
const Lease = @import("lease.zig").Lease;

const dbMeta = dbs.db_meta;
const dbStats = dbs.db_stats;
const dbLease = dbs.db_lease;
const dbLeaseExp = dbs.db_lease_exp;
const dbLeaseKey = dbs.db_lease_key;
const kvStore = dbs.kv_store;

pub const codeNotFound: u64 = 5;

pub const ATOMIC_UINT64_SET = enum(u32) {
    GLOBAL,
    WATCH_REV,
};
pub const ATOMIC_UINT64_GLOBAL = enum(u64) {
    WATCH_ID_SEQ,
    WATCH_PROGRESS,
};
pub const SMALL_CACHE = enum(u64) {
    WATCH_CREATE_REQ,
};

var epoch: u64 = 0;
var new_index: u64 = 0;
var new_rev: u64 = 0;
var old_rev: u64 = 0;
var txn: lmdb.Txn = .{ .id = 0 };

pub const watchCache = small_cache
    .newLocal(@intFromEnum(SMALL_CACHE.WATCH_CREATE_REQ));
pub const watchID = atomic.Uint64Set
    .init(@intFromEnum(ATOMIC_UINT64_SET.GLOBAL))
    .find(@intFromEnum(ATOMIC_UINT64_GLOBAL.WATCH_ID_SEQ));
pub const watchProgress = atomic.Uint64Set
    .init(@intFromEnum(ATOMIC_UINT64_SET.GLOBAL))
    .find(@intFromEnum(ATOMIC_UINT64_GLOBAL.WATCH_PROGRESS));
pub const watchRev = atomic.Uint64Set
    .init(@intFromEnum(ATOMIC_UINT64_SET.WATCH_REV));

var batch_arena_state = std.heap.ArenaAllocator.init(std.heap.wasm_allocator);
var read_arena_state = std.heap.ArenaAllocator.init(std.heap.wasm_allocator);

var out: [2 * 1024 * 1024]u8 = undefined;

comptime {
    _ = statemachine;
    _ = lmdb;
    _ = atomic;
    _ = global;
    _ = range_watch;
    _ = small_cache;
}

export fn _start() void {
    range_watch.init(read_arena_state.allocator());
    statemachine.persistent(&open, &update, &finish, &read);
    statemachine.streaming(&stream.open, &stream.recv, &stream.close);
    range_watch.receive(&stream.rangeWatchRecv) catch unreachable;
}

fn decode(comptime T: type, allocator: std.mem.Allocator, b: []const u8) !T {
    var reader = std.Io.Reader.fixed(b);
    return T.decode(&reader, allocator);
}

fn encodeToOut(msg: anytype, allocator: std.mem.Allocator) ![]const u8 {
    var writer = std.Io.Writer.fixed(&out);
    try msg.encode(&writer, allocator);
    return writer.buffered();
}

fn invalidCommand(allocator: std.mem.Allocator, cmd: []const u8) []const u8 {
    return std.fmt.allocPrint(allocator, "Invalid command: {s}", .{cmd}) catch "Invalid command";
}

fn open() u64 {
    var index: u64 = 0;
    const t = lmdb.begin(0) catch |err| {
        std.debug.panic("Unable to open env {s}", .{@errorName(err)});
    };
    index = dbMeta.init(t);
    dbStats.init(t);
    kvStore.init(t);
    dbLease.init(t);
    dbLeaseExp.init(t);
    dbLeaseKey.init(t);
    t.commit() catch |err| {
        std.debug.panic("Unable to open env {s}", .{@errorName(err)});
    };
    return index;
}

fn update(index: u64, cmd: []u8) statemachine.Result {
    new_index = index;
    if (txn.id == 0) {
        txn = lmdb.begin(0) catch |err| {
            std.debug.panic("Unable to open txn: {s}", .{@errorName(err)});
        };
        epoch = dbMeta.getEpoch(txn) catch |err| {
            std.debug.panic("Unable to get epoch: {s}", .{@errorName(err)});
        };
        const rev = dbMeta.getRevision(txn) catch |err| {
            std.debug.panic("Unable to get revision: {s}", .{@errorName(err)});
        };
        new_rev = rev;
        old_rev = rev;
    }
    const arena = batch_arena_state.allocator();
    switch (cmd[cmd.len - 1]) {
        types.CMD_KV_PUT => {
            const req = decode(pb.PutRequest, arena, cmd[0 .. cmd.len - 1]) catch {
                return .{ .data = invalidCommand(arena, cmd) };
            };
            if (req.ignore_lease and req.lease != 0) {
                return .{ .data = errors.msg(error.GRPCLeaseProvided) };
            }
            if (req.ignore_value and req.value.len != 0) {
                return .{ .data = errors.msg(error.GRPCValueProvided) };
            }
            if (req.lease != 0) {
                const item = dbLease.get(txn, util.u64Of(req.lease)) catch return .{};
                if (item.id == 0) {
                    return .{ .data = errors.msg(error.GRPCLeaseNotFound) };
                }
            }
            var pr = cmdPut(txn, arena, new_rev + 1, 0, epoch, &req) catch |err| {
                if (err == error.GRPCKeyTooLong or err == error.GRPCEmptyKey) {
                    return .{ .data = errors.msg(err) };
                }
                std.debug.panic("Unable to put: {s}", .{@errorName(err)});
            };
            if (pr.affected.len > 0) {
                new_rev += 1;
                range_watch.queue(new_rev, pr.affected);
            }
            pr.res.header = responseHeader(new_rev);
            const data = encodeToOut(pr.res, arena) catch |err| {
                std.debug.panic("Unable to marshal response: {s}", .{@errorName(err)});
            };
            return .{ .value = pr.val, .data = data };
        },
        types.CMD_KV_DELETE_RANGE => {
            const req = decode(pb.DeleteRangeRequest, arena, cmd[0 .. cmd.len - 1]) catch {
                return .{ .data = invalidCommand(arena, cmd) };
            };
            var dr = cmdDeleteRange(txn, arena, new_rev + 1, 0, epoch, &req) catch |err| {
                std.debug.panic("Unable to delete range: {s}", .{@errorName(err)});
            };
            if (dr.affected.len > 0) {
                new_rev += 1;
                range_watch.queue(new_rev, dr.affected);
            }
            dr.res.header = responseHeader(new_rev);
            const data = encodeToOut(dr.res, arena) catch |err| {
                std.debug.panic("Unable to marshal response: {s}", .{@errorName(err)});
            };
            return .{ .value = 1, .data = data };
        },
        types.CMD_KV_COMPACT => {
            const req = decode(pb.CompactionRequest, arena, cmd[0 .. cmd.len - 1]) catch {
                return .{ .data = invalidCommand(arena, cmd) };
            };
            const resp = pb.CompactionResponse{
                .header = responseHeader(new_rev),
            };
            const data = encodeToOut(resp, arena) catch |err| {
                return .{ .data = errors.msg(err) };
            };
            dbMeta.setRevisionMin(txn, util.u64Of(req.revision)) catch |err| {
                return .{ .data = errors.msg(err) };
            };
            return .{ .value = 1, .data = data };
        },
        types.CMD_KV_TXN => {
            const req = decode(pb.TxnRequest, arena, cmd[0 .. cmd.len - 1]) catch {
                return .{ .data = invalidCommand(arena, cmd) };
            };
            const success = txnCompare(txn, arena, req.compare.items) catch {
                std.debug.print("txn compare fail\n", .{});
                return .{};
            };
            var res = pb.TxnResponse{
                .succeeded = success,
            };
            const ops = if (success) req.success.items else req.failure.items;
            const to = txnOps(txn, arena, new_rev + 1, epoch, ops);
            res.responses = to.res;
            const txn_err = to.err;
            if (to.affected.len > 0) {
                new_rev += 1;
                range_watch.queue(new_rev, to.affected);
            }
            if (txn_err) |err| {
                if (err == error.GRPCDuplicateKey or err == error.GRPCKeyTooLong or err == error.GRPCEmptyKey) {
                    return .{ .data = errors.msg(err) };
                }
                return .{};
            }
            res.header = responseHeader(new_rev);
            const data = encodeToOut(res, arena) catch |err| {
                std.debug.panic("Unable to marshal response: {s}", .{@errorName(err)});
            };
            return .{ .value = 1, .data = data };
        },
        types.CMD_LEASE_GRANT => {
            const req = decode(pb.LeaseGrantRequest, arena, cmd[0 .. cmd.len - 1]) catch {
                return .{ .data = invalidCommand(arena, cmd) };
            };
            var gr = cmdLeaseGrant(txn, epoch, &req) catch |err| {
                std.debug.panic("Unable to grant lease: {s}", .{@errorName(err)});
            };
            gr.res.header = responseHeader(new_rev);
            const data = encodeToOut(gr.res, arena) catch |err| {
                std.debug.panic("Unable to marshal response: {s}", .{@errorName(err)});
            };
            return .{ .value = gr.val, .data = data };
        },
        types.CMD_LEASE_REVOKE => {
            const req = decode(pb.LeaseRevokeRequest, arena, cmd[0 .. cmd.len - 1]) catch {
                return .{ .data = invalidCommand(arena, cmd) };
            };
            const rr = cmdLeaseRevoke(txn, arena, new_rev + 1, epoch, util.u64Of(req.ID)) catch |err| {
                std.debug.panic("Unable to revoke lease: {s}", .{@errorName(err)});
            };
            if (rr.keys.len > 0) {
                new_rev += 1;
                range_watch.queue(new_rev, rr.keys);
            }
            const resp = pb.LeaseRevokeResponse{
                .header = responseHeader(new_rev),
            };
            const data = encodeToOut(resp, arena) catch |err| {
                std.debug.panic("Unable to marshal response: {s}", .{@errorName(err)});
            };
            return .{ .value = rr.val, .data = data };
        },
        types.CMD_LEASE_KEEP_ALIVE => {
            const req = decode(pb.LeaseKeepAliveRequest, arena, cmd[0 .. cmd.len - 1]) catch {
                return .{ .data = invalidCommand(arena, cmd) };
            };
            var ka = cmdLeaseKeepAlive(txn, epoch, &req) catch |err| {
                std.debug.panic("Unable to keep lease alive: {s}", .{@errorName(err)});
            };
            ka.res.header = responseHeader(new_rev);
            const data = encodeToOut(ka.res, arena) catch |err| {
                std.debug.panic("Unable to marshal response: {s}", .{@errorName(err)});
            };
            return .{ .value = ka.val, .data = data };
        },
        types.CMD_LEASE_KEEP_ALIVE_BATCH => {
            const req = decode(pb.LeaseKeepAliveBatchRequest, arena, cmd[0 .. cmd.len - 1]) catch {
                return .{ .data = invalidCommand(arena, cmd) };
            };
            var kb = cmdLeaseKeepAliveBatch(txn, arena, epoch, &req) catch |err| {
                std.debug.panic("Unable to keep lease alive batch: {s}", .{@errorName(err)});
            };
            kb.res.header = responseHeader(new_rev);
            const data = encodeToOut(kb.res, arena) catch |err| {
                std.debug.panic("Unable to marshal response: {s}", .{@errorName(err)});
            };
            return .{ .value = kb.val, .data = data };
        },
        types.CMD_INTERNAL_TICK => {
            const req = decode(pb.TickRequest, arena, cmd[0 .. cmd.len - 1]) catch {
                return .{ .data = invalidCommand(arena, cmd) };
            };
            const term = dbMeta.getTerm(txn) catch |err| {
                std.debug.panic("Unable to get term: {s}", .{@errorName(err)});
            };
            if (term > req.term) {
                return .{ .data = errors.msg(error.TermExpired) };
            }
            epoch += 1;
            dbMeta.setEpoch(txn, epoch) catch |err| {
                std.debug.panic("Unable to set epoch: {s}", .{@errorName(err)});
            };
            // lease expiration
            var scan = dbLeaseExp.scan(txn, epoch);
            defer scan.close();
            while (scan.next()) |id| {
                const rr = cmdLeaseRevoke(txn, arena, new_rev + 1, epoch, id) catch |err| {
                    std.debug.panic("Unable to revoke lease: {s}", .{@errorName(err)});
                };
                if (rr.keys.len > 0) {
                    new_rev += 1;
                    range_watch.queue(new_rev, rr.keys);
                }
            }
            // amortized compaction
            const min = dbMeta.getRevisionMin(txn) catch |err| {
                return .{ .data = errors.msg(err) };
            };
            const rev = kvStore.compact(txn, arena, min) catch |err| {
                return .{ .data = errors.msg(err) };
            };
            dbMeta.setRevisionCompacted(txn, rev) catch |err| {
                return .{ .data = errors.msg(err) };
            };
            const resp = pb.TickResponse{
                .epoch = epoch,
            };
            const data = encodeToOut(resp, arena) catch |err| {
                std.debug.panic("Unable to marshal response: {s}", .{@errorName(err)});
            };
            return .{ .value = index, .data = data };
        },
        types.CMD_INTERNAL_TERM => {
            const req = decode(pb.TermRequest, arena, cmd[0 .. cmd.len - 1]) catch {
                return .{ .data = invalidCommand(arena, cmd) };
            };
            const term = dbMeta.getTerm(txn) catch |err| {
                std.debug.panic("Unable to get term: {s}", .{@errorName(err)});
            };
            if (term > req.term) {
                return .{ .data = errors.msg(error.TermExpired) };
            }
            dbMeta.setTerm(txn, req.term) catch |err| {
                std.debug.panic("Unable to set term: {s}", .{@errorName(err)});
            };
            const resp = pb.TermResponse{};
            const data = encodeToOut(resp, arena) catch |err| {
                std.debug.panic("Unable to marshal response: {s}", .{@errorName(err)});
            };
            return .{ .value = index, .data = data };
        },
        else => return .{},
    }
}

fn finish() void {
    defer _ = batch_arena_state.reset(.retain_capacity);
    dbMeta.setIndex(txn, new_index) catch |err| {
        range_watch.clear() catch unreachable;
        std.debug.panic("Unable to set index: {s}", .{@errorName(err)});
    };
    if (new_rev > old_rev) {
        dbMeta.setRevision(txn, new_rev) catch |err| {
            range_watch.clear() catch unreachable;
            std.debug.panic("Unable to set revision: {s}", .{@errorName(err)});
        };
    }
    txn.commit() catch |err| {
        range_watch.clear() catch unreachable;
        std.debug.panic("Unable to commit transaction: {s}", .{@errorName(err)});
    };
    range_watch.flush() catch unreachable;
    txn = .{ .id = 0 };
}

fn read(query: []u8) statemachine.Result {
    defer _ = read_arena_state.reset(.retain_capacity);
    const arena = read_arena_state.allocator();
    var rev: u64 = 0;
    switch (query[query.len - 1]) {
        types.QUERY_KV_RANGE => {
            const req = decode(pb.RangeRequest, arena, query[0 .. query.len - 1]) catch {
                return .{ .data = std.fmt.allocPrint(arena, "Invalid query: {s}", .{query}) catch "Invalid query" };
            };
            var resp: pb.RangeResponse = undefined;
            const err_or: ?anyerror = blk: {
                const t = lmdb.begin(lmdb.readonly) catch |err| break :blk err;
                defer t.abort();
                rev = dbMeta.getRevision(t) catch |err| break :blk err;
                resp = queryRange(t, arena, rev, &req) catch |err| break :blk err;
                break :blk null;
            };
            if (err_or) |err| {
                return .{ .data = errors.msg(err) };
            }
            const data = encodeToOut(resp, arena) catch |err| {
                return .{ .data = errors.msg(err) };
            };
            return .{ .value = 1, .data = data };
        },
        types.QUERY_LEASE_LEASES => {
            _ = decode(pb.LeaseLeasesRequest, arena, query[0 .. query.len - 1]) catch {
                return .{ .data = std.fmt.allocPrint(arena, "Invalid query: {s}", .{query}) catch "Invalid query" };
            };
            var resp: pb.LeaseLeasesResponse = undefined;
            const err_or: ?anyerror = blk: {
                const t = lmdb.begin(lmdb.readonly) catch |err| break :blk err;
                defer t.abort();
                rev = dbMeta.getRevision(t) catch |err| break :blk err;
                resp = queryLeaseLeases(t, arena) catch |err| break :blk err;
                break :blk null;
            };
            if (err_or) |err| {
                return .{ .data = errors.msg(err) };
            }
            resp.header = responseHeader(rev);
            const data = encodeToOut(resp, arena) catch |err| {
                return .{ .data = errors.msg(err) };
            };
            return .{ .value = 1, .data = data };
        },
        types.QUERY_LEASE_TIME_TO_LIVE => {
            const req = decode(pb.LeaseTimeToLiveRequest, arena, query[0 .. query.len - 1]) catch {
                return .{ .data = std.fmt.allocPrint(arena, "Invalid query: {s}", .{query}) catch "Invalid query" };
            };
            var resp: pb.LeaseTimeToLiveResponse = undefined;
            const err_or: ?anyerror = blk: {
                const t = lmdb.begin(lmdb.readonly) catch |err| break :blk err;
                defer t.abort();
                rev = dbMeta.getRevision(t) catch |err| break :blk err;
                resp = queryLeaseTimeToLive(t, &req) catch |err| break :blk err;
                break :blk null;
            };
            if (err_or) |err| {
                return .{ .data = errors.msg(err) };
            }
            resp.header = responseHeader(rev);
            const data = encodeToOut(resp, arena) catch |err| {
                return .{ .data = errors.msg(err) };
            };
            return .{ .value = 1, .data = data };
        },
        types.QUERY_WATCH_PROGRESS => {
            const resp = responseHeader(watchProgress.load());
            const data = encodeToOut(resp, arena) catch |err| {
                return .{ .data = errors.msg(err) };
            };
            stream.printStdout("progress query {d} {d}\n", .{ 0, rev });
            return .{ .value = 1, .data = data };
        },
        types.QUERY_HEADER => {
            blk: {
                const t = lmdb.begin(lmdb.readonly) catch break :blk;
                defer t.abort();
                rev = dbMeta.getRevision(t) catch break :blk;
            }
            const resp = responseHeader(rev);
            const data = encodeToOut(resp, arena) catch |err| {
                return .{ .data = errors.msg(err) };
            };
            return .{ .value = 1, .data = data };
        },
        else => return .{},
    }
}

const PutOut = struct {
    res: pb.PutResponse,
    val: u64 = 0,
    affected: []const []const u8 = &.{},
};

fn cmdPut(
    t: lmdb.Txn,
    arena: std.mem.Allocator,
    rev: u64,
    subrev: u64,
    epoch_: u64,
    req: *const pb.PutRequest,
) !PutOut {
    var res = pb.PutResponse{};
    const pr = kvStore.put(t, arena, rev, subrev, util.u64Of(req.lease), epoch_, req.key, req.value, req.ignore_value, req.ignore_lease) catch |err| {
        std.debug.print("put err {s}\n", .{errors.msg(err)});
        return err;
    };
    const prev = pr.prev;
    if (!req.ignore_lease and util.i64Of(prev.lease) != req.lease) {
        if (req.lease > 0) {
            try dbLeaseKey.put(t, util.u64Of(req.lease), req.key);
        }
        if (prev.lease > 0) {
            try dbLeaseKey.del(t, prev.lease, req.key);
        }
    }
    if (req.prev_kv) {
        res.prev_kv = prev.toProto();
    }
    const affected = try arena.alloc([]const u8, 1);
    affected[0] = req.key;
    return .{ .res = res, .val = 1, .affected = affected };
}

const DelOut = struct {
    res: pb.DeleteRangeResponse,
    affected: []const []const u8 = &.{},
};

fn cmdDeleteRange(
    t: lmdb.Txn,
    arena: std.mem.Allocator,
    rev: u64,
    subrev: u64,
    epoch_: u64,
    req: *const pb.DeleteRangeRequest,
) !DelOut {
    var res = pb.DeleteRangeResponse{};
    const dr = try kvStore.deleteRange(t, arena, rev, subrev, epoch_, req.key, req.range_end);
    var affected = std.ArrayList([]const u8).empty;
    try affected.appendNTimes(arena, "", dr.items.len);
    for (dr.items) |krec| {
        try affected.append(arena, krec.key);
        if (krec.lease != 0) {
            try dbLeaseKey.del(t, krec.lease, krec.key);
        }
    }
    res.deleted = dr.count;
    res.header = responseHeader(rev);
    if (req.prev_kv) {
        var last_err: ?anyerror = null;
        for (dr.items) |prec| {
            var item = Kv{};
            if (kvStore.getRev(t, arena, prec.key, prec.rev.upper(), false)) |g| {
                item = g.item;
            } else |err| {
                last_err = err;
            }
            try res.prev_kvs.append(arena, item.toProto());
        }
        if (last_err) |err| return err;
    }
    return .{ .res = res, .affected = affected.items };
}

const TxnOpsOut = struct {
    res: std.ArrayList(pb.ResponseOp) = .empty,
    affected: []const []const u8 = &.{},
    err: ?anyerror = null,
};

var txn_ops_arena: std.mem.Allocator = undefined;
var txn_ops_rev: u64 = 0;
var txn_ops_epoch: u64 = 0;
var txn_ops_in: []const pb.RequestOp = &.{};
var txn_ops_res: std.ArrayList(pb.ResponseOp) = .empty;
var txn_ops_keys: std.ArrayList([]const u8) = .empty;

fn txnOpsFn(t: lmdb.Txn) anyerror!void {
    const arena = txn_ops_arena;
    for (txn_ops_in, 0..) |op, i| {
        if (op.request) |r| switch (r) {
            .request_put => |put_req| {
                const po = try cmdPut(t, arena, txn_ops_rev, i, txn_ops_epoch, &put_req);
                try txn_ops_res.append(arena, .{
                    .response = .{ .response_put = po.res },
                });
                try txn_ops_keys.appendSlice(arena, po.affected);
            },
            .request_delete_range => |del_req| {
                const dr = try cmdDeleteRange(t, arena, txn_ops_rev, i, txn_ops_epoch, &del_req);
                try txn_ops_res.append(arena, .{
                    .response = .{ .response_delete_range = dr.res },
                });
                try txn_ops_keys.appendSlice(arena, dr.affected);
            },
            .request_range => |range_req| {
                const rr = try queryRange(t, arena, txn_ops_rev, &range_req);
                try txn_ops_res.append(arena, .{
                    .response = .{ .response_range = rr },
                });
            },
            .request_txn => {},
        };
    }
}

fn txnOps(
    t: lmdb.Txn,
    arena: std.mem.Allocator,
    rev: u64,
    epoch_: u64,
    ops: []const pb.RequestOp,
) TxnOpsOut {
    txn_ops_arena = arena;
    txn_ops_rev = rev;
    txn_ops_epoch = epoch_;
    txn_ops_in = ops;
    txn_ops_res = .empty;
    txn_ops_keys = .empty;
    const sub_err: ?anyerror = if (t.sub(txnOpsFn)) |_| null else |err| err;
    return .{ .res = txn_ops_res, .affected = txn_ops_keys.items, .err = sub_err };
}

fn txnCompare(t: lmdb.Txn, arena: std.mem.Allocator, conds: []const pb.Compare) !bool {
    var success = true;
    for (conds) |cond| {
        const item = try kvStore.get(t, arena, cond.key);
        if (item.key.len > 0 and !std.mem.eql(u8, cond.key, item.key)) {
            success = false;
            break;
        }
        switch (cond.target) {
            .VERSION => success = util.txnIntCompare(cond.result, util.i64Of(item.version), cond.target_union.?.version),
            .CREATE => success = util.txnIntCompare(cond.result, util.i64Of(item.created), cond.target_union.?.create_revision),
            .MOD => success = util.txnIntCompare(cond.result, util.i64Of(item.rev.upper()), cond.target_union.?.mod_revision),
            .LEASE => success = util.txnIntCompare(cond.result, util.i64Of(item.lease), cond.target_union.?.lease),
            .VALUE => {
                const v = cond.target_union.?.value;
                switch (cond.result) {
                    .EQUAL => success = std.mem.eql(u8, item.val, v),
                    .GREATER => success = std.mem.order(u8, item.val, v) == .gt,
                    .LESS => success = std.mem.order(u8, item.val, v) == .lt,
                    .NOT_EQUAL => success = !std.mem.eql(u8, item.val, v),
                    else => {},
                }
            },
            else => {},
        }
        if (!success) {
            break;
        }
    }
    return success;
}

const LeaseGrantOut = struct {
    res: pb.LeaseGrantResponse,
    val: u64 = 0,
};

fn cmdLeaseGrant(
    t: lmdb.Txn,
    epoch_: u64,
    req: *const pb.LeaseGrantRequest,
) !LeaseGrantOut {
    var res = pb.LeaseGrantResponse{};
    var item = Lease{ .id = util.u64Of(req.ID) };
    if (item.id == 0) {
        item.id = try dbMeta.getLeaseID(t);
        while (true) {
            item.id += 1;
            const found = try dbLease.get(t, item.id);
            if (found.id == 0) {
                break;
            }
        }
        try dbMeta.setLeaseID(t, item.id);
    } else {
        item = try dbLease.get(t, item.id);
        item.id = util.u64Of(req.ID);
    }
    if (item.expires > 0) {
        res.@"error" = errors.msg(error.GRPCLeaseExist);
        return .{ .res = res };
    }
    item.renewed = epoch_;
    item.expires = epoch_ + util.u64Of(req.TTL);
    try dbLease.put(t, item);
    try dbLeaseExp.put(t, item);
    res.ID = util.i64Of(item.id);
    res.TTL = req.TTL;
    return .{ .res = res, .val = 1 };
}

const LeaseRevokeOut = struct {
    keys: []const []const u8 = &.{},
    val: u64 = 0,
};

fn cmdLeaseRevoke(
    t: lmdb.Txn,
    arena: std.mem.Allocator,
    rev: u64,
    epoch_: u64,
    id: u64,
) !LeaseRevokeOut {
    const item = try dbLease.get(t, id);
    if (item.id == 0) {
        return .{ .val = codeNotFound };
    }
    var out_keys = std.ArrayList([]const u8).empty;
    while (true) {
        const batch = try dbLeaseKey.sweep(t, arena, item.id, 100);
        if (batch.len == 0) {
            break;
        }
        try kvStore.deleteBatch(t, rev, 0, epoch_, batch);
        try out_keys.appendSlice(arena, batch);
    }
    try dbLeaseExp.del(t, item);
    try dbLease.del(t, item.id);
    return .{ .keys = out_keys.items, .val = 1 };
}

const LeaseKeepAliveOut = struct {
    res: pb.LeaseKeepAliveResponse,
    val: u64 = 0,
};

fn cmdLeaseKeepAlive(
    t: lmdb.Txn,
    epoch_: u64,
    req: *const pb.LeaseKeepAliveRequest,
) !LeaseKeepAliveOut {
    var res = pb.LeaseKeepAliveResponse{ .ID = req.ID };
    var item = try dbLease.get(t, util.u64Of(req.ID));
    if (item.id == 0) {
        return .{ .res = res, .val = 1 };
    }
    res.TTL = util.i64Of(item.expires - item.renewed);
    item.expires = epoch_ + util.u64Of(res.TTL);
    item.renewed = epoch_;
    try dbLease.put(t, item);
    try dbLeaseExp.put(t, item);
    return .{ .res = res, .val = 1 };
}

const LeaseKeepAliveBatchOut = struct {
    res: pb.LeaseKeepAliveBatchResponse,
    val: u64 = 0,
};

fn cmdLeaseKeepAliveBatch(
    t: lmdb.Txn,
    arena: std.mem.Allocator,
    epoch_: u64,
    req: *const pb.LeaseKeepAliveBatchRequest,
) !LeaseKeepAliveBatchOut {
    var res = pb.LeaseKeepAliveBatchResponse{};
    for (req.IDs.items) |id| {
        var item = try dbLease.get(t, util.u64Of(id));
        if (item.id == 0) {
            try res.TTLs.append(arena, 0);
            continue;
        }
        const ttl = util.i64Of(item.expires - item.renewed);
        try res.TTLs.append(arena, ttl);
        item.expires = epoch_ + util.u64Of(ttl);
        item.renewed = epoch_;
        try dbLease.put(t, item);
        try dbLeaseExp.put(t, item);
    }
    return .{ .res = res, .val = 1 };
}

pub fn queryRange(
    t: lmdb.Txn,
    arena: std.mem.Allocator,
    rev: u64,
    req: *const pb.RangeRequest,
) !pb.RangeResponse {
    var res = pb.RangeResponse{
        .header = responseHeader(rev),
    };
    if (req.revision > 0) {
        const min = try dbMeta.getRevisionMin(t);
        if (req.revision < util.i64Of(min)) {
            return error.GRPCCompacted;
        }
        if (req.revision > util.i64Of(rev)) {
            return error.GRPCFutureRev;
        }
    }
    const rr = try kvStore.getRange(
        t,
        arena,
        req.key,
        req.range_end,
        util.u64Of(req.revision),
        util.u64Of(req.min_mod_revision),
        util.u64Of(req.max_mod_revision),
        util.u64Of(req.min_create_revision),
        util.u64Of(req.max_create_revision),
        util.u64Of(req.limit),
        req.count_only,
        req.keys_only,
    );
    if (req.count_only or types.RANGE_COUNT_FULL.get() or types.RANGE_COUNT_FAKE.get()) {
        res.count = @intCast(rr.count);
    }
    if (!req.count_only) {
        for (rr.items) |item| {
            try res.kvs.append(arena, item.toProto());
        }
        res.more = rr.more;
    }
    return res;
}

fn queryLeaseLeases(t: lmdb.Txn, arena: std.mem.Allocator) !pb.LeaseLeasesResponse {
    var res = pb.LeaseLeasesResponse{};
    const items = try dbLease.all(t, arena);
    for (items) |item| {
        try res.leases.append(arena, .{ .ID = util.i64Of(item.id) });
    }
    return res;
}

fn queryLeaseTimeToLive(t: lmdb.Txn, req: *const pb.LeaseTimeToLiveRequest) !pb.LeaseTimeToLiveResponse {
    var res = pb.LeaseTimeToLiveResponse{};
    const epoch_ = try dbMeta.getEpoch(t);
    const item = try dbLease.get(t, util.u64Of(req.ID));
    if (item.expires > 0) {
        res.TTL = util.i64Of(item.expires - epoch_);
    } else {
        res.TTL = -1;
    }
    return res;
}

pub fn responseHeader(revision: u64) pb.ResponseHeader {
    return .{
        .revision = util.i64Of(revision),
        .cluster_id = statemachine.shard_id,
        .member_id = statemachine.replica_id,
    };
}
