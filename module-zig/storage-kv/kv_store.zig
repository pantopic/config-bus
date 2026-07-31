//! Mirrors module/storage-kv/kv_store.go

const std = @import("std");
const lmdb = @import("lmdb");

const Db = @import("db.zig").Db;
const errors = @import("error.zig");
const kvpkg = @import("kv.zig");
const types = @import("types.zig");
const util = @import("util.zig");

const Kv = kvpkg.Kv;
const Keyrecord = kvpkg.Keyrecord;
const Keyrev = kvpkg.Keyrev;
const KvEvent = kvpkg.KvEvent;

pub const KvStore = struct {
    evt: Db,
    rev: Db,
    val: Db,

    pub fn init(self: KvStore, txn: lmdb.Txn) void {
        self.rev.open(txn);
        self.evt.open(txn);
        self.val.open(txn);
    }

    pub const PutResult = struct {
        prev: Kv = .{},
        next: Kv = .{},
        patched: bool = false,
    };

    fn putEvent(self: KvStore, txn: lmdb.Txn, rev_key: []const u8, epoch: u64, key: []const u8) !void {
        var data: [util.max_varint_len + types.PCB_LIMIT_KEY_LENGTH]u8 = undefined;
        const n = util.putUvarint(&data, epoch);
        @memcpy(data[n .. n + key.len], key);
        var out: [util.max_varint_len + types.PCB_LIMIT_KEY_LENGTH + 4]u8 = undefined;
        try txn.put(self.evt.i, rev_key, self.evt.addChecksum(rev_key, data[0 .. n + key.len], &out), 0);
    }

    pub fn put(
        self: KvStore,
        txn: lmdb.Txn,
        allocator: std.mem.Allocator,
        rev: u64,
        subrev: u64,
        lease: u64,
        epoch: u64,
        key: []const u8,
        val: []const u8,
        ignore_value: bool,
        ignore_lease: bool,
    ) !PutResult {
        var res = PutResult{};
        if (key.len == 0) {
            return errors.Error.GRPCEmptyKey;
        }
        if (key.len > types.PCB_LIMIT_KEY_LENGTH) {
            return errors.Error.GRPCKeyTooLong;
        }
        const cur = try txn.openCursor(self.rev.i);
        defer cur.close();
        var krec = Keyrecord{};
        if (cur.get(key, "", lmdb.op_set_range)) |entry| {
            if (std.mem.eql(u8, entry.key, key)) {
                krec = try Keyrecord.fromBytes(key, entry.val);
                krec.key = key;
            }
        } else |err| {
            if (err != lmdb.Error.NotFound) return err;
        }
        var krec_buf: [8 + util.max_varint_len + 4]u8 = undefined;
        var rev_key_buf: [8]u8 = undefined;
        if (krec.rev.v == 0 or krec.rev.isdel()) {
            res.next = Kv{
                .rev = Keyrev.init(rev, subrev, false),
                .version = 1,
                .created = rev,
                .lease = lease,
                .key = key,
                .val = val,
            };
            krec.key = key;
            krec.rev = res.next.rev;
            krec.lease = lease;
            try txn.put(self.rev.i, key, krec.bytes(&krec_buf), 0);
            const rev_key = krec.rev.key(&rev_key_buf);
            try txn.put(self.val.i, rev_key, try res.next.bytes(allocator, null), 0);
            try self.putEvent(txn, rev_key, epoch, key);
            return res;
        }
        if (krec.rev.upper() == rev and !types.PCB_TXN_MULTI_WRITE_ENABLED.get()) {
            return errors.Error.GRPCDuplicateKey;
        }
        var prev_rev_key_buf: [8]u8 = undefined;
        const prev_rev_key = krec.rev.key(&prev_rev_key_buf);
        const v = try allocator.dupe(u8, try txn.get(self.val.i, prev_rev_key));
        res.prev = try Kv.fromBytes(prev_rev_key, v, null, false, allocator);
        res.next = Kv{
            .rev = Keyrev.init(rev, subrev, false),
            .version = res.prev.version + 1,
            .created = res.prev.created,
            .lease = lease,
            .key = key,
            .val = val,
        };
        if (ignore_value) {
            res.next.val = res.prev.val;
        }
        if (ignore_lease) {
            res.next.lease = res.prev.lease;
        }
        if (types.PCB_PATCH_ENABLED and !krec.rev.isdel()) {
            const buf = try res.prev.bytes(allocator, val);
            res.patched = buf.len < v.len;
            if (res.patched) {
                try txn.put(self.val.i, prev_rev_key, buf, 0);
            }
        }
        krec.key = key;
        krec.rev = res.next.rev;
        krec.lease = lease;
        try txn.put(self.rev.i, key, krec.bytes(&krec_buf), 0);
        const rev_key = krec.rev.key(&rev_key_buf);
        try txn.put(self.val.i, rev_key, try res.next.bytes(allocator, null), 0);
        try self.putEvent(txn, rev_key, epoch, key);
        return res;
    }

    pub const RangeResult = struct {
        items: []Kv = &.{},
        count: usize = 0,
        more: bool = false,
    };

    pub fn getRange(
        self: KvStore,
        txn: lmdb.Txn,
        allocator: std.mem.Allocator,
        key: []const u8,
        end: []const u8,
        revision: u64,
        min_mod: u64,
        max_mod: u64,
        min_created: u64,
        max_created: u64,
        limit: u64,
        count_only_in: bool,
        keys_only: bool,
    ) !RangeResult {
        var items = std.ArrayList(Kv).empty;
        var count: usize = 0;
        var more = false;
        var count_only = count_only_in;
        var next = std.ArrayList(Keyrev).empty;
        const cur = try txn.openCursor(self.rev.i);
        defer cur.close();
        const is_full_scan = std.mem.eql(u8, key, &[_]u8{0}) and std.mem.eql(u8, end, &[_]u8{0});
        var entry_or_err = cur.get(key, "", lmdb.op_set_range);
        outer: while (true) {
            const entry = entry_or_err catch |err| {
                if (err == lmdb.Error.NotFound) break;
                return err;
            };
            const k = try allocator.dupe(u8, entry.key);
            if (!is_full_scan and end.len == 0 and !std.mem.eql(u8, k, key)) {
                break;
            }
            if (!is_full_scan and end.len > 0 and std.mem.order(u8, k, end) != .lt) {
                break;
            }
            var krec = try Keyrecord.fromBytes(k, entry.val);
            var rev = krec.rev;
            if (!count_only and limit > 0 and items.items.len == limit) {
                more = true;
                if (!types.PCB_RANGE_COUNT_FULL.get() and !types.PCB_RANGE_COUNT_FAKE.get()) {
                    return .{ .items = items.items, .count = count, .more = more };
                }
                count_only = true;
            }
            next.clearRetainingCapacity();
            var not_found = false;
            while (revision > 0 and rev.upper() > revision) {
                if (rev.isdel()) {
                    next.clearRetainingCapacity();
                } else if (!count_only) {
                    try next.append(allocator, rev);
                }
                const e2 = cur.get("", "", lmdb.op_next_dup) catch |err| {
                    if (err == lmdb.Error.NotFound) {
                        not_found = true;
                        break;
                    }
                    return err;
                };
                krec = try Keyrecord.fromBytes(k, e2.val);
                rev = krec.rev;
            }
            adv: {
                if (not_found) break :adv; // goto next
                if (min_mod > 0 and rev.upper() < min_mod) break :adv;
                if (max_mod > 0 and rev.upper() > max_mod) break :adv;
                if (!rev.isdel()) {
                    count += 1;
                    if (!count_only) {
                        try next.append(allocator, rev);
                        var item = Kv{};
                        var base: ?[]const u8 = null;
                        for (next.items) |r| {
                            var rkb: [8]u8 = undefined;
                            const rk = r.key(&rkb);
                            const v = try allocator.dupe(u8, try txn.get(self.val.i, rk));
                            item = try Kv.fromBytes(try allocator.dupe(u8, rk), v, base, keys_only, allocator);
                            base = item.val;
                        }
                        // Filtering on created revision is terribly ineffecient.
                        // It is not used by Kubernetes and should not be used by anyone.
                        if (min_created > 0 and item.created < min_created) {
                            count -= 1;
                            break :adv;
                        }
                        if (max_created > 0 and item.created > max_created) {
                            count -= 1;
                            break :adv;
                        }
                        try items.append(allocator, item);
                    } else if (types.PCB_RANGE_COUNT_FAKE.get()) {
                        break :outer;
                    }
                }
            }
            if (end.len == 0) break;
            entry_or_err = cur.get("", "", lmdb.op_next_no_dup);
        }
        return .{ .items = items.items, .count = count, .more = more };
    }

    pub const DeleteRangeResult = struct {
        items: []Keyrecord = &.{},
        count: i64 = 0,
    };

    pub fn deleteRange(
        self: KvStore,
        txn: lmdb.Txn,
        allocator: std.mem.Allocator,
        rev: u64,
        subrev: u64,
        epoch: u64,
        key: []const u8,
        end: []const u8,
    ) !DeleteRangeResult {
        var items = std.ArrayList(Keyrecord).empty;
        var count: i64 = 0;
        const cur = try txn.openCursor(self.rev.i);
        defer cur.close();
        var entry_or_err = cur.get(key, "", lmdb.op_set_range);
        while (true) {
            const entry = entry_or_err catch |err| {
                if (err == lmdb.Error.NotFound) break;
                return err;
            };
            if (entry.val.len < 12) {
                return errors.Error.ValueInvalid;
            }
            const k = try allocator.dupe(u8, entry.key);
            if (end.len == 0 and !std.mem.eql(u8, k, key)) {
                break;
            }
            if (end.len > 0 and std.mem.order(u8, k, end) == .gt) {
                return .{ .items = items.items, .count = count };
            }
            const prev_rec = try Keyrecord.fromBytes(k, entry.val);
            if (!prev_rec.rev.isdel()) {
                const tombstone = Keyrev.init(rev, subrev, true);
                if (prev_rec.rev.upper() == rev and !types.PCB_TXN_MULTI_WRITE_ENABLED.get()) {
                    return errors.Error.GRPCDuplicateKey;
                }
                const tkrec = Keyrecord{ .rev = tombstone, .key = k };
                var krec_buf: [8 + util.max_varint_len + 4]u8 = undefined;
                try txn.put(self.rev.i, k, tkrec.bytes(&krec_buf), 0);
                var tkb: [8]u8 = undefined;
                const tk = tombstone.key(&tkb);
                try self.putEvent(txn, tk, epoch, k);
                try items.append(allocator, prev_rec);
                count += 1;
            }
            if (end.len == 0) break;
            entry_or_err = cur.get("", "", lmdb.op_next_no_dup);
        }
        return .{ .items = items.items, .count = count };
    }

    pub fn deleteBatch(
        self: KvStore,
        txn: lmdb.Txn,
        rev: u64,
        subrev: u64,
        epoch: u64,
        keys: []const []const u8,
    ) !void {
        const cur = try txn.openCursor(self.rev.i);
        defer cur.close();
        for (keys) |key| {
            const entry = cur.get(key, "", lmdb.op_set_range) catch |err| {
                if (err == lmdb.Error.NotFound) return errors.Error.ModuleNotFound;
                return err;
            };
            if (entry.val.len < 12) {
                return errors.Error.ValueInvalid;
            }
            if (!std.mem.eql(u8, entry.key, key)) {
                return errors.Error.ModuleNotFound;
            }
            const prev_rec = try Keyrecord.fromBytes(key, entry.val);
            if (!prev_rec.rev.isdel()) {
                const tombstone = Keyrecord{ .key = key, .rev = Keyrev.init(rev, subrev, true) };
                var krec_buf: [8 + util.max_varint_len + 4]u8 = undefined;
                try txn.put(self.rev.i, key, tombstone.bytes(&krec_buf), 0);
                var tkb: [8]u8 = undefined;
                const tk = tombstone.rev.key(&tkb);
                try self.putEvent(txn, tk, epoch, key);
            }
        }
    }

    pub fn compact(self: KvStore, txn: lmdb.Txn, allocator: std.mem.Allocator, max: u64) !u64 {
        var last: u64 = 0;
        const cur_rev = try txn.openCursor(self.rev.i);
        defer cur_rev.close();
        const cur_evt = try txn.openCursor(self.evt.i);
        defer cur_evt.close();
        var not_found = false;
        var entry: lmdb.Cursor.Entry = .{ .key = "", .val = "" };
        if (cur_evt.get("", "", lmdb.op_next)) |e| {
            entry = e;
        } else |err| {
            if (err != lmdb.Error.NotFound) return err;
            not_found = true;
        }
        var rev = try Keyrev.fromKey(entry.key, entry.val);
        var keys = std.StringHashMapUnmanaged(Keyrev).empty;
        var done = false;
        var scanned: u64 = 0;
        var keycount: u64 = 0;
        while (!done) {
            while (!not_found) {
                if (max > 0 and rev.upper() >= max) {
                    done = true;
                    break;
                }
                if (scanned >= types.limitCompactionMaxKeys) {
                    done = true;
                    break;
                }
                scanned += 1;
                last = rev.upper();
                const body = try self.rev.trimChecksum(entry.key, entry.val);
                const n = util.uvarint(body) orelse return errors.Error.ValueInvalid;
                try keys.put(allocator, try allocator.dupe(u8, body[n.n..]), rev);
                try cur_evt.del(lmdb.current);
                if (cur_evt.get("", "", lmdb.op_next_dup)) |e| {
                    entry = e;
                } else |err| {
                    if (err != lmdb.Error.NotFound) {
                        done = true;
                        break;
                    }
                    if (cur_evt.get("", "", lmdb.op_next)) |e| {
                        entry = e;
                    } else |err2| {
                        if (err2 == lmdb.Error.NotFound) {
                            not_found = true;
                            break;
                        }
                        done = true;
                        break;
                    }
                }
                rev = Keyrev.fromKey(entry.key, entry.val) catch {
                    done = true;
                    break;
                };
            }
            var it = keys.iterator();
            while (it.next()) |kv| {
                const key = kv.key_ptr.*;
                const key_rev = kv.value_ptr.*;
                keycount += 1;
                var has_newer = false;
                var e_or_err = cur_rev.get(key, "", lmdb.op_set_range);
                while (true) {
                    const e = e_or_err catch |err| {
                        if (err == lmdb.Error.NotFound) break;
                        return err;
                    };
                    if (!std.mem.eql(u8, e.key, key)) {
                        break;
                    }
                    const rec = try Keyrecord.fromBytes(key, e.val);
                    adv: {
                        if (rec.rev.v >= key_rev.v) {
                            has_newer = true;
                            break :adv;
                        }
                        if (has_newer and !rec.rev.isdel()) {
                            var rkb: [8]u8 = undefined;
                            txn.del(self.val.i, rec.rev.key(&rkb), "") catch |err| {
                                if (err != lmdb.Error.NotFound) return err;
                            };
                        }
                        cur_rev.del(lmdb.current) catch |err| {
                            if (err != lmdb.Error.NotFound) return err;
                        };
                        has_newer = true;
                    }
                    e_or_err = cur_rev.get("", "", lmdb.op_next_dup);
                }
            }
            if (!done) {
                keys.clearRetainingCapacity();
            }
        }
        std.debug.print("compacted {d} {d}\n", .{ scanned, keycount });
        return last;
    }

    pub fn get(self: KvStore, txn: lmdb.Txn, allocator: std.mem.Allocator, key: []const u8) !Kv {
        const r = try self.getRev(txn, allocator, key, 0, false);
        return r.item;
    }

    pub const GetRevResult = struct {
        item: Kv = .{},
        prev: Kv = .{},
    };

    pub fn getRev(self: KvStore, txn: lmdb.Txn, allocator: std.mem.Allocator, key: []const u8, revision: u64, with_prev: bool) !GetRevResult {
        var res = GetRevResult{};
        self.getRevInner(txn, allocator, key, revision, with_prev, &res) catch |err| {
            // Mirrors Go's deferred IsNotFound -> nil conversion.
            if (err != lmdb.Error.NotFound) return err;
        };
        return res;
    }

    fn getRevInner(self: KvStore, txn: lmdb.Txn, allocator: std.mem.Allocator, key: []const u8, revision: u64, with_prev: bool, res: *GetRevResult) !void {
        const cur = try txn.openCursor(self.rev.i);
        defer cur.close();
        var next = std.ArrayList(Keyrev).empty;
        const first = try cur.get(key, "", lmdb.op_set_range);
        if (!std.mem.eql(u8, first.key, key)) {
            return;
        }
        var krec = try Keyrecord.fromBytes(key, first.val);
        while (revision > 0 and krec.rev.upper() > revision) {
            if (krec.rev.isdel()) {
                next.clearRetainingCapacity();
            } else {
                try next.append(allocator, krec.rev);
            }
            const e = try cur.get("", "", lmdb.op_next_dup);
            krec = try Keyrecord.fromBytes(key, e.val);
        }
        if (krec.rev.isdel()) {
            if (with_prev) {
                res.prev = try self.prev(txn, allocator, cur, res.item);
            }
            return;
        }
        try next.append(allocator, krec.rev);
        var base: ?[]const u8 = null;
        for (next.items) |rev| {
            var rkb: [8]u8 = undefined;
            const rk = rev.key(&rkb);
            const v2 = try allocator.dupe(u8, try txn.get(self.val.i, rk));
            res.item = try Kv.fromBytes(try allocator.dupe(u8, rk), v2, base, false, allocator);
            base = res.item.val;
        }
        if (with_prev) {
            res.prev = try self.prev(txn, allocator, cur, res.item);
        }
    }

    fn prev(self: KvStore, txn: lmdb.Txn, allocator: std.mem.Allocator, cur: lmdb.Cursor, item: Kv) !Kv {
        const e = try cur.get("", "", lmdb.op_next_dup);
        const prec = try Keyrecord.fromBytes(try allocator.dupe(u8, e.key), e.val);
        var rkb: [8]u8 = undefined;
        const rk = prec.rev.key(&rkb);
        const v = try allocator.dupe(u8, try txn.get(self.val.i, rk));
        return Kv.fromBytes(try allocator.dupe(u8, rk), v, if (item.rev.v != 0) item.val else null, false, allocator);
    }

    pub fn scan(self: KvStore, txn: lmdb.Txn, allocator: std.mem.Allocator, revision: u64) Scan {
        const cur = txn.openCursor(self.evt.i) catch null;
        var s = Scan{ .store = self, .cur = cur, .allocator = allocator };
        if (cur) |c| {
            var kb: [8]u8 = undefined;
            std.mem.writeInt(u64, &kb, revision << 12, .big);
            if (c.get(&kb, "", lmdb.op_set_range)) |e| {
                s.entry = e;
            } else |err| {
                if (err == lmdb.Error.NotFound) {
                    s.finished = true;
                } else {
                    std.debug.panic("{s}", .{@errorName(err)});
                }
            }
        }
        return s;
    }

    pub const Scan = struct {
        store: KvStore,
        cur: ?lmdb.Cursor,
        allocator: std.mem.Allocator,
        entry: lmdb.Cursor.Entry = .{ .key = "", .val = "" },
        finished: bool = false,
        started: bool = false,

        pub fn next(self: *Scan) ?KvEvent {
            const cur = self.cur orelse return null;
            if (self.finished) {
                self.close();
                return null;
            }
            if (self.started) {
                if (cur.get("", "", lmdb.op_next_dup)) |e| {
                    self.entry = e;
                } else |err| {
                    if (err != lmdb.Error.NotFound) std.debug.panic("{s}", .{@errorName(err)});
                    if (cur.get("", "", lmdb.op_next)) |e| {
                        self.entry = e;
                    } else |err2| {
                        if (err2 != lmdb.Error.NotFound) std.debug.panic("{s}", .{@errorName(err2)});
                        self.close();
                        return null;
                    }
                }
            }
            self.started = true;
            const evt = self.store.evtFromBytes(self.allocator, self.entry.key, self.entry.val) catch |err|
                std.debug.panic("{s}", .{@errorName(err)});
            return evt;
        }

        pub fn close(self: *Scan) void {
            if (self.cur) |cur| {
                cur.close();
                self.cur = null;
            }
        }
    };

    fn evtFromBytes(self: KvStore, allocator: std.mem.Allocator, k: []const u8, v: []const u8) !KvEvent {
        var evt = KvEvent{};
        evt.rev = try Keyrev.fromKey(k, v);
        const body = try self.evt.trimChecksum(k, v);
        const n = util.uvarint(body) orelse return errors.Error.ValueInvalid;
        evt.epoch = n.v;
        evt.key = try allocator.dupe(u8, body[n.n..]);
        return evt;
    }
};
