//! Mirrors module/storage-kv/db_lease_key.go

const std = @import("std");
const lmdb = @import("lmdb");

const Db = @import("db.zig").Db;
const errors = @import("error.zig");
const types = @import("types.zig");
const util = @import("util.zig");

pub const DbLeaseKey = struct {
    db: Db,

    pub fn init(self: DbLeaseKey, txn: lmdb.Txn) void {
        self.db.open(txn);
    }

    fn keyOf(id: u64, key: []const u8, buf: []u8) []const u8 {
        const n = util.putUvarint(buf, id);
        @memcpy(buf[n .. n + key.len], key);
        return buf[0 .. n + key.len];
    }

    pub fn put(self: DbLeaseKey, txn: lmdb.Txn, id: u64, key: []const u8) !void {
        var kbuf: [util.max_varint_len + types.PCB_LIMIT_KEY_LENGTH]u8 = undefined;
        const k = keyOf(id, key, &kbuf);
        var vbuf: [4]u8 = undefined;
        return txn.put(self.db.i, k, self.db.addChecksum(k, "", &vbuf), 0);
    }

    /// Deletes and returns up to `max_batch` keys attached to the lease.
    /// Returned keys are allocated from `allocator`.
    pub fn sweep(self: DbLeaseKey, txn: lmdb.Txn, allocator: std.mem.Allocator, id: u64, max_batch: usize) ![][]const u8 {
        var batch = std.ArrayList([]const u8).empty;
        const cur = try txn.openCursor(self.db.i);
        defer cur.close();
        var kbuf: [util.max_varint_len]u8 = undefined;
        const prefix = kbuf[0..util.putUvarint(&kbuf, id)];
        var entry_or_err = cur.get(prefix, "", lmdb.op_set_range);
        for (0..max_batch) |_| {
            const entry = entry_or_err catch |err| {
                if (err == lmdb.Error.NotFound) break;
                return err;
            };
            if (entry.key.len == 0) break;
            _ = try self.db.trimChecksum(entry.key, entry.val);
            const found = util.uvarint(entry.key) orelse return errors.Error.LeaseKeyInvalid;
            if (found.v != id) break;
            const key = try allocator.dupe(u8, entry.key[found.n..]);
            try cur.del(lmdb.current);
            try batch.append(allocator, key);
            entry_or_err = cur.get("", "", lmdb.op_next);
        }
        return batch.items;
    }

    pub fn del(self: DbLeaseKey, txn: lmdb.Txn, id: u64, key: []const u8) !void {
        var kbuf: [util.max_varint_len + types.PCB_LIMIT_KEY_LENGTH]u8 = undefined;
        return txn.del(self.db.i, keyOf(id, key, &kbuf), "");
    }
};
