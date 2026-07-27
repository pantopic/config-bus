//! Mirrors module/storage-kv/db_lease.go

const std = @import("std");
const lmdb = @import("lmdb");

const Db = @import("db.zig").Db;
const Lease = @import("lease.zig").Lease;
const util = @import("util.zig");

pub const DbLease = struct {
    db: Db,

    pub fn init(self: DbLease, txn: lmdb.Txn) void {
        self.db.open(txn);
    }

    pub fn get(self: DbLease, txn: lmdb.Txn, id: u64) !Lease {
        var kbuf: [util.max_varint_len]u8 = undefined;
        const k = kbuf[0..util.putUvarint(&kbuf, id)];
        const v = txn.get(self.db.i, k) catch |err| {
            if (err == lmdb.Error.NotFound) {
                return Lease{};
            }
            return err;
        };
        return Lease.fromBytes(k, v);
    }

    pub fn put(self: DbLease, txn: lmdb.Txn, item: Lease) !void {
        var kbuf: [util.max_varint_len]u8 = undefined;
        const k = kbuf[0..util.putUvarint(&kbuf, item.id)];
        var vbuf: [2 * util.max_varint_len + 4]u8 = undefined;
        return txn.put(self.db.i, k, item.bytes(&vbuf), 0);
    }

    pub fn all(self: DbLease, txn: lmdb.Txn, allocator: std.mem.Allocator) ![]Lease {
        var items = std.ArrayList(Lease).empty;
        const cur = try txn.openCursor(self.db.i);
        defer cur.close();
        while (true) {
            const entry = cur.get("", "", lmdb.op_next) catch |err| {
                if (err == lmdb.Error.NotFound) break;
                return err;
            };
            if (entry.key.len == 0) break;
            const item = try Lease.fromBytes(entry.key, entry.val);
            try items.append(allocator, item);
        }
        return items.items;
    }

    pub fn del(self: DbLease, txn: lmdb.Txn, id: u64) !void {
        var kbuf: [util.max_varint_len]u8 = undefined;
        const k = kbuf[0..util.putUvarint(&kbuf, id)];
        return txn.del(self.db.i, k, "");
    }
};
