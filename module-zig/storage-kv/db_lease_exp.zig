//! Mirrors module/storage-kv/db_lease_exp.go

const std = @import("std");
const lmdb = @import("lmdb");

const Db = @import("db.zig").Db;
const Lease = @import("lease.zig").Lease;
const util = @import("util.zig");

pub const DbLeaseExp = struct {
    db: Db,

    pub fn init(self: DbLeaseExp, txn: lmdb.Txn) void {
        self.db.open(txn);
    }

    fn keyOf(item: Lease, buf: *[8 + util.max_varint_len]u8) []const u8 {
        std.mem.writeInt(u64, buf[0..8], item.expires, .big);
        const n = util.putUvarint(buf[8..], item.id);
        return buf[0 .. 8 + n];
    }

    pub fn put(self: DbLeaseExp, txn: lmdb.Txn, item: Lease) !void {
        var kbuf: [8 + util.max_varint_len]u8 = undefined;
        const k = keyOf(item, &kbuf);
        var vbuf: [4]u8 = undefined;
        return txn.put(self.db.i, k, self.db.addChecksum(k, "", &vbuf), 0);
    }

    pub fn del(self: DbLeaseExp, txn: lmdb.Txn, item: Lease) !void {
        var kbuf: [8 + util.max_varint_len]u8 = undefined;
        return txn.del(self.db.i, keyOf(item, &kbuf), "");
    }

    /// Iterates ids of leases expiring at or before `expires`.
    /// Mirrors Go's scan iterator; the cursor closes when iteration ends.
    pub fn scan(self: DbLeaseExp, txn: lmdb.Txn, expires: u64) Scan {
        const cur = txn.openCursor(self.db.i) catch {
            return .{ .db = self.db, .cur = null, .expires = expires };
        };
        return .{ .db = self.db, .cur = cur, .expires = expires };
    }

    pub const Scan = struct {
        db: Db,
        cur: ?lmdb.Cursor,
        expires: u64,

        pub fn next(self: *Scan) ?u64 {
            const cur = self.cur orelse return null;
            const entry = cur.get("", "", lmdb.op_next) catch {
                self.close();
                return null;
            };
            _ = self.db.trimChecksum(entry.key, entry.val) catch {
                self.close();
                return null;
            };
            if (entry.key.len < 9) {
                self.close();
                return null;
            }
            if (std.mem.readInt(u64, entry.key[0..8], .big) > self.expires) {
                self.close();
                return null;
            }
            const id = util.uvarint(entry.key[8..]) orelse {
                self.close();
                return null;
            };
            return id.v;
        }

        pub fn close(self: *Scan) void {
            if (self.cur) |cur| {
                cur.close();
                self.cur = null;
            }
        }
    };
};
