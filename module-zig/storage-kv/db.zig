//! Mirrors module/storage-kv/db.go

const std = @import("std");
const lmdb = @import("lmdb");

const crc32 = @import("crc.zig");
const errors = @import("error.zig");

pub const Db = struct {
    name: []const u8,
    i: lmdb.DBI,
    flags: u32,

    pub fn open(db: Db, txn: lmdb.Txn) void {
        const i = txn.openDBI(db.name, db.flags) catch |err|
            std.debug.panic("{s}", .{@errorName(err)});
        if (i != db.i) {
            std.debug.panic("Incorrect DBI: {s}({d})", .{ db.name, i });
        }
    }

    pub fn trimChecksum(db: Db, key: []const u8, val: []const u8) ![]const u8 {
        _ = db;
        if (val.len < 4) {
            return errors.Error.ChecksumInvalid;
        }
        const chk = std.mem.readInt(u32, val[val.len - 4 ..][0..4], .big);
        const body = val[0 .. val.len - 4];
        if (chk != crc32.crc(key, body)) {
            return errors.Error.ChecksumInvalid;
        }
        return body;
    }

    pub fn addChecksum(db: Db, key: []const u8, val: []const u8, out: []u8) []const u8 {
        _ = db;
        @memcpy(out[0..val.len], val);
        std.mem.writeInt(u32, out[val.len..][0..4], crc32.crc(key, val), .big);
        return out[0 .. val.len + 4];
    }

    pub fn getUint64(db: Db, txn: lmdb.Txn, key: []const u8) !u64 {
        const val = try txn.get(db.i, key);
        const body = try db.trimChecksum(key, val);
        if (body.len < 8) {
            return errors.Error.ValueInvalid;
        }
        return std.mem.readInt(u64, body[0..8], .big);
    }

    pub fn putUint64(db: Db, txn: lmdb.Txn, key: []const u8, val: u64) !void {
        var buf: [12]u8 = undefined;
        std.mem.writeInt(u64, buf[0..8], val, .big);
        std.mem.writeInt(u32, buf[8..12], crc32.crc(key, buf[0..8]), .big);
        return txn.put(db.i, key, &buf, 0);
    }
};

pub const db_meta = @import("db_meta.zig").DbMeta{
    .db = .{ .name = "meta", .i = 2, .flags = lmdb.create },
};
pub const db_stats = @import("db_stats.zig").DbStats{
    .db = .{ .name = "stats", .i = 3, .flags = lmdb.create },
};
pub const kv_store = @import("kv_store.zig").KvStore{
    .rev = .{ .name = "revision", .i = 4, .flags = lmdb.create | lmdb.dup_sort },
    .evt = .{ .name = "event", .i = 5, .flags = lmdb.create | lmdb.dup_sort },
    .val = .{ .name = "value", .i = 6, .flags = lmdb.create },
};
pub const db_lease = @import("db_lease.zig").DbLease{
    .db = .{ .name = "lease", .i = 7, .flags = lmdb.create },
};
pub const db_lease_exp = @import("db_lease_exp.zig").DbLeaseExp{
    .db = .{ .name = "lease_exp", .i = 8, .flags = lmdb.create },
};
pub const db_lease_key = @import("db_lease_key.zig").DbLeaseKey{
    .db = .{ .name = "lease_key", .i = 9, .flags = lmdb.create },
};
