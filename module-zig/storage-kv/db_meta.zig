//! Mirrors module/storage-kv/db_meta.go

const std = @import("std");
const lmdb = @import("lmdb");

const Db = @import("db.zig").Db;

/// Logical clock representing seconds of uptime since shard creation
const meta_key_epoch = "epoch";

/// Index of last applied raft log entry
const meta_key_index = "index";

/// Autoincrement cursor for generating lease ids
const meta_key_lease_id = "lease_id";

/// Last applied data revision
const meta_key_revision = "rev";

/// Compaction cursor - Keys up to this revision have been compacted (always <= rev_min)
const meta_key_revision_compacted = "rev_compacted";

/// Compaction target - Keys up to this revision are no longer visible
const meta_key_revision_min = "rev_min";

/// Shard raft term - Prevents duplicate controllers
const meta_key_term = "term";

pub const DbMeta = struct {
    db: Db,

    pub fn init(self: DbMeta, txn: lmdb.Txn) u64 {
        self.db.open(txn);
        for ([_][]const u8{
            meta_key_epoch,
            meta_key_index,
            meta_key_lease_id,
            meta_key_revision_compacted,
            meta_key_term,
        }) |k| {
            _ = self.db.getUint64(txn, k) catch |err| {
                if (err != lmdb.Error.NotFound) {
                    std.debug.panic("{s}", .{@errorName(err)});
                }
                self.db.putUint64(txn, k, 0) catch |perr|
                    std.debug.panic("{s}", .{@errorName(perr)});
            };
        }
        for ([_][]const u8{
            meta_key_revision,
            meta_key_revision_min,
        }) |k| {
            _ = self.db.getUint64(txn, k) catch |err| {
                if (err != lmdb.Error.NotFound) {
                    return 0;
                }
                self.db.putUint64(txn, k, 1) catch {
                    return 0;
                };
            };
        }
        return self.getIndex(txn) catch |err|
            std.debug.panic("{s}", .{@errorName(err)});
    }

    pub fn getEpoch(self: DbMeta, txn: lmdb.Txn) !u64 {
        return self.db.getUint64(txn, meta_key_epoch);
    }

    pub fn setEpoch(self: DbMeta, txn: lmdb.Txn, val: u64) !void {
        return self.db.putUint64(txn, meta_key_epoch, val);
    }

    pub fn getIndex(self: DbMeta, txn: lmdb.Txn) !u64 {
        return self.db.getUint64(txn, meta_key_index);
    }

    pub fn setIndex(self: DbMeta, txn: lmdb.Txn, val: u64) !void {
        return self.db.putUint64(txn, meta_key_index, val);
    }

    pub fn getLeaseID(self: DbMeta, txn: lmdb.Txn) !u64 {
        return self.db.getUint64(txn, meta_key_lease_id);
    }

    pub fn setLeaseID(self: DbMeta, txn: lmdb.Txn, val: u64) !void {
        return self.db.putUint64(txn, meta_key_lease_id, val);
    }

    pub fn getRevision(self: DbMeta, txn: lmdb.Txn) !u64 {
        return self.db.getUint64(txn, meta_key_revision);
    }

    pub fn setRevision(self: DbMeta, txn: lmdb.Txn, val: u64) !void {
        return self.db.putUint64(txn, meta_key_revision, val);
    }

    pub fn getRevisionCompacted(self: DbMeta, txn: lmdb.Txn) !u64 {
        return self.db.getUint64(txn, meta_key_revision_compacted);
    }

    pub fn setRevisionCompacted(self: DbMeta, txn: lmdb.Txn, val: u64) !void {
        return self.db.putUint64(txn, meta_key_revision_compacted, val);
    }

    pub fn getRevisionMin(self: DbMeta, txn: lmdb.Txn) !u64 {
        return self.db.getUint64(txn, meta_key_revision_min);
    }

    pub fn setRevisionMin(self: DbMeta, txn: lmdb.Txn, val: u64) !void {
        return self.db.putUint64(txn, meta_key_revision_min, val);
    }

    pub fn getTerm(self: DbMeta, txn: lmdb.Txn) !u64 {
        return self.db.getUint64(txn, meta_key_term);
    }

    pub fn setTerm(self: DbMeta, txn: lmdb.Txn, val: u64) !void {
        return self.db.putUint64(txn, meta_key_term, val);
    }
};
