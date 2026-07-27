//! Mirrors module/storage-kv/db_stats.go

const lmdb = @import("lmdb");

const Db = @import("db.zig").Db;

pub const DbStats = struct {
    db: Db,

    pub fn init(self: DbStats, txn: lmdb.Txn) void {
        self.db.open(txn);
    }
};
