//! Mirrors module/storage-kv/lease.go

const std = @import("std");

const crc32 = @import("crc.zig");
const errors = @import("error.zig");
const util = @import("util.zig");

pub const Lease = struct {
    id: u64 = 0,
    expires: u64 = 0,
    renewed: u64 = 0,

    pub fn fromBytes(key: []const u8, buf: []const u8) !Lease {
        var item = Lease{};
        if (buf.len < 6) {
            return errors.Error.ChecksumMissing;
        }
        if (std.mem.readInt(u32, buf[buf.len - 4 ..][0..4], .big) != crc32.crc(key, buf[0 .. buf.len - 4])) {
            return errors.Error.ChecksumInvalid;
        }
        const id = util.uvarint(key) orelse return errors.Error.LeaseKeyInvalid;
        item.id = id.v;
        const body = buf[0 .. buf.len - 4];
        var pos: usize = 0;
        const expires = util.uvarint(body[pos..]) orelse return errors.Error.ValueInvalid;
        item.expires = expires.v;
        pos += expires.n;
        const renewed = util.uvarint(body[pos..]) orelse return errors.Error.ValueInvalid;
        item.renewed = renewed.v;
        return item;
    }

    /// Serializes into buf (expires + renewed uvarints, then a crc keyed on
    /// the uvarint encoding of the lease id).
    pub fn bytes(item: Lease, buf: *[2 * util.max_varint_len + 4]u8) []const u8 {
        var n = util.putUvarint(buf[0..], item.expires);
        n += util.putUvarint(buf[n..], item.renewed);
        var key_buf: [util.max_varint_len]u8 = undefined;
        const key_len = util.putUvarint(&key_buf, item.id);
        std.mem.writeInt(u32, buf[n..][0..4], crc32.crc(key_buf[0..key_len], buf[0..n]), .big);
        return buf[0 .. n + 4];
    }
};
