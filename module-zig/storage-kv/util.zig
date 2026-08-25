//! Mirrors module/storage-kv/util.go plus varint helpers shared across files
//! (Go's encoding/binary Uvarint equivalents).

const std = @import("std");
const pb = @import("pb/etcdserverpb.pb.zig");

pub fn txnIntCompare(cond: pb.Compare.CompareResult, a: i64, b: i64) bool {
    return switch (cond) {
        .EQUAL => a == b,
        .GREATER => a > b,
        .LESS => a < b,
        .NOT_EQUAL => a != b,
        else => false,
    };
}

pub const max_varint_len = 10;

/// Writes x into buf, returning the number of bytes written.
pub fn putUvarint(buf: []u8, x_in: u64) usize {
    var x = x_in;
    var i: usize = 0;
    while (x >= 0x80) {
        buf[i] = @as(u8, @truncate(x)) | 0x80;
        x >>= 7;
        i += 1;
    }
    buf[i] = @truncate(x);
    return i + 1;
}

pub fn appendUvarint(list: *std.ArrayList(u8), allocator: std.mem.Allocator, x: u64) !void {
    var buf: [max_varint_len]u8 = undefined;
    const n = putUvarint(&buf, x);
    try list.appendSlice(allocator, buf[0..n]);
}

pub const Uvarint = struct {
    v: u64,
    n: usize,
};

/// Reads a uvarint from buf; null when buf is truncated or the value
/// overflows (Go's binary.Uvarint n <= 0 cases).
pub fn uvarint(buf: []const u8) ?Uvarint {
    var v: u64 = 0;
    var shift: u6 = 0;
    var i: usize = 0;
    while (i < buf.len) {
        const b = buf[i];
        i += 1;
        if (b < 0x80) {
            if (i == max_varint_len and b > 1) return null; // overflow
            return .{ .v = v | @as(u64, b) << shift, .n = i };
        }
        if (i == max_varint_len) return null; // overflow
        v |= @as(u64, b & 0x7f) << shift;
        shift += 7;
    }
    return null;
}

pub fn i64Of(u: u64) i64 {
    return @bitCast(u);
}

pub fn u64Of(i: i64) u64 {
    return @bitCast(i);
}

test "uvarint round trip" {
    var buf: [max_varint_len]u8 = undefined;
    for ([_]u64{ 0, 1, 127, 128, 300, 1 << 32, std.math.maxInt(u64) }) |x| {
        const n = putUvarint(&buf, x);
        const r = uvarint(buf[0..n]).?;
        try std.testing.expectEqual(x, r.v);
        try std.testing.expectEqual(n, r.n);
    }
}
