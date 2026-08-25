//! Snappy block-format codec, wire-compatible with github.com/golang/snappy.
//! Replaces the Go module's snappy dependency (no Zig equivalent exists).

const std = @import("std");

pub const Error = error{
    Corrupt,
    TooLarge,
};

const tag_literal: u8 = 0x00;
const tag_copy1: u8 = 0x01;
const tag_copy2: u8 = 0x02;
const tag_copy4: u8 = 0x03;

const max_block_size = 65536;
const input_margin = 16 - 1;
const min_non_literal_block_size = 1 + 1 + input_margin;

/// Maximum length of a snappy block, given its uncompressed length.
pub fn maxEncodedLen(src_len: usize) usize {
    return 32 + src_len + src_len / 6;
}

/// Returns the length of the decoded block.
fn decodedLen(src: []const u8) Error!struct { len: usize, n: usize } {
    var v: u64 = 0;
    var shift: u6 = 0;
    var n: usize = 0;
    while (n < src.len) {
        const b = src[n];
        n += 1;
        v |= @as(u64, b & 0x7f) << shift;
        if (b < 0x80) {
            if (v > std.math.maxInt(u32)) return Error.TooLarge;
            return .{ .len = @intCast(v), .n = n };
        }
        if (shift > 56) return Error.Corrupt;
        shift += 7;
    }
    return Error.Corrupt;
}

/// Decodes src, returning the uncompressed bytes (allocated from `allocator`).
pub fn decode(allocator: std.mem.Allocator, src: []const u8) ![]u8 {
    const hdr = try decodedLen(src);
    const dst = try allocator.alloc(u8, hdr.len);
    errdefer allocator.free(dst);
    var d: usize = 0;
    var s: usize = hdr.n;
    var offset: usize = 0;
    var length: usize = 0;
    while (s < src.len) {
        switch (src[s] & 0x03) {
            tag_literal => {
                var x: usize = src[s] >> 2;
                switch (x) {
                    0...59 => s += 1,
                    60 => {
                        s += 2;
                        if (s > src.len) return Error.Corrupt;
                        x = src[s - 1];
                    },
                    61 => {
                        s += 3;
                        if (s > src.len) return Error.Corrupt;
                        x = @as(usize, src[s - 2]) | @as(usize, src[s - 1]) << 8;
                    },
                    62 => {
                        s += 4;
                        if (s > src.len) return Error.Corrupt;
                        x = @as(usize, src[s - 3]) | @as(usize, src[s - 2]) << 8 | @as(usize, src[s - 1]) << 16;
                    },
                    else => {
                        s += 5;
                        if (s > src.len) return Error.Corrupt;
                        x = @as(usize, src[s - 4]) | @as(usize, src[s - 3]) << 8 | @as(usize, src[s - 2]) << 16 | @as(usize, src[s - 1]) << 24;
                    },
                }
                length = x + 1;
                if (length > dst.len - d or length > src.len - s) return Error.Corrupt;
                @memcpy(dst[d .. d + length], src[s .. s + length]);
                d += length;
                s += length;
                continue;
            },
            tag_copy1 => {
                s += 2;
                if (s > src.len) return Error.Corrupt;
                length = 4 + (@as(usize, src[s - 2]) >> 2 & 0x7);
                offset = (@as(usize, src[s - 2]) & 0xe0) << 3 | @as(usize, src[s - 1]);
            },
            tag_copy2 => {
                s += 3;
                if (s > src.len) return Error.Corrupt;
                length = 1 + (@as(usize, src[s - 3]) >> 2);
                offset = @as(usize, src[s - 2]) | @as(usize, src[s - 1]) << 8;
            },
            else => { // tag_copy4
                s += 5;
                if (s > src.len) return Error.Corrupt;
                length = 1 + (@as(usize, src[s - 5]) >> 2);
                offset = @as(usize, src[s - 4]) | @as(usize, src[s - 3]) << 8 | @as(usize, src[s - 2]) << 16 | @as(usize, src[s - 1]) << 24;
            },
        }
        if (offset == 0 or d < offset or length > dst.len - d) return Error.Corrupt;
        // Byte-at-a-time copy: copies can overlap forward (run-length encoding).
        var i = d - offset;
        while (length > 0) : (length -= 1) {
            dst[d] = dst[i];
            d += 1;
            i += 1;
        }
    }
    if (d != dst.len) return Error.Corrupt;
    return dst;
}

fn load32(b: []const u8, i: usize) u32 {
    return std.mem.readInt(u32, b[i..][0..4], .little);
}

fn load64(b: []const u8, i: usize) u64 {
    return std.mem.readInt(u64, b[i..][0..8], .little);
}

fn hash(u: u32, shift: u5) u32 {
    return (u *% 0x1e35a7bd) >> shift;
}

fn emitLiteral(dst: []u8, lit: []const u8) usize {
    var i: usize = 0;
    const n = lit.len - 1;
    if (n < 60) {
        dst[0] = @as(u8, @intCast(n)) << 2 | tag_literal;
        i = 1;
    } else if (n < 1 << 8) {
        dst[0] = 60 << 2 | tag_literal;
        dst[1] = @intCast(n);
        i = 2;
    } else {
        dst[0] = 61 << 2 | tag_literal;
        dst[1] = @truncate(n);
        dst[2] = @truncate(n >> 8);
        i = 3;
    }
    @memcpy(dst[i .. i + lit.len], lit);
    return i + lit.len;
}

fn emitCopy(dst: []u8, offset: usize, length_in: usize) usize {
    var length = length_in;
    var i: usize = 0;
    // Emit 64-byte copies as long as possible, then a smaller remainder.
    while (length >= 68) {
        dst[i + 0] = 63 << 2 | tag_copy2;
        dst[i + 1] = @truncate(offset);
        dst[i + 2] = @truncate(offset >> 8);
        i += 3;
        length -= 64;
    }
    if (length > 64) {
        dst[i + 0] = 59 << 2 | tag_copy2;
        dst[i + 1] = @truncate(offset);
        dst[i + 2] = @truncate(offset >> 8);
        i += 3;
        length -= 60;
    }
    if (length >= 12 or offset >= 2048) {
        dst[i + 0] = @as(u8, @intCast(length - 1)) << 2 | tag_copy2;
        dst[i + 1] = @truncate(offset);
        dst[i + 2] = @truncate(offset >> 8);
        return i + 3;
    }
    dst[i + 0] = @as(u8, @intCast(offset >> 8)) << 5 | @as(u8, @intCast(length - 4)) << 2 | tag_copy1;
    dst[i + 1] = @truncate(offset);
    return i + 2;
}

fn encodeBlock(dst: []u8, src: []const u8) usize {
    var d: usize = 0;
    const max_table_size = 1 << 14;
    const table_mask = max_table_size - 1;
    var shift: u5 = 32 - 8;
    var table_size: usize = 1 << 8;
    while (table_size < max_table_size and table_size < src.len) : (table_size *= 2) {
        shift -= 1;
    }
    var table = [_]u16{0} ** max_table_size;

    const s_limit = src.len - input_margin;
    var next_emit: usize = 0;
    var s: usize = 1;
    var next_hash = hash(load32(src, s), shift);

    outer: while (true) {
        var skip: usize = 32;
        var next_s = s;
        var candidate: usize = 0;
        while (true) {
            s = next_s;
            const bytes_between_hash_lookups = skip >> 5;
            next_s = s + bytes_between_hash_lookups;
            skip += bytes_between_hash_lookups;
            if (next_s > s_limit) break :outer;
            candidate = table[next_hash & table_mask];
            table[next_hash & table_mask] = @intCast(s);
            next_hash = hash(load32(src, next_s), shift);
            if (load32(src, s) == load32(src, candidate)) break;
        }

        d += emitLiteral(dst[d..], src[next_emit..s]);

        while (true) {
            const base = s;
            s += 4;
            var i = candidate + 4;
            while (s < src.len and src[i] == src[s]) {
                i += 1;
                s += 1;
            }
            d += emitCopy(dst[d..], base - candidate, s - base);
            next_emit = s;
            if (s >= s_limit) break :outer;

            const x = load64(src, s - 1);
            const prev_hash = hash(@truncate(x), shift);
            table[prev_hash & table_mask] = @intCast(s - 1);
            const curr_hash = hash(@truncate(x >> 8), shift);
            candidate = table[curr_hash & table_mask];
            table[curr_hash & table_mask] = @intCast(s);
            if (@as(u32, @truncate(x >> 8)) != load32(src, candidate)) {
                next_hash = hash(@truncate(x >> 16), shift);
                s += 1;
                break;
            }
        }
    }

    if (next_emit < src.len) {
        d += emitLiteral(dst[d..], src[next_emit..]);
    }
    return d;
}

/// Encodes src, returning the compressed bytes (allocated from `allocator`).
pub fn encode(allocator: std.mem.Allocator, src_in: []const u8) ![]u8 {
    const dst = try allocator.alloc(u8, maxEncodedLen(src_in.len));
    var src = src_in;

    // The block starts with the varint-encoded length of the decompressed bytes.
    var d: usize = 0;
    var v: u64 = src.len;
    while (v >= 0x80) {
        dst[d] = @as(u8, @truncate(v)) | 0x80;
        v >>= 7;
        d += 1;
    }
    dst[d] = @truncate(v);
    d += 1;

    while (src.len > 0) {
        var p = src;
        if (p.len > max_block_size) {
            p = p[0..max_block_size];
        }
        src = src[p.len..];
        if (p.len < min_non_literal_block_size) {
            d += emitLiteral(dst[d..], p);
        } else {
            d += encodeBlock(dst[d..], p);
        }
    }
    return allocator.realloc(dst, d);
}

test "snappy round trip" {
    const allocator = std.testing.allocator;
    const cases = [_][]const u8{
        "",
        "a",
        "abcdefghijklmnopqrstuvwxyz",
        "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
        "abab" ** 100,
        "The quick brown fox jumps over the lazy dog. " ** 50,
    };
    for (cases) |c| {
        const enc = try encode(allocator, c);
        defer allocator.free(enc);
        const dec = try decode(allocator, enc);
        defer allocator.free(dec);
        try std.testing.expectEqualSlices(u8, c, dec);
    }
}
