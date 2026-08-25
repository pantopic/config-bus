//! Mirrors module/storage-kv/kv.go

const std = @import("std");
const pb = @import("pb/etcdserverpb.pb.zig");

const crc32 = @import("crc.zig");
const errors = @import("error.zig");
const patch = @import("patch.zig");
const snappy = @import("snappy.zig");
const types = @import("types.zig");
const util = @import("util.zig");

pub const rev_mask_lower: u64 = std.math.maxInt(u64) >> 54;

pub const rev_mask_delete: u64 = 1 << 1;
pub const rev_mask_reserved: u64 = 1 << 2;

pub const Keyrev = struct {
    v: u64 = 0,

    pub fn init(upper_: u64, lower_: u64, is_del: bool) Keyrev {
        var a = (upper_ << 12) +% (lower_ << 2);
        if (is_del) {
            a |= rev_mask_delete;
        }
        return .{ .v = a };
    }

    pub fn invert(kr: Keyrev) Keyrev {
        return .{ .v = std.math.maxInt(u64) - kr.v };
    }

    /// Plain revision number without subrevision or flags
    pub fn upper(kr: Keyrev) u64 {
        return kr.v >> 12;
    }

    pub fn lower(kr: Keyrev) u64 {
        return kr.v >> 2 & rev_mask_lower;
    }

    pub fn isdel(kr: Keyrev) bool {
        return kr.v & rev_mask_delete > 0;
    }

    pub fn key(kr: Keyrev, buf: *[8]u8) []const u8 {
        std.mem.writeInt(u64, buf, kr.v, .big);
        return buf;
    }

    pub fn fromBytes(key_: []const u8, buf: []const u8) !Keyrev {
        if (buf.len < 11) {
            return errors.Error.ChecksumMissing;
        }
        if (std.mem.readInt(u32, buf[buf.len - 4 ..][0..4], .big) != crc32.crc(key_, buf[0 .. buf.len - 4])) {
            return errors.Error.ChecksumInvalid;
        }
        return (Keyrev{ .v = std.mem.readInt(u64, buf[0..8], .big) }).invert();
    }

    pub fn fromKey(key_: []const u8, buf: []const u8) !Keyrev {
        if (buf.len < 4) {
            return errors.Error.ChecksumMissing;
        }
        if (std.mem.readInt(u32, buf[buf.len - 4 ..][0..4], .big) != crc32.crc(key_, buf[0 .. buf.len - 4])) {
            return errors.Error.ChecksumInvalid;
        }
        return .{ .v = std.mem.readInt(u64, key_[0..8], .big) };
    }

    pub fn bytes(kr: Keyrev, key_: []const u8, buf: *[12]u8) []const u8 {
        std.mem.writeInt(u64, buf[0..8], kr.invert().v, .big);
        std.mem.writeInt(u32, buf[8..12], crc32.crc(key_, buf[0..8]), .big);
        return buf;
    }
};

pub const Keyrecord = struct {
    key: []const u8 = &.{},
    rev: Keyrev = .{},
    lease: u64 = 0,

    pub fn fromBytes(key_: []const u8, buf: []const u8) !Keyrecord {
        var kr = Keyrecord{};
        if (buf.len < 11) {
            return errors.Error.ChecksumMissing;
        }
        if (std.mem.readInt(u32, buf[buf.len - 4 ..][0..4], .big) != crc32.crc(key_, buf[0 .. buf.len - 4])) {
            return errors.Error.ChecksumInvalid;
        }
        kr.rev = (Keyrev{ .v = std.mem.readInt(u64, buf[0..8], .big) }).invert();
        kr.key = key_;
        if (!kr.rev.isdel()) {
            if (util.uvarint(buf[8..])) |r| {
                kr.lease = r.v;
            }
        }
        return kr;
    }

    pub fn bytes(kr: Keyrecord, buf: *[8 + util.max_varint_len + 4]u8) []const u8 {
        std.mem.writeInt(u64, buf[0..8], kr.rev.invert().v, .big);
        var n: usize = 8;
        if (!kr.rev.isdel()) {
            n += util.putUvarint(buf[n..], kr.lease);
        }
        std.mem.writeInt(u32, buf[n..][0..4], crc32.crc(kr.key, buf[0..n]), .big);
        return buf[0 .. n + 4];
    }
};

pub const Kv = struct {
    rev: Keyrev = .{},
    version: u64 = 0,
    created: u64 = 0,
    lease: u64 = 0,
    flags: u8 = 0,
    key: []const u8 = &.{},
    val: []const u8 = &.{},

    /// Serializes the record; when `next` is given (and shorter), the value
    /// is stored as a patch against it. Allocates from `allocator`.
    pub fn bytes(kv_in: Kv, allocator: std.mem.Allocator, next: ?[]const u8) ![]u8 {
        var kv = kv_in;
        var buf = std.ArrayList(u8).empty;
        try util.appendUvarint(&buf, allocator, kv.key.len);
        try buf.appendSlice(allocator, kv.key);
        if (!kv.rev.isdel()) {
            kv.flags = 0;
            try util.appendUvarint(&buf, allocator, kv.created);
            try util.appendUvarint(&buf, allocator, kv.version);
            try util.appendUvarint(&buf, allocator, kv.lease);
            if (next != null and types.PATCH_ENABLED) {
                const p = try patch.generate(allocator, next.?, kv.val);
                if (p.len < kv.val.len) {
                    kv.val = p;
                    kv.flags |= types.KV_FLAG_PATCH;
                }
            }
            if (types.COMPRESSION_ENABLED and kv.val.len > 16) {
                const p = try snappy.encode(allocator, kv.val);
                if (p.len < kv.val.len) {
                    kv.val = p;
                    kv.flags |= types.KV_FLAG_COMPRESSED;
                }
            }
            try buf.append(allocator, kv.flags);
            try buf.appendSlice(allocator, kv.val);
        }
        var rev_key: [8]u8 = undefined;
        var crc_buf: [4]u8 = undefined;
        std.mem.writeInt(u32, &crc_buf, crc32.crc(kv.rev.key(&rev_key), buf.items), .big);
        try buf.appendSlice(allocator, &crc_buf);
        return buf.items;
    }

    /// Deserializes a record. `buf` must outlive the result (the key and any
    /// unpatched, uncompressed value are subslices of it); patched/compressed
    /// values are allocated from `allocator`.
    pub fn fromBytes(rev_key: []const u8, buf: []const u8, next: ?[]const u8, noval: bool, allocator: std.mem.Allocator) !Kv {
        var kv = Kv{};
        if (buf.len < 11) {
            return errors.Error.ChecksumMissing;
        }
        if (std.mem.readInt(u32, buf[buf.len - 4 ..][0..4], .big) != crc32.crc(rev_key, buf[0 .. buf.len - 4])) {
            return errors.Error.ChecksumInvalid;
        }
        kv.rev = .{ .v = std.mem.readInt(u64, rev_key[0..8], .big) };
        if (kv.rev.isdel()) {
            return kv;
        }
        const keylen = util.uvarint(buf) orelse return errors.Error.ValueInvalid;
        var pos: usize = keylen.n;
        kv.key = buf[pos .. pos + @as(usize, @intCast(keylen.v))];
        pos += @intCast(keylen.v);
        const rest = buf[pos .. buf.len - 4];
        var rpos: usize = 0;
        const created = util.uvarint(rest[rpos..]) orelse return errors.Error.ValueInvalid;
        kv.created = created.v;
        rpos += created.n;
        const version = util.uvarint(rest[rpos..]) orelse return errors.Error.ValueInvalid;
        kv.version = version.v;
        rpos += version.n;
        const lease_ = util.uvarint(rest[rpos..]) orelse return errors.Error.ValueInvalid;
        kv.lease = lease_.v;
        rpos += lease_.n;
        if (rpos >= rest.len) {
            return errors.Error.ValueInvalid;
        }
        kv.flags = rest[rpos];
        rpos += 1;
        if (!noval) {
            kv.val = rest[rpos..];
            if (kv.flags & types.KV_FLAG_COMPRESSED > 0) {
                kv.val = try snappy.decode(allocator, kv.val);
            }
            if (kv.flags & types.KV_FLAG_PATCH > 0) {
                if (next == null) {
                    return errors.Error.PatchInvalid;
                }
                kv.val = try patch.apply(allocator, next.?, kv.val);
            }
        }
        return kv;
    }

    pub fn toProto(kv: Kv) pb.KeyValue {
        return .{
            .create_revision = util.i64Of(kv.created),
            .mod_revision = util.i64Of(kv.rev.upper()),
            .version = util.i64Of(kv.version),
            .lease = util.i64Of(kv.lease),
            .key = kv.key,
            .value = kv.val,
        };
    }
};

pub const KvEvent = struct {
    epoch: u64 = 0,
    key: []const u8 = &.{},
    rev: Keyrev = .{},

    pub fn etype(evt: KvEvent) u8 {
        if (evt.rev.isdel()) {
            return @intCast(@intFromEnum(pb.Event.EventType.DELETE));
        }
        return @intCast(@intFromEnum(pb.Event.EventType.PUT));
    }
};

test "keyrev round trip" {
    const kr = Keyrev.init(1234, 7, false);
    try std.testing.expectEqual(@as(u64, 1234), kr.upper());
    try std.testing.expectEqual(@as(u64, 7), kr.lower());
    try std.testing.expect(!kr.isdel());
    const del = Keyrev.init(1234, 7, true);
    try std.testing.expect(del.isdel());
    try std.testing.expectEqual(kr.v, kr.invert().invert().v);
}
