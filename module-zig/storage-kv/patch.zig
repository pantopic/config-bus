//! Mirrors module/storage-kv/patch (generate.go + apply.go)

const std = @import("std");
const lcs = @import("lcs.zig");
const util = @import("util.zig");

pub const Error = error{CorruptPatch};

/// Generate returns a patch representing the difference between a and b.
pub fn generate(allocator: std.mem.Allocator, a: []const u8, b: []const u8) ![]u8 {
    var diff_buf: [lcs.max_out_diffs]lcs.Diff = undefined;
    const diffs = lcs.diffBytes(a, b, &diff_buf);
    var patch = std.ArrayList(u8).empty;
    try util.appendUvarint(&patch, allocator, b.len);
    try util.appendUvarint(&patch, allocator, diffs.len);
    for (diffs) |diff| {
        try util.appendUvarint(&patch, allocator, diff.start);
        try util.appendUvarint(&patch, allocator, diff.end);
        try util.appendUvarint(&patch, allocator, diff.repl_end - diff.repl_start);
        try patch.appendSlice(allocator, b[diff.repl_start..diff.repl_end]);
    }
    return patch.items;
}

/// Apply applies a patch to a to recreate b.
pub fn apply(allocator: std.mem.Allocator, a: []const u8, patch: []const u8) ![]u8 {
    var pos: usize = 0;
    const size = readUvarint(patch, &pos) orelse return Error.CorruptPatch;
    const count = readUvarint(patch, &pos) orelse return Error.CorruptPatch;
    var b = std.ArrayList(u8).empty;
    var prev: usize = 0;
    for (0..count) |_| {
        const start = readUvarint(patch, &pos) orelse return Error.CorruptPatch;
        if (start > a.len) return Error.CorruptPatch;
        const end = readUvarint(patch, &pos) orelse return Error.CorruptPatch;
        if (end > a.len) return Error.CorruptPatch;
        const diff = readUvarint(patch, &pos) orelse return Error.CorruptPatch;
        if (prev > start) return Error.CorruptPatch;
        try b.appendSlice(allocator, a[prev..start]);
        const take = @min(diff, patch.len - pos);
        try b.appendSlice(allocator, patch[pos .. pos + take]);
        pos += take;
        prev = end;
    }
    if (prev > a.len) return Error.CorruptPatch;
    try b.appendSlice(allocator, a[prev..]);
    if (size != b.items.len) return Error.CorruptPatch;
    return b.items;
}

fn readUvarint(buf: []const u8, pos: *usize) ?usize {
    const r = util.uvarint(buf[pos.*..]) orelse return null;
    pos.* += r.n;
    return @intCast(r.v);
}

test "patch round trip" {
    const allocator = std.testing.allocator;
    const cases = [_][2][]const u8{
        .{ "abcdefg", "abcdefg" },
        .{ "abcdefg", "abXdefg" },
        .{ "", "abc" },
        .{ "abc", "" },
        .{ "aaabbbccc", "aaaccc" },
        .{ "the quick brown fox", "the quick brown foxes jumped" },
    };
    for (cases) |c| {
        var arena = std.heap.ArenaAllocator.init(allocator);
        defer arena.deinit();
        const p = try generate(arena.allocator(), c[0], c[1]);
        const b = try apply(arena.allocator(), c[0], p);
        try std.testing.expectEqualSlices(u8, c[1], b);
    }
}
