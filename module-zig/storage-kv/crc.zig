//! Mirrors module/storage-kv/crc.go (IEEE CRC-32 over key then val)

const std = @import("std");

pub fn crc(key: []const u8, val: []const u8) u32 {
    var h = std.hash.crc.Crc32.init();
    h.update(key);
    h.update(val);
    return h.final();
}
