const std = @import("std");
const protobuf = @import("protobuf");

pub fn build(b: *std.Build) void {
    const target = b.resolveTargetQuery(.{
        .cpu_arch = .wasm32,
        .os_tag = .wasi,
    });
    const optimize = b.standardOptimizeOption(.{
        .preferred_optimize_mode = .ReleaseSmall,
    });
    const protobuf_dep = b.dependency("protobuf", .{});
    const lmdb_dep = b.dependency("lmdb_sdk_zig", .{});
    const atomic_dep = b.dependency("atomic_sdk_zig", .{});
    const global_dep = b.dependency("global_sdk_zig", .{});
    const range_watch_dep = b.dependency("range_watch_sdk_zig", .{});
    // SDKs without build.zig packaging are imported by source path,
    // matching wazero-state-machine's `-Mstatemachine=../sdk-zig/statemachine.zig`.
    const statemachine_mod = b.createModule(.{
        .root_source_file = b.path("../../../wazero-state-machine/sdk-zig/statemachine.zig"),
        .target = target,
        .optimize = optimize,
    });
    const small_cache_mod = b.createModule(.{
        .root_source_file = b.path("../../../wazero-small-cache/sdk-zig/src/lib.zig"),
        .target = target,
        .optimize = optimize,
    });
    const exe = b.addExecutable(.{
        .name = "storage-kv",
        .root_module = b.createModule(.{
            .root_source_file = b.path("module.zig"),
            .target = target,
            .optimize = optimize,
            .imports = &.{
                .{ .name = "lmdb", .module = lmdb_dep.module("lmdb") },
                .{ .name = "atomic", .module = atomic_dep.module("atomic") },
                .{ .name = "global", .module = global_dep.module("global") },
                .{ .name = "range_watch", .module = range_watch_dep.module("range_watch") },
                .{ .name = "statemachine", .module = statemachine_mod },
                .{ .name = "small_cache", .module = small_cache_mod },
                .{ .name = "protobuf", .module = protobuf_dep.module("protobuf") },
            },
        }),
    });
    exe.entry = .disabled;
    exe.rdynamic = true;
    exe.stack_size = 1024 * 1024;
    b.installArtifact(exe);

    const gen_proto = b.step("gen-proto", "generates zig files from protocol buffer definitions");
    const protoc_step = protobuf.RunProtocStep.create(protobuf_dep.builder, b.graph.host, .{
        .destination_directory = b.path("pb"),
        .source_files = &.{
            b.path("../../internal/api.proto"),
            b.path("../../internal/internal.proto"),
        },
        .include_directories = &.{
            b.path("../../internal"),
        },
    });
    gen_proto.dependOn(&protoc_step.step);
}
