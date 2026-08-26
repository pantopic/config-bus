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
    const grpc_dep = b.dependency("grpc_sdk_zig", .{});
    const buffer_dep = b.dependency("buffer_sdk_zig", .{});
    const shard_client_mod = b.createModule(.{
        .root_source_file = b.path("../../../wazero-shard-client/sdk-zig/src/root.zig"),
        .target = target,
        .optimize = optimize,
    });
    const exe = b.addExecutable(.{
        .name = "service-grpc",
        .root_module = b.createModule(.{
            .root_source_file = b.path("module.zig"),
            .target = target,
            .optimize = optimize,
            .imports = &.{
                .{ .name = "grpc_server", .module = grpc_dep.module("grpc_server") },
                .{ .name = "buffer", .module = buffer_dep.module("buffer") },
                .{ .name = "shard_client", .module = shard_client_mod },
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
