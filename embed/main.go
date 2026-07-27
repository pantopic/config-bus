package turbokube

import (
	_ "embed"
)

const (
	ServiceGrpcName = "pantopic/turbokube/service/grpc"
	StorageKvName   = "pantopic/turbokube/storage/kv"
	Version         = 0
)

//go:embed service\-grpc\.wasm
var ServiceGrpcWasm []byte

//go:embed service\-grpc\.dev\.wasm
var ServiceGrpcDevWasm []byte

//go:embed storage\-kv\.wasm
var StorageKvWasm []byte

//go:embed storage\-kv\.dev\.wasm
var StorageKvDevWasm []byte

//go:embed service\-grpc\.zig\.wasm
var ServiceGrpcZigWasm []byte

//go:embed service\-grpc\.zig\.dev\.wasm
var ServiceGrpcZigDevWasm []byte

//go:embed storage\-kv\.zig\.wasm
var StorageKvZigWasm []byte

//go:embed storage\-kv\.zig\.dev\.wasm
var StorageKvZigDevWasm []byte
