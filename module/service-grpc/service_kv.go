package main

import (
	"github.com/pantopic/wazero-grpc-server/sdk-go"
	"github.com/pantopic/wazero-grpc-server/sdk-go/codes"
	"github.com/pantopic/wazero-grpc-server/sdk-go/status"

	internal "github.com/pantopic/config-bus/module/service-grpc/internal"
)

var (
	rangeRequest = &internal.RangeRequest{}
	shardNameKv  = []byte(`kv`)
)

func serviceKvInit() {
	grpc_server.NewService(`etcdserverpb.KV`).
		Unary(`Range`, kvRange).
		Unary(`Put`, kvPut).
		Unary(`DeleteRange`, kvDeleteRange).
		Unary(`Txn`, kvTxn).
		Unary(`Compact`, kvCompact)
}

func kvRange(in []byte) (out []byte, err error) {
	err = rangeRequest.UnmarshalVT(in)
	if err != nil {
		return []byte(err.Error()), status.New(codes.InvalidArgument, err.Error()).Err()
	}
	return grpcError(kvShard().
		Read(append(in, QUERY_KV_RANGE), rangeRequest.Serializable))
}

func kvPut(in []byte) (out []byte, err error) {
	return grpcError(kvShard().
		Apply(append(in, CMD_KV_PUT)))
}

func kvDeleteRange(in []byte) (out []byte, err error) {
	return grpcError(kvShard().
		Apply(append(in, CMD_KV_DELETE_RANGE)))
}

func kvTxn(in []byte) (out []byte, err error) {
	return grpcError(kvShard().
		Apply(append(in, CMD_KV_TXN)))
}

func kvCompact(in []byte) (out []byte, err error) {
	return grpcError(kvShard().
		Apply(append(in, CMD_KV_COMPACT)))
}
