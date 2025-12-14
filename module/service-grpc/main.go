package main

import (
	"bytes"
	"errors"

	"github.com/pantopic/wazero-cluster/sdk-go"
	"github.com/pantopic/wazero-grpc-server/sdk-go"

	internal "github.com/pantopic/config-bus/module/service-grpc/internal"
)

var (
	rangeRequest = new(internal.RangeRequest)
)

func main() {
	s := grpc_server.NewService(`etcdserverpb.KV`)
	s.Unary(`Range`, grpcKvRange)
	s.Unary(`Put`, grpcKvPut)
	s.Unary(`DeleteRange`, grpcKvDeleteRange)
	s.Unary(`Txn`, grpcKvTxn)
	s.Unary(`Compact`, grpcKvCompact)
}

func kvShard() cluster.Shard {
	return cluster.ShardFind(`kv`)
}

func grpcKvRange(in []byte) (out []byte, err error) {
	err = rangeRequest.UnmarshalVT(in)
	if err != nil {
		return []byte(err.Error()), grpc_server.ErrMalformed
	}
	_, out, err = kvShard().Read(append(in, byte(internal.QUERY_KV_RANGE)), !rangeRequest.Serializable)
	return
}

func grpcKvPut(in []byte) (out []byte, err error) {
	val, out, err := kvShard().Apply(append(in, byte(internal.CMD_KV_PUT)))
	if val != 1 {
		if bytes.Equal(out, []byte(ErrGRPCLeaseProvided.Error())) {
			err = ErrGRPCLeaseProvided
		} else if bytes.Equal(out, []byte(ErrGRPCValueProvided.Error())) {
			err = ErrGRPCValueProvided
		} else {
			err = errors.New(string(out))
		}
		return
	}
	return
}

func grpcKvDeleteRange(in []byte) (out []byte, err error) {
	_, out, err = kvShard().Apply(append(in, byte(internal.CMD_KV_DELETE_RANGE)))
	return
}

func grpcKvTxn(in []byte) (out []byte, err error) {
	_, out, err = kvShard().Apply(append(in, byte(internal.CMD_KV_TXN)))
	return
}

func grpcKvCompact(in []byte) (out []byte, err error) {
	_, out, err = kvShard().Apply(append(in, byte(internal.CMD_KV_COMPACT)))
	return
}
