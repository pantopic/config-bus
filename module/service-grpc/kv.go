package main

import (
	"github.com/pantopic/wazero-grpc-server/sdk-go/codes"
	"github.com/pantopic/wazero-grpc-server/sdk-go/status"
	"github.com/pantopic/wazero-shard-client/sdk-go"

	internal "github.com/pantopic/config-bus/module/service-grpc/internal"
)

var (
	rangeRequest = new(internal.RangeRequest)
)

func kvShard() shard_client.Client {
	return shard_client.New(`kv`)
}

func kvRange(in []byte) (out []byte, err error) {
	err = rangeRequest.UnmarshalVT(in)
	if err != nil {
		return []byte(err.Error()), status.New(codes.InvalidArgument, err.Error()).Err()
	}
	_, out, err = kvShard().Read(append(in, QUERY_KV_RANGE), !rangeRequest.Serializable)
	return
}

func kvPut(in []byte) (out []byte, err error) {
	var val uint64
	val, out, err = kvShard().Apply(append(in, CMD_KV_PUT))
	if val != 1 {
		if grpcErr, ok := errStringToError[string(out)]; ok {
			err = grpcErr
		} else {
			err = status.New(codes.Unknown, string(out)).Err()
		}
	}
	return
}

func kvDeleteRange(in []byte) (out []byte, err error) {
	_, out, err = kvShard().Apply(append(in, CMD_KV_DELETE_RANGE))
	return
}

func kvTxn(in []byte) (out []byte, err error) {
	_, out, err = kvShard().Apply(append(in, CMD_KV_TXN))
	return
}

func kvCompact(in []byte) (out []byte, err error) {
	_, out, err = kvShard().Apply(append(in, CMD_KV_COMPACT))
	return
}
