package main

import (
	"github.com/pantopic/wazero-grpc-server/sdk-go/codes"
	"github.com/pantopic/wazero-grpc-server/sdk-go/status"
	"github.com/pantopic/wazero-shard-client/sdk-go"
)

func grpcError(val uint64, out []byte, err error) ([]byte, error) {
	if err == nil && val != 1 {
		if grpcErr, ok := errStringToError[string(out)]; ok {
			err = grpcErr
		} else {
			err = status.New(codes.Unknown, string(out)).Err()
		}
	}
	return out, err
}

func kvShard() shard_client.Client {
	return shard_client.New(shardNameKv)
}
