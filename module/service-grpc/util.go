package main

import (
	"github.com/pantopic/wazero-grpc-server/sdk-go"
	"github.com/pantopic/wazero-grpc-server/sdk-go/codes"
	"github.com/pantopic/wazero-grpc-server/sdk-go/status"
	"github.com/pantopic/wazero-shard-client/sdk-go"
)

var (
	componentName  = []byte(`default`)
	shardNameKv    = []byte(`kv`)
	shardNameLease = []byte(`lease`)
)

func autoSend(out []byte, err error) error {
	if err == nil {
		grpc_server.Send(out)
	} else if grpcErr, ok := err.(*status.Error); ok {
		grpc_server.SendErr(grpcErr.Code(), []byte(grpcErr.Message()))
	} else {
		panic(err.Error())
	}
	return err
}

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
	return shard_client.New(componentName, shardNameKv)
}
func leaseShard() shard_client.Client {
	return shard_client.New(componentName, shardNameLease)
}
