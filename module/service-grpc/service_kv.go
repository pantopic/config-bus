package main

import (
	"github.com/pantopic/ext-grpc-server/sdk-go"
	"github.com/pantopic/ext-grpc-server/sdk-go/codes"

	internal "github.com/pantopic/turbokube/module/service-grpc/internal"
)

var (
	rangeRequest = &internal.RangeRequest{}
)

func kvRange(in []byte) (err error) {
	err = rangeRequest.UnmarshalVT(in)
	if err != nil {
		grpc_server.SendErr(codes.InvalidArgument, []byte(err.Error()))
		return
	}
	kvShard().AsyncRead(append(in, QUERY_KV_RANGE), []byte("kvRange"), rangeRequest.Serializable)
	return
}

func kvPut(in []byte) (err error) {
	kvShard().AsyncApply(append(in, CMD_KV_PUT), []byte("kvPut"))
	return
}

func kvDeleteRange(in []byte) (err error) {
	kvShard().AsyncApply(append(in, CMD_KV_DELETE_RANGE), []byte("kvDeleteRange"))
	return
}

func kvTxn(in []byte) (err error) {
	kvShard().AsyncApply(append(in, CMD_KV_TXN), []byte("kvTxn"))
	return
}

func kvCompact(in []byte) (err error) {
	kvShard().AsyncApply(append(in, CMD_KV_COMPACT), []byte("kvCompact"))
	return
}
