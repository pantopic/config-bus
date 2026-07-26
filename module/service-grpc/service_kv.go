package main

import (
	"github.com/pantopic/wazero-grpc-server/sdk-go"
	"github.com/pantopic/wazero-grpc-server/sdk-go/codes"

	internal "github.com/pantopic/turbokube/module/service-grpc/internal"
)

var (
	rangeRequest      = &internal.RangeRequest{}
	putRequest        = &internal.PutRequest{}
	leaseCheckRequest = &internal.LeaseCheckBatchRequest{}
	txnRequest        = &internal.TxnRequest{}
)

func kvRange(in []byte) (err error) {
	err = rangeRequest.UnmarshalVT(in)
	if err != nil {
		grpc_server.SendErr(codes.InvalidArgument, []byte(err.Error()))
		return
	}
	return autoSend(grpcError(kvShard().Read(append(in, QUERY_KV_RANGE), rangeRequest.Serializable)))
}

func kvPut(in []byte) (err error) {
	if PCB_LEASE_PARTITIONS() > 0 {
		err = putRequest.UnmarshalVT(in)
		if err != nil {
			grpc_server.SendErr(codes.InvalidArgument, []byte(err.Error()))
			return
		}
		if putRequest.Lease > 0 {
			leaseCheckRequest.IDs = append(leaseCheckRequest.IDs[:0], putRequest.Lease)
			q, _ := leaseCheckRequest.MarshalVT()
			if out, err := grpcError(leaseShard().Read(append(q, QUERY_LEASE_CHECK_BATCH), true)); err != nil {
				return autoSend(out, err)
			}
		}
	}
	return autoSend(grpcError(kvShard().Apply(append(in, CMD_KV_PUT))))
}

func kvDeleteRange(in []byte) (err error) {
	return autoSend(grpcError(kvShard().Apply(append(in, CMD_KV_DELETE_RANGE))))
}

func kvTxn(in []byte) (err error) {
	return autoSend(grpcError(kvShard().Apply(append(in, CMD_KV_TXN))))
}

func kvCompact(in []byte) (err error) {
	return autoSend(grpcError(kvShard().Apply(append(in, CMD_KV_COMPACT))))
}
