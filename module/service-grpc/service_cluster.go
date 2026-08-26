package main

import (
	"github.com/pantopic/ext-grpc-server/sdk-go"
	"github.com/pantopic/ext-grpc-server/sdk-go/codes"
	"github.com/pantopic/ext-grpc-server/sdk-go/status"

	internal "github.com/pantopic/turbokube/module/service-grpc/internal"
)

var (
	memberListResp = &internal.MemberListResponse{
		Header:  &internal.ResponseHeader{},
		Members: []*internal.Member{{}},
	}
)

func clusterMemberAdd(in []byte) (err error) {
	return grpc_server.Send(nil)
}

func clusterMemberRemove(in []byte) (err error) {
	return grpc_server.Send(nil)
}

func clusterMemberUpdate(in []byte) (err error) {
	return grpc_server.Send(nil)
}

func clusterMemberList(in []byte) (err error) {
	out, err := grpcError(kvShard().Read(append(in, QUERY_HEADER), false))
	if err != nil {
		return
	}
	err = memberListResp.Header.UnmarshalVT(out)
	if err != nil {
		return status.New(codes.Unknown, err.Error()).Err()
	}
	memberListResp.Members[0].ID = memberListResp.Header.MemberId
	out, err = memberListResp.MarshalVT()
	if err != nil {
		return
	}
	grpc_server.Send(out)
	return
}

func clusterMemberPromote(in []byte) (err error) {
	return grpc_server.Send(nil)
}
