package main

import (
	"github.com/pantopic/wazero-grpc-server/sdk-go"
	"github.com/pantopic/wazero-grpc-server/sdk-go/codes"
	"github.com/pantopic/wazero-grpc-server/sdk-go/status"

	internal "github.com/pantopic/config-bus/module/service-grpc/internal"
)

var (
	memberListResp = &internal.MemberListResponse{
		Header: &internal.ResponseHeader{},
		Members: []*internal.Member{
			&internal.Member{},
		},
	}
)

func clusterInit() {
	grpc_server.NewService(`etcdserverpb.Cluster`).
		Unary(`MemberAdd`, clusterMemberAdd).
		Unary(`MemberRemove`, clusterMemberRemove).
		Unary(`MemberUpdate`, clusterMemberUpdate).
		Unary(`MemberList`, clusterMemberList).
		Unary(`MemberPromote`, clusterMemberPromote)
}

func clusterMemberAdd(in []byte) (out []byte, err error) {
	return
}

func clusterMemberRemove(in []byte) (out []byte, err error) {
	return
}

func clusterMemberUpdate(in []byte) (out []byte, err error) {
	return
}

func clusterMemberList(in []byte) (out []byte, err error) {
	out, err = grpcError(kvShard().Read(append(in, QUERY_HEADER), false))
	if err != nil {
		return
	}
	err = memberListResp.Header.UnmarshalVT(out)
	if err != nil {
		return []byte(err.Error()), status.New(codes.Unknown, err.Error()).Err()
	}
	memberListResp.Members[0].ID = memberListResp.Header.MemberId
	return memberListResp.MarshalVT()
}

func clusterMemberPromote(in []byte) (out []byte, err error) {
	return
}
