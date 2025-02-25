package icarus

import (
	"context"

	"github.com/logbn/zongzi"

	"github.com/logbn/icarus/internal"
)

type serviceCluster struct {
	internal.UnimplementedClusterServer

	client zongzi.ShardClient
}

func NewServiceCluster(client zongzi.ShardClient) *serviceCluster {
	return &serviceCluster{client: client}
}

func (s *serviceCluster) addTerm(header *internal.ResponseHeader) {
	_, term := s.client.Leader()
	header.RaftTerm = term
}

func (s serviceCluster) MemberAdd(ctx context.Context,
	req *internal.MemberAddRequest,
) (res *internal.MemberAddResponse, err error) {
	return
}
func (s serviceCluster) MemberRemove(ctx context.Context,
	req *internal.MemberRemoveRequest,
) (res *internal.MemberRemoveResponse, err error) {
	return
}

func (s serviceCluster) MemberUpdate(ctx context.Context,
	req *internal.MemberUpdateRequest,
) (res *internal.MemberUpdateResponse, err error) {
	return
}

func (s serviceCluster) MemberList(ctx context.Context,
	req *internal.MemberListRequest,
) (res *internal.MemberListResponse, err error) {
	res = &internal.MemberListResponse{Header: &internal.ResponseHeader{}}
	s.addTerm(res.Header)
	// TODO - Collect member list
	return
}

func (s serviceCluster) MemberPromote(ctx context.Context,
	req *internal.MemberPromoteRequest,
) (res *internal.MemberPromoteResponse, err error) {
	return
}
