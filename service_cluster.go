package krv

import (
	"context"

	"github.com/logbn/zongzi"

	"github.com/pantopic/krv/internal"
)

type serviceCluster struct {
	internal.UnimplementedClusterServer

	client  zongzi.ShardClient
	apiAddr string
}

func NewServiceCluster(client zongzi.ShardClient, apiAddr string) *serviceCluster {
	return &serviceCluster{client: client, apiAddr: apiAddr}
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
	leader, term := s.client.Leader()
	res = &internal.MemberListResponse{
		Header: &internal.ResponseHeader{
			RaftTerm: term,
		},
		Members: []*internal.Member{
			{
				ID: leader,
			},
		},
	}

	// type Member struct {
	// 	state         protoimpl.MessageState `protogen:"open.v1"`
	// 	ID            uint64                 `protobuf:"varint,1,opt,name=ID,proto3" json:"ID,omitempty"`
	// 	Name          string                 `protobuf:"bytes,2,opt,name=name,proto3" json:"name,omitempty"`
	// 	PeerURLs      []string               `protobuf:"bytes,3,rep,name=peerURLs,proto3" json:"peerURLs,omitempty"`
	// 	ClientURLs    []string               `protobuf:"bytes,4,rep,name=clientURLs,proto3" json:"clientURLs,omitempty"`
	// 	IsLearner     bool                   `protobuf:"varint,5,opt,name=isLearner,proto3" json:"isLearner,omitempty"`
	// 	unknownFields protoimpl.UnknownFields
	// 	sizeCache     protoimpl.SizeCache
	// }
	// TODO - Collect member list
	return
}

func (s serviceCluster) MemberPromote(ctx context.Context,
	req *internal.MemberPromoteRequest,
) (res *internal.MemberPromoteResponse, err error) {
	return
}
