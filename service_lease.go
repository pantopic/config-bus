package icarus

import (
	"context"
	"fmt"

	"github.com/logbn/zongzi"
	"google.golang.org/protobuf/proto"

	"github.com/logbn/icarus/internal"
)

type serviceLease struct {
	internal.UnimplementedLeaseServer

	client zongzi.ShardClient
}

func NewServiceLease(client zongzi.ShardClient) *serviceLease {
	return &serviceLease{client: client}
}

func (s *serviceLease) addTerm(header *internal.ResponseHeader) {
	_, term := s.client.Leader()
	header.RaftTerm = term
}

func (s *serviceLease) LeaseGrant(ctx context.Context, req *internal.LeaseGrantRequest) (res *internal.LeaseGrantResponse, err error) {
	b, err := proto.Marshal(req)
	if err != nil {
		return
	}
	val, data, err := s.client.Apply(ctx, append(b, CMD_LEASE_GRANT))
	if err != nil {
		return
	}
	if val != 1 {
		err = fmt.Errorf("%s", string(data))
		return
	}
	res = &internal.LeaseGrantResponse{}
	err = proto.Unmarshal(data, res)
	s.addTerm(res.Header)
	return
}

func (s *serviceLease) LeaseRevoke(ctx context.Context, req *internal.LeaseRevokeRequest) (res *internal.LeaseRevokeResponse, err error) {
	b, err := proto.Marshal(req)
	if err != nil {
		return
	}
	val, data, err := s.client.Apply(ctx, append(b, CMD_LEASE_REVOKE))
	if err != nil {
		return
	}
	if val != 1 {
		err = fmt.Errorf("%s", string(data))
		return
	}
	res = &internal.LeaseRevokeResponse{}
	err = proto.Unmarshal(data, res)
	s.addTerm(res.Header)
	return
}

/*
	func (s *serviceLease) LeaseKeepAlive(ctx context.Context, req *internal.LeaseKeepAliveRequest) (res *internal.LeaseKeepAliveResponse, err error) {
		b, err := proto.Marshal(req)
		if err != nil {
			return
		}
		val, data, err := s.client.Apply(ctx, append(b, CMD_LEASE_KEEP_ALIVE))
		if err != nil {
			return
		}
		if val != 1 {
			err = fmt.Errorf("%s", string(data))
			return
		}
		res = &internal.LeaseKeepAliveResponse{}
		err = proto.Unmarshal(data, res)
		s.addTerm(res.Header)
		return
	}
*/

func (s *serviceLease) LeaseLeases(ctx context.Context, req *internal.LeaseLeasesRequest) (res *internal.LeaseLeasesResponse, err error) {
	val, data, err := s.client.Read(ctx, []byte{QUERY_LEASE_LEASES}, true)
	if err != nil {
		return
	}
	if val != 1 {
		err = fmt.Errorf("%s", string(data))
		return
	}
	res = &internal.LeaseLeasesResponse{}
	err = proto.Unmarshal(data, res)
	s.addTerm(res.Header)
	return
}

func (s *serviceLease) LeaseTimeToLive(ctx context.Context, req *internal.LeaseTimeToLiveRequest) (res *internal.LeaseTimeToLiveResponse, err error) {
	b, err := proto.Marshal(req)
	if err != nil {
		return
	}
	val, data, err := s.client.Read(ctx, append(b, QUERY_LEASE_TIME_TO_LIVE), true)
	if err != nil {
		return
	}
	if val != 1 {
		err = fmt.Errorf("%s", string(data))
		return
	}
	res = &internal.LeaseTimeToLiveResponse{}
	err = proto.Unmarshal(data, res)
	s.addTerm(res.Header)
	return
}
