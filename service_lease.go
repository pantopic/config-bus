package krv

import (
	"context"
	"fmt"
	"io"
	"slices"
	// "log/slog"

	"github.com/kevburnsjr/batchy"
	"github.com/logbn/zongzi"
	"google.golang.org/grpc/codes"
	"google.golang.org/protobuf/proto"

	"github.com/pantopic/krv/internal"
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

// LeaseGrant creates a new lease
func (s *serviceLease) LeaseGrant(
	ctx context.Context,
	req *internal.LeaseGrantRequest,
) (res *internal.LeaseGrantResponse, err error) {
	b, err := proto.Marshal(req)
	if err != nil {
		return
	}
	// slog.Info(`Lease Grant Req`, `req`, req)
	val, data, err := s.client.Apply(ctx, append(b, CMD_LEASE_GRANT))
	if err != nil {
		return
	}
	// slog.Info(`Lease Grant`, `val`, val, `data`, data, `err`, err)
	if val != 1 {
		err = fmt.Errorf("%s", string(data))
		return
	}
	res = &internal.LeaseGrantResponse{}
	err = proto.Unmarshal(data, res)
	if len(res.Error) > 0 && res.Error == internal.ErrGRPCDuplicateKey.Error() {
		err = internal.ErrGRPCDuplicateKey
		res = nil
	} else {
		s.addTerm(res.Header)
	}
	return
}

// LeaseRevoke revokes a new lease, deleting all associated keys
func (s *serviceLease) LeaseRevoke(
	ctx context.Context,
	req *internal.LeaseRevokeRequest,
) (res *internal.LeaseRevokeResponse, err error) {
	b, err := proto.Marshal(req)
	if err != nil {
		return
	}
	val, data, err := s.client.Apply(ctx, append(b, CMD_LEASE_REVOKE))
	if err != nil {
		return
	}
	if val == uint64(codes.NotFound) {
		err = internal.ErrGRPCLeaseNotFound
		return
	}
	res = &internal.LeaseRevokeResponse{}
	err = proto.Unmarshal(data, res)
	s.addTerm(res.Header)
	return
}

// LeaseKeepAlive renews a lease to prevent it from expiring after TTL seconds
func (s *serviceLease) LeaseKeepAlive(
	server internal.Lease_LeaseKeepAliveServer,
) (err error) {
	var batcher batchy.Batcher
	if KRV_BATCH_LEASE_RENEWAL {
		batcher = batchy.New(
			LEASE_KEEPALIVE_BATCH_LIMIT,
			LEASE_KEEPALIVE_BATCH_INTERVAL,
			func(items []any) (errs []error) {
				errs = slices.Repeat([]error{nil}, len(items))
				req := &internal.LeaseKeepAliveBatchRequest{}
				for _, i := range items {
					req.IDs = append(req.IDs, i.(int64))
				}
				b, err := proto.Marshal(req)
				if err != nil {
					panic(err)
				}
				val, data, err := s.client.Apply(server.Context(), append(b, CMD_LEASE_KEEP_ALIVE_BATCH))
				if err != nil {
					return errRepeat(len(items), err)
				}
				if val != 1 {
					err = fmt.Errorf("%s", string(data))
					return errRepeat(len(items), err)
				}
				res := &internal.LeaseKeepAliveBatchResponse{}
				if err = proto.Unmarshal(data, res); err != nil {
					return errRepeat(len(items), err)
				}
				s.addTerm(res.Header)
				for i, id := range req.IDs {
					res := &internal.LeaseKeepAliveResponse{
						ID:  id,
						TTL: res.TTLs[i],
					}
					if err = server.Send(res); err != nil {
						errs[i] = err
					}
				}
				return
			})
		defer batcher.Stop()
	}
	for {
		req, err := server.Recv()
		if err == io.EOF {
			return nil
		}
		if err != nil {
			break
		}
		if batcher != nil {
			if err := batcher.Add(req.ID); err != nil {
				panic(err)
			}
		} else {
			b, err := proto.Marshal(req)
			if err != nil {
				return err
			}
			val, data, err := s.client.Apply(server.Context(), append(b, CMD_LEASE_KEEP_ALIVE))
			if err != nil {
				return err
			}
			if val != 1 {
				err = fmt.Errorf("%s", string(data))
				return err
			}
			res := &internal.LeaseKeepAliveResponse{}
			if err = proto.Unmarshal(data, res); err != nil {
				return err
			}
			s.addTerm(res.Header)
			if err = server.Send(res); err != nil {
				return err
			}
		}
	}
	return
}

// LeaseLeases reurns a list of all leases
func (s *serviceLease) LeaseLeases(
	ctx context.Context,
	req *internal.LeaseLeasesRequest,
) (res *internal.LeaseLeasesResponse, err error) {
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

func (s *serviceLease) LeaseTimeToLive(
	ctx context.Context,
	req *internal.LeaseTimeToLiveRequest,
) (res *internal.LeaseTimeToLiveResponse, err error) {
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
