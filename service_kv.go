package krv

import (
	"bytes"
	"context"
	"fmt"
	"log/slog"

	"github.com/logbn/zongzi"
	"google.golang.org/protobuf/proto"

	"github.com/pantopic/krv/internal"
)

type kvService struct {
	internal.UnimplementedKVServer

	client zongzi.ShardClient
}

func NewServiceKv(client zongzi.ShardClient) *kvService {
	return &kvService{client: client}
}

func (s *kvService) addTerm(header *internal.ResponseHeader) {
	_, term := s.client.Leader()
	header.RaftTerm = term
}

// Put a key to the KV store
func (s *kvService) Put(
	ctx context.Context,
	req *internal.PutRequest,
) (res *internal.PutResponse, err error) {
	b, err := proto.Marshal(req)
	if err != nil {
		return
	}
	// slog.Info(`KV Put Req`, `req`, req)
	val, data, err := s.client.Apply(ctx, append(b, CMD_KV_PUT))
	if err != nil {
		return
	}
	if val != 1 {
		if bytes.Equal(data, []byte(internal.ErrGRPCLeaseProvided.Error())) {
			err = internal.ErrGRPCLeaseProvided
		} else if bytes.Equal(data, []byte(internal.ErrGRPCValueProvided.Error())) {
			err = internal.ErrGRPCValueProvided
		} else {
			err = fmt.Errorf("%s", string(data))
		}
		return
	}
	res = &internal.PutResponse{}
	err = proto.Unmarshal(data, res)
	s.addTerm(res.Header)
	return
}

// Range over keys in the KV store
func (s *kvService) Range(
	ctx context.Context,
	req *internal.RangeRequest,
) (res *internal.RangeResponse, err error) {
	b, err := proto.Marshal(req)
	if err != nil {
		return
	}
	val, data, err := s.client.Read(ctx, append(b, QUERY_KV_RANGE), req.Serializable)
	if err != nil {
		return
	}
	if val != 1 {
		err = fmt.Errorf("%s", string(data))
		return
	}
	res = &internal.RangeResponse{}
	err = proto.Unmarshal(data, res)
	s.addTerm(res.Header)
	return
}

// DeleteRange of keys in the KV store
func (s *kvService) DeleteRange(
	ctx context.Context,
	req *internal.DeleteRangeRequest,
) (res *internal.DeleteRangeResponse, err error) {
	b, err := proto.Marshal(req)
	if err != nil {
		return
	}
	val, data, err := s.client.Apply(ctx, append(b, CMD_KV_DELETE_RANGE))
	if err != nil {
		return
	}
	if val != 1 {
		err = fmt.Errorf("%s", string(data))
		return
	}
	res = &internal.DeleteRangeResponse{}
	err = proto.Unmarshal(data, res)
	s.addTerm(res.Header)
	return
}

// Compact old revisions of keys in the KV store
func (s *kvService) Compact(
	ctx context.Context,
	req *internal.CompactionRequest,
) (res *internal.CompactionResponse, err error) {
	slog.Info(`KV - Compact`)
	b, err := proto.Marshal(req)
	if err != nil {
		return
	}
	val, data, err := s.client.Apply(ctx, append(b, CMD_KV_COMPACT))
	if err != nil {
		return
	}
	if val != 1 {
		err = fmt.Errorf("%s", string(data))
		return
	}
	res = &internal.CompactionResponse{}
	err = proto.Unmarshal(data, res)
	s.addTerm(res.Header)
	return
}

// Txn over keys in the KV store
func (s *kvService) Txn(
	ctx context.Context,
	req *internal.TxnRequest,
) (res *internal.TxnResponse, err error) {
	b, err := proto.Marshal(req)
	if err != nil {
		return
	}
	// slog.Info(`KV Txn Req`, `req`, req)
	val, data, err := s.client.Apply(ctx, append(b, CMD_KV_TXN))
	if err != nil {
		return
	}
	if val != 1 {
		err = fmt.Errorf("%s", string(data))
		return
	}
	res = &internal.TxnResponse{}
	err = proto.Unmarshal(data, res)
	s.addTerm(res.Header)
	// slog.Info(`KV Txn Res`, `res`, res)
	return
}
