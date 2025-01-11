package icarus

import (
	"context"
	"fmt"

	"github.com/logbn/zongzi"
	"google.golang.org/protobuf/proto"

	"github.com/logbn/icarus/internal"
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

func (s *kvService) Put(ctx context.Context, req *internal.PutRequest) (res *internal.PutResponse, err error) {
	b, err := proto.Marshal(req)
	if err != nil {
		return
	}
	val, data, err := s.client.Apply(ctx, append(b, CMD_KV_PUT))
	if err != nil {
		return
	}
	if val != 1 {
		err = fmt.Errorf("%s", string(data))
		return
	}
	res = &internal.PutResponse{}
	err = proto.Unmarshal(data, res)
	s.addTerm(res.Header)
	return
}

func (s *kvService) Range(ctx context.Context, req *internal.RangeRequest) (res *internal.RangeResponse, err error) {
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

func (s *kvService) DeleteRange(ctx context.Context, req *internal.DeleteRangeRequest) (res *internal.DeleteRangeResponse, err error) {
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

func (s *kvService) Compact(ctx context.Context, req *internal.CompactionRequest) (res *internal.CompactionResponse, err error) {
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

func (s *kvService) Txn(ctx context.Context, req *internal.TxnRequest) (res *internal.TxnResponse, err error) {
	b, err := proto.Marshal(req)
	if err != nil {
		return
	}
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
	return
}
