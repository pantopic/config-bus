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

func NewKvService(client zongzi.ShardClient) *kvService {
	return &kvService{client: client}
}

func (s *kvService) Put(ctx context.Context, req *internal.PutRequest) (res *internal.PutResponse, err error) {
	res = &internal.PutResponse{}
	b, err := proto.Marshal(req)
	if err != nil {
		return
	}
	val, data, err := s.client.Apply(ctx, append([]byte{CMD_KV_PUT}, b...))
	if err != nil {
		return
	}
	if val != 1 {
		err = fmt.Errorf("%s", string(data))
		return
	}
	err = proto.Unmarshal(data, res)
	return
}
