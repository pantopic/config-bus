package icarus

import (
	"context"
	"io"

	"github.com/logbn/zongzi"
	"google.golang.org/grpc"

	"github.com/logbn/icarus/internal"
)

type serviceMaintenance struct {
	internal.UnimplementedMaintenanceServer

	client zongzi.ShardClient
}

func NewServiceMaintenance(client zongzi.ShardClient) *serviceMaintenance {
	return &serviceMaintenance{client: client}
}

func (svc *serviceMaintenance) Alarm(ctx context.Context,
	req *internal.AlarmRequest,
) (res *internal.AlarmResponse, err error) {
	return
}

func (svc *serviceMaintenance) Status(ctx context.Context,
	req *internal.StatusRequest,
) (res *internal.StatusResponse, err error) {
	return
}

func (svc *serviceMaintenance) Defragment(ctx context.Context,
	req *internal.DefragmentRequest,
) (res *internal.DefragmentResponse, err error) {
	return
}

func (svc *serviceMaintenance) Hash(ctx context.Context,
	req *internal.HashRequest,
) (res *internal.HashResponse, err error) {
	return
}

func (svc *serviceMaintenance) HashKV(ctx context.Context,
	req *internal.HashKVRequest,
) (res *internal.HashKVResponse, err error) {
	return
}

func (svc *serviceMaintenance) Snapshot(
	req *internal.SnapshotRequest,
	s internal.Maintenance_SnapshotServer,
) (err error) {
	return
}

func (svc *serviceMaintenance) MoveLeader(ctx context.Context,
	req *internal.MoveLeaderRequest,
) (res *internal.MoveLeaderResponse, err error) {
	return
}

func (svc *serviceMaintenance) Downgrade(ctx context.Context,
	req *internal.DowngradeRequest,
) (res *internal.DowngradeResponse, err error) {
	return
}

type maintenanceSnapshotServer struct {
	grpc.ServerStream
	ctx     context.Context
	reqChan chan *internal.SnapshotRequest
	resChan chan *internal.SnapshotResponse
}

func newMaintenanceSnapshotServer(ctx context.Context) *maintenanceSnapshotServer {
	return &maintenanceSnapshotServer{
		ctx:     ctx,
		reqChan: make(chan *internal.SnapshotRequest),
		resChan: make(chan *internal.SnapshotResponse),
	}
}

func (s *maintenanceSnapshotServer) Send(res *internal.SnapshotResponse) (err error) {
	s.resChan <- res
	return
}

func (s *maintenanceSnapshotServer) Recv() (req *internal.SnapshotRequest, err error) {
	select {
	case req = <-s.reqChan:
	case <-s.ctx.Done():
		err = io.EOF
	}
	return
}

func (s *maintenanceSnapshotServer) Context() context.Context {
	return s.ctx
}

var _ internal.Maintenance_SnapshotServer = new(maintenanceSnapshotServer)
