package main

import (
	"iter"

	"github.com/pantopic/wazero-grpc-server/sdk-go"
	"github.com/pantopic/wazero-grpc-server/sdk-go/codes"
	"github.com/pantopic/wazero-grpc-server/sdk-go/status"

	internal "github.com/pantopic/config-bus/module/service-grpc/internal"
)

var (
	statusResp = &internal.StatusResponse{
		Version:     "3.6.5",
		DbSize:      28672,
		DbSizeInUse: 28672,
		IsLearner:   false,
		Header:      &internal.ResponseHeader{},
	}
)

func serviceMaintenanceInit() {
	grpc_server.NewService(`etcdserverpb.Maintenance`).
		Unary(`Alarm`, maintenanceAlarm).
		Unary(`Status`, maintenanceStatus).
		Unary(`Defragment`, maintenanceDefragment).
		Unary(`Hash`, maintenanceHash).
		Unary(`HashKV`, maintenanceHashKV).
		ServerStream(`Snapshot`, maintenanceSnapshot).
		Unary(`MoveLeader`, maintenanceMoveLeader).
		Unary(`Downgrade`, maintenanceDowngrade)
}

func maintenanceAlarm(in []byte) (out []byte, err error) {
	return
}

func maintenanceStatus(in []byte) (out []byte, err error) {
	out, err = grpcError(kvShard().Read(append(in, QUERY_HEADER), false))
	if err != nil {
		return
	}
	err = statusResp.Header.UnmarshalVT(out)
	if err != nil {
		return []byte(err.Error()), status.New(codes.Unknown, err.Error()).Err()
	}
	statusResp.Leader = statusResp.Header.MemberId
	statusResp.RaftIndex = uint64(statusResp.Header.Revision)
	statusResp.RaftTerm = 1
	statusResp.RaftAppliedIndex = uint64(statusResp.Header.Revision)
	return statusResp.MarshalVT()
}

func maintenanceDefragment(in []byte) (out []byte, err error) {
	return
}

func maintenanceHash(in []byte) (out []byte, err error) {
	return
}

func maintenanceHashKV(in []byte) (out []byte, err error) {
	return
}

func maintenanceSnapshot(in []byte) (out iter.Seq[[]byte], err error) {
	out = func(yield func([]byte) bool) {}
	return
}

func maintenanceMoveLeader(in []byte) (out []byte, err error) {
	return
}

func maintenanceDowngrade(in []byte) (out []byte, err error) {
	return
}
