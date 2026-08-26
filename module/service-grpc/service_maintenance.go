package main

import (
	"github.com/pantopic/ext-grpc-server/sdk-go"
	"github.com/pantopic/ext-grpc-server/sdk-go/codes"
	"github.com/pantopic/ext-grpc-server/sdk-go/status"

	internal "github.com/pantopic/turbokube/module/service-grpc/internal"
)

var (
	statusResp = &internal.StatusResponse{
		Version:     "3.6.5",
		DbSize:      28672,
		DbSizeInUse: 28672,
		IsLearner:   false,
		Header:      &internal.ResponseHeader{},
	}
	alarmResp = &internal.AlarmResponse{
		Header: &internal.ResponseHeader{},
	}
)

func maintenanceAlarm(in []byte) (err error) {
	// out, err := alarmResp.MarshalVT()
	// if err != nil {
	// 	return
	// }
	// return grpc_server.Send(out)
	return grpc_server.Send(nil)
}

func maintenanceStatus(in []byte) (err error) {
	out, err := grpcError(kvShard().Read(append(in, QUERY_HEADER), false))
	if err != nil {
		return
	}
	err = statusResp.Header.UnmarshalVT(out)
	if err != nil {
		return status.New(codes.Unknown, err.Error()).Err()
	}
	statusResp.Leader = statusResp.Header.MemberId
	statusResp.RaftIndex = uint64(statusResp.Header.Revision)
	statusResp.RaftTerm = 1
	statusResp.RaftAppliedIndex = uint64(statusResp.Header.Revision)
	out, err = statusResp.MarshalVT()
	if err != nil {
		return
	}
	return grpc_server.Send(out)
}

func maintenanceDefragment(in []byte) (err error) {
	return grpc_server.Send(nil)
}

func maintenanceHash(in []byte) (err error) {
	return grpc_server.Send(nil)
}

func maintenanceHashKV(in []byte) (err error) {
	return grpc_server.Send(nil)
}

func maintenanceSnapshotOpen(in []byte) (err error) {
	return grpc_server.Send(nil)
}

func maintenanceSnapshotClose() (err error) {
	return grpc_server.Send(nil)
}

func maintenanceMoveLeader(in []byte) (err error) {
	return grpc_server.Send(nil)
}

func maintenanceDowngrade(in []byte) (err error) {
	return grpc_server.Send(nil)
}
