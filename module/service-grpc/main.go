package main

import (
	"github.com/pantopic/wazero-buffer-pool/sdk-go"
	"github.com/pantopic/wazero-grpc-server/sdk-go"
	"github.com/pantopic/wazero-shard-client/sdk-go"
)

const (
	BUFFER_POOL_WATCH_EVENT = iota
)

var (
	bufferPoolWatchEvent buffer_pool.MultiValueSet
)

func init() {
	shard_client.RegisterStreamRecv(shardRecv)
	shard_client.RegisterAsyncRecv(asyncRecv)
	bufferPoolWatchEvent = buffer_pool.NewMultiValueSet(BUFFER_POOL_WATCH_EVENT, buffer_pool.WithSizeLimit(PCB_RESPONSE_SIZE_MAX))
	grpc_server.Init(
		grpc_server.WithBufferCap(256, 1.5*1024*1024),
		grpc_server.WithHttpHandler(httpHandler),
	)
	grpc_server.NewService(`etcdserverpb.Cluster`).
		Unary(`MemberAdd`, clusterMemberAdd).
		Unary(`MemberRemove`, clusterMemberRemove).
		Unary(`MemberUpdate`, clusterMemberUpdate).
		Unary(`MemberList`, clusterMemberList).
		Unary(`MemberPromote`, clusterMemberPromote)
	grpc_server.NewService(`etcdserverpb.KV`).
		Unary(`Range`, kvRange).
		Unary(`Put`, kvPut).
		Unary(`DeleteRange`, kvDeleteRange).
		Unary(`Txn`, kvTxn).
		Unary(`Compact`, kvCompact)
	grpc_server.NewService(`etcdserverpb.Lease`).
		Unary(`LeaseGrant`, leaseGrant).
		Unary(`LeaseRevoke`, leaseRevoke).
		BidirectionalStream(`LeaseKeepAlive`, leaseKeepaliveOpen, leaseKeepaliveRecv, leaseKeepaliveClose).
		Unary(`LeaseLeases`, leaseLeases).
		Unary(`LeaseTimeToLive`, leaseTimeToLive)
	grpc_server.NewService(`etcdserverpb.Maintenance`).
		Unary(`Alarm`, maintenanceAlarm).
		Unary(`Status`, maintenanceStatus).
		Unary(`Defragment`, maintenanceDefragment).
		Unary(`Hash`, maintenanceHash).
		Unary(`HashKV`, maintenanceHashKV).
		ServerStream(`Snapshot`, maintenanceSnapshotOpen, maintenanceSnapshotClose).
		Unary(`MoveLeader`, maintenanceMoveLeader).
		Unary(`Downgrade`, maintenanceDowngrade)
	grpc_server.NewService(`etcdserverpb.Watch`).
		BidirectionalStream(`Watch`, watchOpen, watchRecv, watchClose)
}

func main() {
}

func asyncRecv(name, data []byte, val uint64, err error) {
	autoSend(grpcError(val, data, err))
}

func httpHandler(method, path, body []byte) (code int, res []byte) {
	switch string(path) {
	case "/metrics":
		res = append(res, []byte(`pantopic_power_level 9001`)...)
	case "/health":
		res = append(res, []byte(`{"health":"true","reason":""}`)...)
	case "/version":
		res = append(res, []byte(`{"etcdserver":"3.5.25","etcdcluster":"3.5.0"}`)...)
	default:
		code = 405
	}
	return
}
