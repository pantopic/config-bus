package main

import (
	"github.com/pantopic/wazero-grpc-server/sdk-go"
	"github.com/pantopic/wazero-pipe/sdk-go"
)

var leaseBridge *pipe.Pipe[[]byte]

func main() {
	leaseBridge = pipe.New[[]byte]()
	grpc_server.NewService(`etcdserverpb.KV`).
		Unary(`Range`, kvRange).
		Unary(`Put`, kvPut).
		Unary(`DeleteRange`, kvDeleteRange).
		Unary(`Txn`, kvTxn).
		Unary(`Compact`, kvCompact)
	grpc_server.NewService(`etcdserverpb.Lease`).
		Unary(`LeaseGrant`, leaseGrant).
		Unary(`LeaseRevoke`, leaseRevoke).
		BidirectionalStream(`LeaseKeepAlive`, leaseKeepaliveRecv, leaseKeepaliveSend).
		Unary(`LeaseLeases`, leaseLeases).
		Unary(`LeaseTimeToLive`, leaseTimeToLive)
}
