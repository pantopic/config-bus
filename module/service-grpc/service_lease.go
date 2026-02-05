package main

import (
	"iter"

	"github.com/pantopic/wazero-grpc-server/sdk-go"
)

func serviceLeaseInit() {
	grpc_server.NewService(`etcdserverpb.Lease`).
		Unary(`LeaseGrant`, leaseGrant).
		Unary(`LeaseRevoke`, leaseRevoke).
		BidirectionalStream(`LeaseKeepAlive`, leaseKeepaliveRecv, leaseKeepaliveSend).
		Unary(`LeaseLeases`, leaseLeases).
		Unary(`LeaseTimeToLive`, leaseTimeToLive)
}

func leaseGrant(in []byte) (out []byte, err error) {
	return grpcError(kvShard().
		Apply(append(in, CMD_LEASE_GRANT)))
}

func leaseRevoke(in []byte) (out []byte, err error) {
	return grpcError(kvShard().
		Apply(append(in, CMD_LEASE_REVOKE)))
}

func leaseKeepaliveRecv(in iter.Seq[[]byte]) (err error) {
	var out []byte
	for item := range in {
		_, out, err = kvShard().
			Apply(append(item, CMD_LEASE_KEEP_ALIVE))
		if err != nil {
			break
		}
		pipeLease.Send(out)
	}
	return
}

func leaseKeepaliveSend() (out iter.Seq[[]byte], err error) {
	var res []byte
	out = func(yield func([]byte) bool) {
		for {
			res, err = pipeLease.Recv()
			if err != nil {
				break
			}
			if !yield(res) {
				return
			}
		}
	}
	return
}

func leaseLeases(in []byte) (out []byte, err error) {
	return grpcError(kvShard().
		Read(append(in, QUERY_LEASE_LEASES), true))
}

func leaseTimeToLive(in []byte) (out []byte, err error) {
	return grpcError(kvShard().
		Read(append(in, QUERY_LEASE_TIME_TO_LIVE), true))
}
