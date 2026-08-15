package main

func leaseGrant(in []byte) (err error) {
	return autoSend(grpcError(kvShard().Apply(append(in, CMD_LEASE_GRANT))))
}

func leaseRevoke(in []byte) (err error) {
	return autoSend(grpcError(kvShard().Apply(append(in, CMD_LEASE_REVOKE))))
}

func leaseKeepaliveOpen() (err error) {
	return
}

func leaseKeepaliveRecv(item []byte) (err error) {
	return autoSend(grpcError(kvShard().Apply(append(item, CMD_LEASE_KEEP_ALIVE))))
}

func leaseKeepaliveClose() (err error) {
	return
}

func leaseLeases(in []byte) (err error) {
	return autoSend(grpcError(kvShard().Read(append(in, QUERY_LEASE_LEASES), false)))
}

func leaseTimeToLive(in []byte) (err error) {
	return autoSend(grpcError(kvShard().Read(append(in, QUERY_LEASE_TIME_TO_LIVE), false)))
}
