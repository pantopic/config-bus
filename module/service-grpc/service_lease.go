package main

func leaseGrant(in []byte) (err error) {
	return autoSend(grpcError(kvShard().Apply(append(in, CMD_LEASE_GRANT))))
}

func leaseRevoke(in []byte) (err error) {
	if PCB_LEASE_PARTITIONS() == 0 {
		return autoSend(grpcError(kvShard().Apply(append(in, CMD_LEASE_REVOKE))))
	}
	// TODO - Add support for multiple lease shards. Route to correct partition.
	if out, err := grpcError(leaseShard().Apply(append(in, CMD_LEASE_LOCK))); err != nil {
		return autoSend(out, err)
	}
	if out, err := grpcError(kvShard().Apply(append(in, CMD_LEASE_REVOKE))); err != nil {
		leaseShard().Apply(append(in, CMD_LEASE_UNLOCK))
		return autoSend(out, err)
	}
	return autoSend(grpcError(leaseShard().Apply(append(in, CMD_LEASE_REVOKE))))
}

func leaseKeepaliveOpen() (err error) {
	return
}

func leaseKeepaliveRecv(item []byte) (err error) {
	if PCB_LEASE_PARTITIONS() == 0 {
		return autoSend(grpcError(kvShard().Apply(append(item, CMD_LEASE_KEEP_ALIVE))))
	}
	// TODO - Add support for multiple lease shards. Route to correct partition.
	return autoSend(grpcError(leaseShard().Apply(append(item, CMD_LEASE_KEEP_ALIVE))))
}

func leaseKeepaliveClose() (err error) {
	return
}

func leaseLeases(in []byte) (err error) {
	if PCB_LEASE_PARTITIONS() == 0 {
		return autoSend(grpcError(kvShard().Read(append(in, QUERY_LEASE_LEASES), true)))
	}
	return autoSend(grpcError(leaseShard().Read(append(in, QUERY_LEASE_LEASES), true)))
}

func leaseTimeToLive(in []byte) (err error) {
	if PCB_LEASE_PARTITIONS() == 0 {
		return autoSend(grpcError(kvShard().Read(append(in, QUERY_LEASE_TIME_TO_LIVE), true)))
	}
	return autoSend(grpcError(leaseShard().Read(append(in, QUERY_LEASE_TIME_TO_LIVE), true)))
}
