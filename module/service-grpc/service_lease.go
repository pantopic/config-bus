package main

func leaseGrant(in []byte) (err error) {
	kvShard().AsyncApply(append(in, CMD_LEASE_GRANT), []byte("leaseGrant"))
	return
}

func leaseRevoke(in []byte) (err error) {
	kvShard().AsyncApply(append(in, CMD_LEASE_REVOKE), []byte("leaseRevoke"))
	return
}

func leaseKeepaliveOpen() (err error) {
	return
}

func leaseKeepaliveRecv(item []byte) (err error) {
	kvShard().AsyncApply(append(item, CMD_LEASE_KEEP_ALIVE), []byte(`leaseKeepaliveRecv`))
	return
}

func leaseKeepaliveClose() (err error) {
	return
}

func leaseLeases(in []byte) (err error) {
	kvShard().AsyncRead(append(in, QUERY_LEASE_LEASES), []byte(`leaseLeases`), false)
	return
}

func leaseTimeToLive(in []byte) (err error) {
	kvShard().AsyncRead(append(in, QUERY_LEASE_TIME_TO_LIVE), []byte(`leaseTimeToLive`), false)
	return
}
