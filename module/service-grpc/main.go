package main

import (
	"github.com/pantopic/wazero-pipe/sdk-go"
)

const (
	PIPE_ID_LEASE = iota
	PIPE_ID_WATCH
)

const (
	SET_ID_WATCH = iota
)

var (
	pipeLease *pipe.Pipe[[]byte]
	pipeWatch *pipe.Pipe[[]byte]
)

func main() {
	pipeLease = pipe.New[[]byte](pipe.WithID(PIPE_ID_LEASE))
	pipeWatch = pipe.New[[]byte](pipe.WithID(PIPE_ID_WATCH))
	serviceClusterInit()
	serviceKvInit()
	serviceLeaseInit()
	serviceMaintenanceInit()
	serviceWatchInit()
}
