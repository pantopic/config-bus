package main

import (
	"github.com/pantopic/wazero-pipe/sdk-go"
)

var (
	leaseBridge *pipe.Pipe[[]byte]
)

func main() {
	leaseBridge = pipe.New[[]byte](pipe.WithID(0))
	kvInit()
	leaseInit()
	clusterInit()
	maintenanceInit()
}
