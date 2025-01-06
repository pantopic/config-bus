package main

import (
	"github.com/caarlos0/env/v11"
)

type config struct {
	ClusterName string `env:"ICARUS_CLUSTER_NAME"`
	Dir         string `env:"ICARUS_DIR"`
	HostName    string `env:"ICARUS_HOST_NAME"`
	HostPeers   string `env:"ICARUS_HOST_PEERS"`
	PortApi     uint16 `env:"ICARUS_PORT_API"`
	PortGossip  uint16 `env:"ICARUS_PORT_GOSSIP"`
	PortRaft    uint16 `env:"ICARUS_PORT_RAFT"`
	PortZongzi  uint16 `env:"ICARUS_PORT_ZONGZI"`
}

func getConfig() config {
	cfg := config{
		ClusterName: "icarus",
		Dir:         "/var/lib/icarus",
		HostName:    "icarus-0",
		HostPeers:   "icarus-0:17003,icarus-1:17003,icarus-2:17003",
		PortApi:     19000,
		PortGossip:  17001,
		PortRaft:    17002,
		PortZongzi:  17003,
	}
	if err := env.Parse(&cfg); err != nil {
		panic(err)
	}
	return cfg
}
