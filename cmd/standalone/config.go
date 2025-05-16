package main

import (
	"github.com/caarlos0/env/v11"
)

type config struct {
	ClusterName string `env:"KVR_CLUSTER_NAME"`
	Dir         string `env:"KVR_DIR"`
	HostName    string `env:"KVR_HOST_NAME"`
	HostPeers   string `env:"KVR_HOST_PEERS"`
	PortApi     uint16 `env:"KVR_PORT_API"`
	PortGossip  uint16 `env:"KVR_PORT_GOSSIP"`
	PortRaft    uint16 `env:"KVR_PORT_RAFT"`
	PortZongzi  uint16 `env:"KVR_PORT_ZONGZI"`
}

func getConfig() config {
	cfg := config{
		ClusterName: "kvr",
		Dir:         "/var/lib/kvr",
		HostName:    "kvr-0",
		HostPeers:   "kvr-0:17003,kvr-1:17003,kvr-2:17003",
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
