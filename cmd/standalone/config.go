package main

import (
	"strings"

	"github.com/caarlos0/env/v11"
)

type config struct {
	ClusterName string `env:"PCB_CLUSTER_NAME"`
	Dir         string `env:"PCB_DIR"`
	HostName    string `env:"PCB_HOST_NAME"`
	HostPeers   string `env:"PCB_HOST_PEERS"`
	HostTags    string `env:"PCB_HOST_TAGS"`
	PortApi     uint16 `env:"PCB_PORT_API"`
	PortGossip  uint16 `env:"PCB_PORT_GOSSIP"`
	PortRaft    uint16 `env:"PCB_PORT_RAFT"`
	PortZongzi  uint16 `env:"PCB_PORT_ZONGZI"`
	TlsCrt      string `env:"PCB_TLS_CRT"`
	TlsKey      string `env:"PCB_TLS_KEY"`
}

func getConfig() config {
	cfg := config{
		ClusterName: "pcb",
		Dir:         "/var/lib/pcb",
		HostName:    "pcb-0",
		HostPeers:   "pcb-0:17003,pcb-1:17003,pcb-2:17003",
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

func (c config) GetHostTags() (tags []string) {
	tags = []string{}
	for _, t := range strings.Split(c.HostTags, ",") {
		t = strings.TrimSpace(t)
		if len(t) == 0 {
			continue
		}
		tags = append(tags, t)
	}
	return
}
