package main

import (
	"strings"

	"github.com/caarlos0/env/v11"
)

type config struct {
	ClusterName string `env:"KRV_CLUSTER_NAME"`
	Dir         string `env:"KRV_DIR"`
	HostName    string `env:"KRV_HOST_NAME"`
	HostPeers   string `env:"KRV_HOST_PEERS"`
	HostTags    string `env:"KRV_HOST_TAGS"`
	PortApi     uint16 `env:"KRV_PORT_API"`
	PortGossip  uint16 `env:"KRV_PORT_GOSSIP"`
	PortRaft    uint16 `env:"KRV_PORT_RAFT"`
	PortZongzi  uint16 `env:"KRV_PORT_ZONGZI"`
	TlsCrt      string `env:"KRV_TLS_CRT"`
	TlsKey      string `env:"KRV_TLS_KEY"`
}

func getConfig() config {
	cfg := config{
		ClusterName: "krv",
		Dir:         "/var/lib/krv",
		HostName:    "krv-0",
		HostPeers:   "krv-0:17003,krv-1:17003,krv-2:17003",
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
