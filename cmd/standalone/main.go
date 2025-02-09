package main

import (
	"context"
	"fmt"
	"log/slog"
	"net"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/logbn/zongzi"
	"google.golang.org/grpc"

	"github.com/logbn/icarus"
	"github.com/logbn/icarus/internal"
)

func main() {
	zongzi.SetLogLevel(zongzi.LogLevelWarning)
	var cfg = getConfig()
	var ctx = context.Background()
	var log = slog.Default()
	var ctrl = icarus.NewController(ctx, log)
	agent, err := zongzi.NewAgent(cfg.ClusterName, strings.Split(cfg.HostPeers, ","),
		zongzi.WithRaftDir(cfg.Dir+"/raft"),
		zongzi.WithWALDir(cfg.Dir+"/wal"),
		zongzi.WithGossipAddress(fmt.Sprintf("%s:%d", cfg.HostName, cfg.PortGossip)),
		zongzi.WithRaftAddress(fmt.Sprintf("%s:%d", cfg.HostName, cfg.PortRaft)),
		zongzi.WithApiAddress(fmt.Sprintf("%s:%d", cfg.HostName, cfg.PortZongzi)),
		zongzi.WithHostMemoryLimit(zongzi.HostMemory256),
		zongzi.WithRaftEventListener(ctrl),
	)
	if err != nil {
		panic(err)
	}
	var grpcServer = grpc.NewServer()
	agent.StateMachineRegister(icarus.Uri, icarus.NewStateMachineFactory(log, cfg.Dir+"/data"))
	if err = agent.Start(ctx); err != nil {
		panic(err)
	}
	shard, _, err := agent.ShardCreate(ctx, icarus.Uri,
		zongzi.WithName("standalone-000"),
		zongzi.WithPlacementMembers(3))
	if err != nil {
		panic(err)
	}
	if err = ctrl.Start(agent, shard); err != nil {
		panic(err)
	}
	client := agent.Client(shard.ID, zongzi.WithWriteToLeader())
	internal.RegisterKVServer(grpcServer, icarus.NewServiceKv(client))
	internal.RegisterLeaseServer(grpcServer, icarus.NewServiceLease(client))
	lis, err := net.Listen("tcp", fmt.Sprintf(":%d", cfg.PortApi))
	if err != nil {
		panic(err)
	}
	go func() {
		if err = grpcServer.Serve(lis); err != nil {
			panic(err)
		}
	}()

	// await stop
	stop := make(chan os.Signal, 1)
	signal.Notify(stop, os.Interrupt)
	signal.Notify(stop, syscall.SIGTERM)
	<-stop

	if grpcServer != nil {
		var ch = make(chan bool)
		go func() {
			grpcServer.GracefulStop()
			close(ch)
		}()
		select {
		case <-ch:
		case <-time.After(5 * time.Second):
			grpcServer.Stop()
		}
	}
	agent.Stop()
}
