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

	"github.com/pantopic/wazero"
	"github.com/pantopic/wazero-lmdb/host"

	"github.com/pantopic/krv"
	"github.com/pantopic/krv/internal"
)

func main() {
	zongzi.SetLogLevel(zongzi.LogLevelWarning)
	var cfg = getConfig()
	var ctx = context.Background()
	var log = slog.Default()
	var apiAddr = fmt.Sprintf("%s:%d", cfg.HostName, cfg.PortApi)
	var ctrl = krv.NewController(ctx, log)
	agent, err := zongzi.NewAgent(cfg.ClusterName, strings.Split(cfg.HostPeers, ","),
		zongzi.WithDirRaft(cfg.Dir+"/raft"),
		zongzi.WithDirWAL(cfg.Dir+"/wal"),
		zongzi.WithAddrGossip(fmt.Sprintf("%s:%d", cfg.HostName, cfg.PortGossip)),
		zongzi.WithAddrRaft(fmt.Sprintf("%s:%d", cfg.HostName, cfg.PortRaft)),
		zongzi.WithAddrApi(fmt.Sprintf("%s:%d", cfg.HostName, cfg.PortZongzi)),
		zongzi.WithHostMemoryLimit(zongzi.HostMemory256),
		zongzi.WithRaftEventListener(ctrl),
	)
	if err != nil {
		panic(err)
	}
	var binary []byte
	runtime := wazero.NewRuntime(ctx)
	runtime.ExtensionRegister(wazero_lmdb.New(cfg.Dir + "/data"))
	sm, err := runtime.StateMachineFactory(binary)
	if err != nil {
		panic(err)
	}
	agent.StateMachineRegister(krv.Uri, sm)
	if err = agent.Start(ctx); err != nil {
		panic(err)
	}
	shard, _, err := agent.ShardCreate(ctx, krv.Uri,
		zongzi.WithName("krv"),
		zongzi.WithPlacementMembers(3))
	if err != nil {
		panic(err)
	}
	if err = ctrl.Start(agent.Client(shard.ID), shard); err != nil {
		panic(err)
	}
	client := agent.Client(shard.ID, zongzi.WithWriteToLeader())
	var grpcServer = grpc.NewServer()
	internal.RegisterKVServer(grpcServer, krv.NewServiceKv(client))
	internal.RegisterLeaseServer(grpcServer, krv.NewServiceLease(client))
	internal.RegisterClusterServer(grpcServer, krv.NewServiceCluster(client, apiAddr))
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
