package main

import (
	"context"
	"flag"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	_ "net/http/pprof"
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
	flag.Parse()
	zongzi.SetLogLevel(zongzi.LogLevelWarning)
	ctx := context.Background()
	log := slog.Default()
	cfg := getConfig()
	go func() {
		err := http.ListenAndServe("0.0.0.0:6060", nil)
		if err != nil {
			log.Error(err.Error())
		}
	}()
	agent, err := zongzi.NewAgent(cfg.ClusterName, strings.Split(cfg.HostPeers, ","),
		zongzi.WithRaftDir(cfg.Dir+"/raft"),
		zongzi.WithWALDir(cfg.Dir+"/wal"),
		zongzi.WithGossipAddress(fmt.Sprintf("%s:%d", cfg.HostName, cfg.PortGossip)),
		zongzi.WithRaftAddress(fmt.Sprintf("%s:%d", cfg.HostName, cfg.PortRaft)),
		zongzi.WithApiAddress(fmt.Sprintf("%s:%d", cfg.HostName, cfg.PortZongzi)),
		zongzi.WithHostMemoryLimit(zongzi.HostMemory256))
	if err != nil {
		panic(err)
	}

	grpcServer := grpc.NewServer()
	if _, err = start(ctx, grpcServer, agent, log, cfg.Dir+"/data", cfg.PortApi); err != nil {
		panic(err)
	}

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

func start(
	ctx context.Context,
	grpcServer *grpc.Server,
	agent *zongzi.Agent,
	log *slog.Logger,
	dataDir string,
	port uint16,
) (shard zongzi.Shard, err error) {
	// Start zongzi
	agent.StateMachineRegister(icarus.Uri, icarus.NewStateMachineFactory(log, dataDir))
	if err = agent.Start(ctx); err != nil {
		panic(err)
	}
	shard, _, err = agent.ShardCreate(ctx, icarus.Uri,
		zongzi.WithName("icarus-standalone"),
		zongzi.WithPlacementMembers(3))
	if err != nil {
		panic(err)
	}

	/*
		for i := range 23 {
			_, _, err = agent.ShardCreate(ctx, icarus.Uri,
				zongzi.WithName(fmt.Sprintf("icarus-%05d", i)),
				zongzi.WithPlacementMembers(3))
			if err != nil {
				panic(err)
			}
		}
	*/

	client := agent.Client(shard.ID, zongzi.WithWriteToLeader())

	// Start gRPC Server
	internal.RegisterKVServer(grpcServer, icarus.NewServiceKv(client))
	internal.RegisterLeaseServer(grpcServer, icarus.NewServiceLease(client))
	lis, err := net.Listen("tcp", fmt.Sprintf(":%d", port))
	if err != nil {
		panic(err)
	}
	go func() {
		if err = grpcServer.Serve(lis); err != nil {
			panic(err)
		}
	}()
	return
}
