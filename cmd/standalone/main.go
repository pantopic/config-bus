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
		zongzi.WithApiAddress(fmt.Sprintf("%s:%d", cfg.HostName, cfg.PortZongzi)))
	if err != nil {
		panic(err)
	}

	// Start zongzi
	agent.StateMachineRegister(icarus.Uri, icarus.NewStateMachineFactory(log, cfg.Dir+"/data"))
	if err = agent.Start(ctx); err != nil {
		panic(err)
	}
	shard, _, err := agent.ShardCreate(ctx, icarus.Uri,
		zongzi.WithName("icarus-standalone"),
		zongzi.WithPlacementMembers(3))
	if err != nil {
		panic(err)
	}

	client := agent.Client(shard.ID, zongzi.WithWriteToLeader())

	// Start gRPC Server
	grcpServer := grpc.NewServer()
	internal.RegisterKVServer(grcpServer, icarus.NewKvService(client))
	stop := make(chan os.Signal, 1)
	lis, err := net.Listen("tcp", fmt.Sprintf(":%d", cfg.PortApi))
	if err != nil {
		panic(err)
	}
	go func() {
		err = grcpServer.Serve(lis)
		log.Error(err.Error())
		close(stop)
	}()

	// await stop
	signal.Notify(stop, os.Interrupt)
	signal.Notify(stop, syscall.SIGTERM)
	<-stop

	if grcpServer != nil {
		var ch = make(chan bool)
		go func() {
			grcpServer.GracefulStop()
			close(ch)
		}()
		select {
		case <-ch:
		case <-time.After(5 * time.Second):
			grcpServer.Stop()
		}
	}
	agent.Stop()
}
