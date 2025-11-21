package main

import (
	"context"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/logbn/zongzi"
	"github.com/soheilhy/cmux"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/keepalive"

	"github.com/pantopic/krv"
	"github.com/pantopic/krv/internal"
)

func main() {
	zongzi.SetLogLevel(zongzi.LogLevelInfo)
	var cfg = getConfig()
	var ctx = context.Background()
	var log = slog.Default()
	var apiAddr = fmt.Sprintf("%s:%d", cfg.HostName, cfg.PortApi)
	var ctrl = krv.NewController(ctx, log)
	agent, err := zongzi.NewAgent(cfg.ClusterName, strings.Split(cfg.HostPeers, ","),
		zongzi.WithDirRaft(cfg.Dir+"/raft"),
		zongzi.WithDirWAL(cfg.Dir+"/wal"),
		zongzi.WithHostTags(cfg.GetHostTags()...),
		zongzi.WithAddrGossip(fmt.Sprintf("%s:%d", cfg.HostName, cfg.PortGossip)),
		zongzi.WithAddrRaft(fmt.Sprintf("%s:%d", cfg.HostName, cfg.PortRaft)),
		zongzi.WithAddrApi(fmt.Sprintf("%s:%d", cfg.HostName, cfg.PortZongzi)),
		zongzi.WithHostMemoryLimit(zongzi.HostMemory256),
		zongzi.WithRaftEventListener(ctrl),
	)
	if err != nil {
		panic(err)
	}
	var opts = []grpc.ServerOption{
		grpc.KeepaliveEnforcementPolicy(keepalive.EnforcementPolicy{
			MinTime:             5 * time.Second,
			PermitWithoutStream: false,
		}),
		grpc.KeepaliveParams(keepalive.ServerParameters{
			Time:    2 * time.Hour,
			Timeout: 20 * time.Second,
		}),
	}
	if cfg.TlsCrt != "" && cfg.TlsKey != "" {
		fc, err := credentials.NewServerTLSFromFile(cfg.TlsCrt, cfg.TlsKey)
		if err != nil {
			panic(err)
		}
		opts = append(opts, grpc.Creds(fc))
	}
	var grpcServer = grpc.NewServer(opts...)
	agent.StateMachineRegister(krv.Uri, krv.NewStateMachineFactory(log, cfg.Dir+"/data"))
	if err = agent.Start(ctx); err != nil {
		panic(err)
	}
	shard, _, err := agent.ShardCreate(ctx, krv.Uri,
		zongzi.WithName("krv"),
		zongzi.WithPlacementMembers(3, `pantopic/krv=member`),
		zongzi.WithPlacementCover(`pantopic/krv=nonvoting`))
	if err != nil {
		panic(err)
	}
	if err = agent.ReplicaAwait(ctx, 30*time.Second, shard.ID); err != nil {
		panic(err)
	}
	if err = ctrl.Start(agent.Client(shard.ID), shard); err != nil {
		panic(err)
	}
	client := agent.Client(shard.ID, zongzi.WithWriteToLeader())
	internal.RegisterKVServer(grpcServer, krv.NewServiceKv(client))
	internal.RegisterWatchServer(grpcServer, krv.NewServiceWatch(client))
	internal.RegisterLeaseServer(grpcServer, krv.NewServiceLease(client))
	internal.RegisterMaintenanceServer(grpcServer, krv.NewServiceMaintenance(client))
	internal.RegisterClusterServer(grpcServer, krv.NewServiceCluster(client, apiAddr))
	lis, err := net.Listen("tcp", fmt.Sprintf(":%d", cfg.PortApi))
	if err != nil {
		panic(err)
	}
	m := cmux.New(lis)
	grpcL := m.Match(cmux.HTTP2())
	httpL := m.Match(cmux.Any())
	go func() {
		if err = grpcServer.Serve(grpcL); err != nil {
			panic(err)
		}
	}()
	httpS := &http.Server{
		Handler: krv.NewEndpointHandler(grpcServer),
	}
	go func() {
		if cfg.TlsCrt != "" && cfg.TlsKey != "" {
			if err = httpS.ServeTLS(httpL, cfg.TlsCrt, cfg.TlsKey); err != nil {
				panic(err)
			}
		} else {
			if err = httpS.Serve(httpL); err != nil {
				panic(err)
			}
		}
	}()
	go func() {
		if err := m.Serve(); err != nil {
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
