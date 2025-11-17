package main

import (
	"context"
	_ "embed"
	"fmt"
	"log/slog"
	"net"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/logbn/zongzi"
	"google.golang.org/grpc"

	"github.com/pantopic/cluster-runtime-wazero"
	"github.com/pantopic/wazero-grpc-server/host"
	"github.com/pantopic/wazero-lmdb/host"
	"github.com/pantopic/wazero-shard-client/host"
	"github.com/pantopic/wazero-state-machine/host"

	"github.com/pantopic/krv"
	"github.com/pantopic/krv/internal"
)

//go:embed storage.wasm
var wasmStorage []byte

//go:embed service.wasm
var wasmService []byte

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
		zongzi.WithAddrGossip(hostport(cfg.HostName, cfg.PortGossip)),
		zongzi.WithAddrRaft(hostport(cfg.HostName, cfg.PortRaft)),
		zongzi.WithAddrApi(hostport(cfg.HostName, cfg.PortZongzi)),
		zongzi.WithHostMemoryLimit(zongzi.HostMemory256),
		zongzi.WithRaftEventListener(ctrl),
	)
	if err != nil {
		panic(err)
	}
	// Create wazero runtime
	runtime := cluster_runtime_wazero.New(ctx, agent)
	// Register runtime extensions
	for _, ext := range []cluster_runtime_wazero.Extension{
		wazero_grpc_server.New(),
		wazero_lmdb.New(),
		wazero_shard_client.New(),
		wazero_state_machine.New(),
	} {
		if err = runtime.ExtensionRegister(ext); err != nil {
			panic(fmt.Errorf("Unable to register extension %s: %w", ext.Name(), err))
		}
	}
	// Create service
	service, err := cluster.ServiceFactory(wasmService)
	if err != nil {
		panic(err)
	}
	// pool, err := runtime.ModuleRegister(wasmStorage)
	// sm, err := wazero_state_machine.NewFactory(ctx, pool)
	smf, err := runtime.StateMachineFactory(wasmStorage)
	if err != nil {
		panic(err)
	}
	agent.StateMachineRegister(krv.Uri, smf)
	if err = agent.Start(ctx); err != nil {
		panic(err)
	}
	shard, _, err := agent.ShardCreate(ctx, krv.Uri,
		zongzi.WithName("krv"),
		zongzi.WithPlacementMembers(3))
	if err != nil {
		panic(err)
	}
	client := agent.Client(shard.ID, zongzi.WithWriteToLeader())
	if err = ctrl.Start(client, shard); err != nil {
		panic(err)
	}
	var grpcServer = grpc.NewServer()
	// Adapter registers runtime services with grpc server
	ext := wazero_grpc_server.New()
	if err := ext.Register(ctx, runtime); err != nil {
		panic(err)
	}
	ext.RegisterServices(ctx, grpcServer, ext.InstancePoolFactory())
	// old
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

func hostport(host string, port uint16) string {
	return net.JoinHostPort(host, strconv.Itoa(int(port)))
}
