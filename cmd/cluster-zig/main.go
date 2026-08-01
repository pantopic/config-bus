package main

import (
	"context"
	_ "embed"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	_ "net/http/pprof"
	"os"
	"os/signal"
	"runtime"
	"strings"
	"syscall"
	"time"

	"github.com/logbn/zongzi"
	"github.com/tetratelabs/wazero"
	"github.com/tetratelabs/wazero/api"
	"github.com/tetratelabs/wazero/imports/wasi_snapshot_preview1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/keepalive"

	"github.com/pantopic/wazero-atomic/host"
	"github.com/pantopic/wazero-buffer-pool/host"
	"github.com/pantopic/wazero-cluster/host"
	"github.com/pantopic/wazero-global/host"
	"github.com/pantopic/wazero-grpc-server/host"
	"github.com/pantopic/wazero-lmdb/host"
	"github.com/pantopic/wazero-pool"
	"github.com/pantopic/wazero-range-watch/host"
	"github.com/pantopic/wazero-shard-client/host"
	"github.com/pantopic/wazero-small-cache/host"
	"github.com/pantopic/wazero-state-machine/host"

	"github.com/pantopic/turbokube"
	"github.com/pantopic/turbokube/embed"
)

type extension interface {
	Register(context.Context, wazero.Runtime) error
	InitContext(context.Context, api.Module) (context.Context, error)
	ContextCopy(dst, src context.Context) context.Context
}

func main() {
	go func() {
		slog.Info("pprof server", "err", http.ListenAndServe(":6060", nil))
	}()
	zongzi.SetLogLevel(zongzi.LogLevelInfo)
	var cfg = getConfig()
	var ctx = context.Background()
	var log = slog.Default()
	var ctrl = pcb.NewController(ctx, log)
	agent, err := zongzi.NewAgent(cfg.ClusterName, strings.Split(cfg.HostPeers, ","),
		zongzi.WithDirRaft(cfg.Dir+"/raft"),
		zongzi.WithDirWAL(cfg.Dir+"/wal"),
		zongzi.WithHostTags(cfg.GetHostTags()...),
		zongzi.WithAddrGossip(fmt.Sprintf("%s:%d", cfg.HostName, cfg.PortGossip)),
		zongzi.WithAddrRaft(fmt.Sprintf("%s:%d", cfg.HostName, cfg.PortRaft)),
		zongzi.WithAddrApi(fmt.Sprintf("%s:%d", cfg.HostName, cfg.PortZongzi)),
		zongzi.WithHostMemoryLimit(zongzi.HostMemory256),
		zongzi.WithRaftEventListener(ctrl))
	if err != nil {
		panic(err)
	}
	hostModGlobal := wazero_global.New()
	runtimeStorageKv := wazero.NewRuntimeWithConfig(ctx, wazero.NewRuntimeConfig())
	wasi_snapshot_preview1.MustInstantiate(ctx, runtimeStorageKv)
	hostModLMDB := wazero_lmdb.New()
	storageExtensions := []extension{
		hostModLMDB,
		hostModGlobal,
		wazero_atomic.New(),
		wazero_range_watch.New(),
		wazero_small_cache.New(),
		wazero_state_machine.New(),
	}
	var ctxCopy []func(dst, src context.Context) context.Context
	for _, m := range storageExtensions {
		if err = m.Register(ctx, runtimeStorageKv); err != nil {
			panic(err)
		}
		ctxCopy = append(ctxCopy, m.ContextCopy)
	}
	poolStorageKv, err := wazeropool.New(ctx, runtimeStorageKv, turbokube.StorageKvZigWasm,
		wazeropool.WithModuleConfig(wazero.NewModuleConfig().WithStdout(os.Stdout)),
		wazeropool.WithLimit(runtime.NumCPU()),
		wazeropool.WithMemoryLimit(16<<20),
		wazeropool.WithName(turbokube.StorageKvName),
		wazeropool.WithVersion(turbokube.Version))
	if err != nil {
		panic(err)
	}
	ctx = wazeropool.ContextSet(ctx, poolStorageKv)
	poolStorageKv.Run(func(mod api.Module) {
		for _, m := range storageExtensions {
			if ctx, err = m.InitContext(ctx, mod); err != nil {
				panic(err)
			}
		}
	})
	logger := zongzi.GetLogger(`statemachine`)
	ctxInit := func(ctx context.Context, shardID, replicaID uint64) context.Context {
		dir := fmt.Sprintf("%s/data/%d/%d", cfg.Dir, shardID, replicaID)
		return wazero_lmdb.EnvRegisterDir(ctx, dir)
	}
	poolProvider := func(shardID uint64) wazeropool.Instance {
		return poolStorageKv
	}
	fsmFactory := wazero_state_machine.FactoryPersistent(ctx, ctxInit, ctxCopy, logger, poolProvider, hostModLMDB)
	agent.StateMachineRegister(turbokube.StorageKvName, fsmFactory)
	go func() {
		for {
			time.Sleep(5 * time.Second)
			if stats := poolStorageKv.Stats(); stats.Active > 0 {
				log.Info("poolStorageKv.Stats", "total", stats.Total,
					"avgMemSize", stats.MemSize/max(stats.Total, 1), "memMax", stats.MemMax, "memMin", stats.MemMin,
					"active", stats.Active/max(stats.Total, 1), "actMax", stats.ActMax, "actMin", stats.ActMin,
				)
			}
		}
	}()
	if err = agent.Start(ctx); err != nil {
		panic(err)
	}
	// TODO - Replace shard create with resource create
	shard, _, err := agent.ShardCreate(ctx, turbokube.StorageKvName,
		zongzi.WithName("default.turbokube.default.kv"),
		zongzi.WithPlacementMembers(3, `pantopic:turbokube=member`),
		zongzi.WithPlacementCover(`pantopic:turbokube=nonvoting`))
	if err != nil {
		panic(err)
	}
	if err = agent.ReplicaAwait(ctx, 30*time.Second, shard.ID); err != nil {
		panic(err)
	}
	if err = ctrl.Start(agent.Client(shard.ID), shard); err != nil {
		panic(err)
	}

	// gRPC server
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

	// Create Runtime
	runtimeServiceGrpc := wazero.NewRuntimeWithConfig(ctx, wazero.NewRuntimeConfig())
	wasi_snapshot_preview1.MustInstantiate(ctx, runtimeServiceGrpc)
	hostModGrpcServer := wazero_grpc_server.New()
	serviceExtensions := []extension{
		hostModGlobal,
		hostModGrpcServer,
		wazero_buffer_pool.New(),
		wazero_shard_client.New(agent),
	}
	var svcCtxCopy []func(dst, src context.Context) context.Context
	for _, m := range serviceExtensions {
		if err = m.Register(ctx, runtimeServiceGrpc); err != nil {
			panic(err)
		}
		svcCtxCopy = append(svcCtxCopy, m.ContextCopy)
	}
	poolServiceGrpc, err := wazeropool.New(ctx, runtimeServiceGrpc, turbokube.ServiceGrpcZigWasm,
		wazeropool.WithModuleConfig(wazero.NewModuleConfig().WithStdout(os.Stdout)),
		wazeropool.WithLimit(runtime.NumCPU()),
		wazeropool.WithName(turbokube.ServiceGrpcName),
		wazeropool.WithVersion(turbokube.Version))
	if err != nil {
		panic(err)
	}
	ctx = wazeropool.ContextSet(ctx, poolServiceGrpc)
	poolServiceGrpc.Run(func(mod api.Module) {
		for _, m := range serviceExtensions {
			if ctx, err = m.InitContext(ctx, mod); err != nil {
				panic(err)
			}
		}
	})
	svcCtxCopy = append(svcCtxCopy, wazero_cluster.NewResolver(`default`, `turbokube`))
	lis, err := net.Listen("tcp", fmt.Sprintf(":%d", cfg.PortApi))
	if err != nil {
		panic(err)
	}
	hostModGrpcServer.ServerStart(ctx, lis, cfg.TlsCrt, cfg.TlsKey, poolServiceGrpc, svcCtxCopy...)
	go func() {
		for {
			time.Sleep(5 * time.Second)
			if stats := poolServiceGrpc.Stats(); stats.Active > 0 {
				log.Info("poolServiceGrpc.Stats", "total", stats.Total,
					"avgMemSize", stats.MemSize/max(stats.Total, 1), "memMax", stats.MemMax, "memMin", stats.MemMin,
					"active", stats.Active/max(stats.Total, 1), "actMax", stats.ActMax, "actMin", stats.ActMin,
				)
			}
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
			hostModGrpcServer.Stop()
			close(ch)
		}()
		select {
		case <-ch:
		case <-time.After(5 * time.Second):
		}
	}
	agent.Stop()
}
