//go:build !unit

package icarus

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"os"
	"testing"
	"time"

	"github.com/logbn/zongzi"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	"github.com/logbn/icarus/internal"
)

var (
	ctx    = context.Background()
	parity = os.Getenv("ICARUS_PARITY_CHECK") == "true"

	err      error
	svcKv    internal.KVServer
	svcLease internal.LeaseServer
	svcWatch internal.WatchServer
)

func TestService(t *testing.T) {
	if parity {
		t.Run("setup-parity", setupParity)
	} else {
		t.Run("setup-icarus", setupIcarus)
	}
	t.Run("insert", testInsert)
	t.Run("update", testUpdate)
	t.Run("range", testRange)
	t.Run("patch", testPatch)
	t.Run("delete", testDelete)
	t.Run("compact", testCompact)
	t.Run("transaction", testTransaction)
	t.Run("lease-grant", testLeaseGrant)
	t.Run("lease-revoke", testLeaseRevoke)
	// TODO - Keep Alive keeps itself alive
	t.Run("lease-keep-alive", testLeaseKeepAlive)
	// TODO - Advance epoch to test lease TTL response
	t.Run("lease-ttl", testLeaseTimeToLive)
	t.Run("lease-leases", testLeaseLeases)
	// TODO - Watch at revision, progress notify, drive watches async from kv_events
	// TODO - Watch merge w/ multiplex, auto reconnect
	t.Run("watch", testWatch)

	// Add leader tick in controller to make epoch work
	// - Incr epoch
	// - Sweep leases and keys
	// - Quarantine dead leases w/ amortized cleanup

	// Status
	// Defragment
	// Hash
	// HashKV
	// MemberList
}

// Run integration tests against locally running etcd instance
// Be sure to completely destroy the etcd cluster between parity runs
// Otherwise data from previous runs will give bad results
func setupParity(t *testing.T) {
	conn, err := grpc.NewClient("127.0.0.1:2379", grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		panic(err)
	}
	svcKv = newParityKvService(conn)
	svcLease = newParityLeaseService(conn)
	svcWatch = newParityWatchService(conn)

	// Etcd bugs
	ICARUS_RANGE_COUNT_FILTER_CORRECT = false
}

// Run integration tests against bootstrapped icarus instance
func setupIcarus(t *testing.T) {
	logLevel := new(slog.LevelVar)
	if os.Getenv("ICARUS_LOG_LEVEL") == "debug" {
		logLevel.Set(slog.LevelDebug)
	} else {
		logLevel.Set(slog.LevelInfo)
	}
	log := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{
		Level: logLevel,
	}))
	var (
		agents = make([]*zongzi.Agent, 3)
		dir    = "/tmp/icarus/test"
		host   = "127.0.0.1"
		port   = 19000
		peers  = []string{
			fmt.Sprintf(host+":%d", port+3),
			fmt.Sprintf(host+":%d", port+13),
			fmt.Sprintf(host+":%d", port+23),
		}
	)
	zongzi.SetLogLevel(zongzi.LogLevelWarning)
	if err = os.RemoveAll(dir); err != nil {
		panic(err)
	}
	var shard zongzi.Shard
	for i := range len(agents) {
		dir := fmt.Sprintf("%s/%d", dir, i)
		if agents[i], err = zongzi.NewAgent("icarus", peers,
			zongzi.WithRaftDir(dir+"/raft"),
			zongzi.WithWALDir(dir+"/wal"),
			zongzi.WithGossipAddress(fmt.Sprintf(host+":%d", port+(i*10)+1)),
			zongzi.WithRaftAddress(fmt.Sprintf(host+":%d", port+(i*10)+2)),
			zongzi.WithApiAddress(fmt.Sprintf(host+":%d", port+(i*10)+3)),
		); err != nil {
			panic(err)
		}
		agents[i].StateMachineRegister(Uri, NewStateMachineFactory(log, dir+"/data"))
		go func(i int) {
			if err = agents[i].Start(ctx); err != nil {
				panic(err)
			}
			shard, _, err = agents[i].ShardCreate(ctx, Uri,
				zongzi.WithName("icarus-standalone"),
				zongzi.WithPlacementMembers(3))
			if err != nil {
				panic(err)
			}
		}(i)
	}
	// 10 seconds to start the cluster.
	require.True(t, await(10, 100, func() bool {
		for _, agent := range agents {
			if agent.Status() != zongzi.AgentStatus_Ready {
				return false
			}
		}
		return true
	}), `%#v`, agents)
	// 5 seconds for shard to have active leader
	require.True(t, await(10, 100, func() bool {
		agents[0].State(ctx, func(s *zongzi.State) {
			if found, ok := s.Shard(shard.ID); ok {
				shard = found
			}
		})
		return shard.Leader > 0
	}))
	svcKv = NewServiceKv(agents[0].Client(shard.ID))
	svcLease = NewServiceLease(agents[0].Client(shard.ID))
	svcWatch = NewServiceWatch(agents[0].Client(shard.ID))
}

func testInsert(t *testing.T) {
	put := &internal.PutRequest{
		Key:   []byte(`test-key`),
		Value: []byte(`test-value`),
	}
	t.Run("put", func(t *testing.T) {
		resp, err := svcKv.Put(ctx, put)
		require.Nil(t, err, err)
		assert.NotNil(t, resp)
		assert.Greater(t, resp.Header.Revision, int64(0))
	})
	t.Run("get", func(t *testing.T) {
		resp, err := svcKv.Range(ctx, &internal.RangeRequest{
			Key: put.Key,
		})
		require.Nil(t, err, err)
		assert.NotNil(t, resp)
		assert.Greater(t, resp.Header.Revision, int64(0))
		require.Greater(t, len(resp.Kvs), 0)
		assert.Equal(t, put.Key, resp.Kvs[0].Key)
		assert.Equal(t, put.Value, resp.Kvs[0].Value)
	})
}

func testUpdate(t *testing.T) {
	var rev int64
	put := &internal.PutRequest{
		Key:   []byte(`test-key`),
		Value: []byte(`test-value-2`),
	}
	t.Run("put", func(t *testing.T) {
		resp, err := svcKv.Put(ctx, put)
		require.Nil(t, err, err)
		assert.NotNil(t, resp)
		rev = resp.Header.Revision
	})
	t.Run("get", func(t *testing.T) {
		resp, err := svcKv.Range(ctx, &internal.RangeRequest{
			Key: put.Key,
		})
		require.Nil(t, err, err)
		assert.NotNil(t, resp)
		assert.Equal(t, resp.Header.Revision, rev)
		require.Greater(t, len(resp.Kvs), 0)
		assert.Equal(t, put.Key, resp.Kvs[0].Key)
		assert.Equal(t, put.Value, resp.Kvs[0].Value)
	})
	t.Run("prev", func(t *testing.T) {
		resp, err := svcKv.Put(ctx, &internal.PutRequest{
			Key:    []byte(`test-key`),
			Value:  []byte(`test-value-3`),
			PrevKv: true,
		})
		require.Nil(t, err, err)
		assert.NotNil(t, resp)
		assert.Greater(t, resp.Header.Revision, rev)
		assert.Equal(t, resp.PrevKv.Value, put.Value)
	})
	t.Run("ignore-value", func(t *testing.T) {
		resp, err := svcKv.Put(ctx, &internal.PutRequest{
			Key:         []byte(`test-key`),
			Value:       []byte(`test-value-4`),
			IgnoreValue: true,
		})
		require.NotNil(t, err, err)
		assert.Equal(t, internal.ErrGRPCValueProvided, err)
		resp, err = svcKv.Put(ctx, &internal.PutRequest{
			Key:         []byte(`test-key`),
			IgnoreValue: true,
		})
		require.Nil(t, err, err)
		assert.NotNil(t, resp)
		resp2, err := svcKv.Range(ctx, &internal.RangeRequest{
			Key: []byte(`test-key`),
		})
		require.Nil(t, err, err)
		assert.NotNil(t, resp2)
		require.Equal(t, len(resp2.Kvs), 1)
		assert.Equal(t, []byte(`test-value-3`), resp2.Kvs[0].Value)
	})
	return
}

func testRange(t *testing.T) {
	resp1, err := svcKv.Put(ctx, &internal.PutRequest{
		Key:   []byte(`test-range-key-1`),
		Value: []byte(`test-range-value-2`),
	})
	require.Nil(t, err, err)
	rev := resp1.Header.Revision
	_, err = svcKv.Put(ctx, &internal.PutRequest{
		Key:   []byte(`test-range-key-2`),
		Value: []byte(`test-range-value-2`),
	})
	require.Nil(t, err, err)
	t.Run("all", func(t *testing.T) {
		resp, err := svcKv.Range(ctx, &internal.RangeRequest{
			Key:      []byte{0},
			RangeEnd: []byte{0},
		})
		require.Nil(t, err, err)
		assert.NotNil(t, resp)
		require.Equal(t, 3, len(resp.Kvs))
	})
	t.Run("basic", func(t *testing.T) {
		resp, err := svcKv.Range(ctx, &internal.RangeRequest{
			Key:      []byte(`test-range-key-1`),
			RangeEnd: []byte(`test-range-key-2`),
		})
		require.Nil(t, err, err)
		assert.NotNil(t, resp)
		assert.Greater(t, resp.Header.Revision, int64(0))
		require.Equal(t, 1, len(resp.Kvs))
		assert.Equal(t, []byte(`test-range-key-1`), resp.Kvs[0].Key)
	})
	t.Run("revision", func(t *testing.T) {
		resp, err := svcKv.Range(ctx, &internal.RangeRequest{
			Key:      []byte(`test-range-key-1`),
			RangeEnd: []byte(`test-range-key-3`),
			Revision: rev,
		})
		require.Nil(t, err, err)
		assert.NotNil(t, resp)
		assert.Greater(t, resp.Header.Revision, int64(0))
		require.Equal(t, 1, len(resp.Kvs))
		assert.Equal(t, []byte(`test-range-key-1`), resp.Kvs[0].Key, string(resp.Kvs[0].Key))
		assert.Equal(t, []byte(`test-range-value-2`), resp.Kvs[0].Value, string(resp.Kvs[0].Value))
	})
	t.Run("next", func(t *testing.T) {
		resp, err := svcKv.Range(ctx, &internal.RangeRequest{
			Key:      []byte(`test`),
			RangeEnd: []byte(`vest`),
		})
		require.Nil(t, err, err)
		assert.NotNil(t, resp)
		assert.Greater(t, resp.Header.Revision, int64(0))
		assert.Equal(t, 3, len(resp.Kvs))
		assert.Equal(t, []byte(`test-key`), resp.Kvs[0].Key, string(resp.Kvs[0].Key))
		assert.Equal(t, []byte(`test-range-key-1`), resp.Kvs[1].Key, string(resp.Kvs[1].Key))
		assert.Equal(t, []byte(`test-range-key-2`), resp.Kvs[2].Key, string(resp.Kvs[2].Key))
	})
	t.Run("missing", func(t *testing.T) {
		resp, err := svcKv.Range(ctx, &internal.RangeRequest{
			Key: []byte(`rest`),
		})
		require.Nil(t, err, err)
		assert.NotNil(t, resp)
		assert.Greater(t, resp.Header.Revision, int64(0))
		require.Equal(t, 0, len(resp.Kvs))
	})
	var revs []int64
	for i := range 100 {
		resp, err := svcKv.Put(ctx, &internal.PutRequest{
			Key:   []byte(fmt.Sprintf(`test-range-%03d`, i)),
			Value: []byte(fmt.Sprintf(`value-range-%03d`, i)),
		})
		revs = append(revs, resp.Header.Revision)
		require.Nil(t, err, err)
	}
	t.Run("count", func(t *testing.T) {
		t.Run("only", func(t *testing.T) {
			resp, err := svcKv.Range(ctx, &internal.RangeRequest{
				Key:       []byte(`test-range-000`),
				RangeEnd:  []byte(`test-range-100`),
				CountOnly: true,
			})
			require.Nil(t, err, err)
			assert.NotNil(t, resp)
			require.Equal(t, int64(100), resp.Count)
			resp, err = svcKv.Range(ctx, &internal.RangeRequest{
				Key:       []byte(`test-range-050`),
				RangeEnd:  []byte(`test-range-100`),
				CountOnly: true,
			})
			require.Nil(t, err, err)
			assert.NotNil(t, resp)
			require.Equal(t, int64(50), resp.Count)
		})
		t.Run("partial", func(t *testing.T) {
			withGlobal(&ICARUS_KV_RANGE_COUNT_FULL, false, func() {
				resp, err := svcKv.Range(ctx, &internal.RangeRequest{
					Key:      []byte(`test-range-000`),
					RangeEnd: []byte(`test-range-100`),
					Limit:    10,
				})
				require.Nil(t, err, err)
				require.NotNil(t, resp)
				assert.Len(t, resp.Kvs, 10)
				if parity {
					// Full count always enabled for etcd
					assert.Equal(t, int64(100), resp.Count)
				} else {
					assert.Equal(t, int64(0), resp.Count)
				}
			})
		})
		t.Run("full", func(t *testing.T) {
			withGlobal(&ICARUS_KV_RANGE_COUNT_FULL, true, func() {
				resp, err := svcKv.Range(ctx, &internal.RangeRequest{
					Key:      []byte(`test-range-000`),
					RangeEnd: []byte(`test-range-100`),
					Limit:    10,
				})
				require.Nil(t, err, err)
				require.NotNil(t, resp)
				assert.Len(t, resp.Kvs, 10)
				assert.Equal(t, int64(100), resp.Count)
				resp, err = svcKv.Range(ctx, &internal.RangeRequest{
					Key:      []byte(`test-range-000`),
					RangeEnd: []byte(`test-range-100`),
					Revision: revs[49],
					Limit:    10,
				})
				require.Nil(t, err, err)
				require.NotNil(t, resp)
				assert.Len(t, resp.Kvs, 10)
				assert.Equal(t, int64(50), resp.Count)
			})
		})
		t.Run("fake", func(t *testing.T) {
			withGlobal(&ICARUS_KV_RANGE_COUNT_FAKE, true, func() {
				resp, err := svcKv.Range(ctx, &internal.RangeRequest{
					Key:      []byte(`test-range-000`),
					RangeEnd: []byte(`test-range-100`),
					Limit:    10,
				})
				require.Nil(t, err, err)
				require.NotNil(t, resp)
				assert.Len(t, resp.Kvs, 10)
				if parity {
					// Full count always enabled for etcd
					assert.Equal(t, int64(100), resp.Count)
				} else {
					assert.Equal(t, int64(11), resp.Count)
				}
				assert.True(t, resp.More)
				resp, err = svcKv.Range(ctx, &internal.RangeRequest{
					Key:      []byte(`test-range-000`),
					RangeEnd: []byte(`test-range-100`),
					Revision: revs[49],
					Limit:    10,
				})
				require.Nil(t, err, err)
				require.NotNil(t, resp)
				assert.Len(t, resp.Kvs, 10)
				if parity {
					// Full count always enabled for etcd
					assert.Equal(t, int64(50), resp.Count)
				} else {
					assert.Equal(t, int64(11), resp.Count)
				}
				assert.True(t, resp.More)
			})
		})
	})
	var modRevs []int64
	for i := range 100 {
		resp, err := svcKv.Put(ctx, &internal.PutRequest{
			Key:   []byte(fmt.Sprintf(`test-range-%03d`, i)),
			Value: []byte(fmt.Sprintf(`value-range-%03d`, i+100)),
		})
		modRevs = append(modRevs, resp.Header.Revision)
		require.Nil(t, err, err)
	}
	t.Run("min-max-rev", func(t *testing.T) {
		t.Run("mod", func(t *testing.T) {
			resp, err := svcKv.Range(ctx, &internal.RangeRequest{
				Key:            []byte(`test-range-000`),
				RangeEnd:       []byte(`test-range-100`),
				MinModRevision: modRevs[50],
			})
			require.Nil(t, err, err)
			require.NotNil(t, resp)
			assert.Len(t, resp.Kvs, 50)
			if ICARUS_RANGE_COUNT_FILTER_CORRECT {
				assert.Equal(t, int64(50), resp.Count)
			} else {
				assert.Equal(t, int64(100), resp.Count)
			}
			resp, err = svcKv.Range(ctx, &internal.RangeRequest{
				Key:            []byte(`test-range-000`),
				RangeEnd:       []byte(`test-range-100`),
				MaxModRevision: modRevs[49],
			})
			require.Nil(t, err, err)
			require.NotNil(t, resp)
			assert.Len(t, resp.Kvs, 50)
			if ICARUS_RANGE_COUNT_FILTER_CORRECT {
				assert.Equal(t, int64(50), resp.Count)
			} else {
				assert.Equal(t, int64(100), resp.Count)
			}
			resp, err = svcKv.Range(ctx, &internal.RangeRequest{
				Key:            []byte(`test-range-000`),
				RangeEnd:       []byte(`test-range-100`),
				MinModRevision: modRevs[25],
				MaxModRevision: modRevs[74],
			})
			require.Nil(t, err, err)
			require.NotNil(t, resp)
			assert.Len(t, resp.Kvs, 50)
			if ICARUS_RANGE_COUNT_FILTER_CORRECT {
				assert.Equal(t, int64(50), resp.Count)
			} else {
				assert.Equal(t, int64(100), resp.Count)
			}
		})
		t.Run("create", func(t *testing.T) {
			resp, err := svcKv.Range(ctx, &internal.RangeRequest{
				Key:               []byte(`test-range-000`),
				RangeEnd:          []byte(`test-range-100`),
				MinCreateRevision: revs[50],
			})
			require.Nil(t, err, err)
			require.NotNil(t, resp)
			assert.Len(t, resp.Kvs, 50)
			if ICARUS_RANGE_COUNT_FILTER_CORRECT {
				assert.Equal(t, int64(50), resp.Count)
			} else {
				assert.Equal(t, int64(100), resp.Count)
			}
			resp, err = svcKv.Range(ctx, &internal.RangeRequest{
				Key:               []byte(`test-range-000`),
				RangeEnd:          []byte(`test-range-100`),
				MaxCreateRevision: revs[49],
			})
			require.Nil(t, err, err)
			require.NotNil(t, resp)
			assert.Len(t, resp.Kvs, 50)
			if ICARUS_RANGE_COUNT_FILTER_CORRECT {
				assert.Equal(t, int64(50), resp.Count)
			} else {
				assert.Equal(t, int64(100), resp.Count)
			}
			resp, err = svcKv.Range(ctx, &internal.RangeRequest{
				Key:               []byte(`test-range-000`),
				RangeEnd:          []byte(`test-range-100`),
				MinCreateRevision: revs[25],
				MaxCreateRevision: revs[74],
			})
			require.Nil(t, err, err)
			require.NotNil(t, resp)
			assert.Len(t, resp.Kvs, 50)
			if ICARUS_RANGE_COUNT_FILTER_CORRECT {
				assert.Equal(t, int64(50), resp.Count)
			} else {
				assert.Equal(t, int64(100), resp.Count)
			}
		})
		t.Run("both", func(t *testing.T) {
			resp, err := svcKv.Range(ctx, &internal.RangeRequest{
				Key:               []byte(`test-range-000`),
				RangeEnd:          []byte(`test-range-100`),
				MinCreateRevision: revs[0],
				MaxCreateRevision: revs[74],
				MinModRevision:    modRevs[25],
				MaxModRevision:    modRevs[99],
			})
			require.Nil(t, err, err)
			require.NotNil(t, resp)
			assert.Len(t, resp.Kvs, 50)
			if ICARUS_RANGE_COUNT_FILTER_CORRECT {
				assert.Equal(t, int64(50), resp.Count)
			} else {
				assert.Equal(t, int64(100), resp.Count)
			}
			resp, err = svcKv.Range(ctx, &internal.RangeRequest{
				Key:               []byte(`test-range-000`),
				RangeEnd:          []byte(`test-range-100`),
				MinCreateRevision: revs[25],
				MaxCreateRevision: revs[99],
				MinModRevision:    modRevs[0],
				MaxModRevision:    modRevs[74],
			})
			require.Nil(t, err, err)
			require.NotNil(t, resp)
			assert.Len(t, resp.Kvs, 50)
			if ICARUS_RANGE_COUNT_FILTER_CORRECT {
				assert.Equal(t, int64(50), resp.Count)
			} else {
				assert.Equal(t, int64(100), resp.Count)
			}
		})
	})
}

func testPatch(t *testing.T) {
	resp, err := svcKv.Put(ctx, &internal.PutRequest{
		Key:   []byte(`test-key-patch`),
		Value: []byte(`--------------------------------------------------------------------------------`),
	})
	require.Nil(t, err, err)
	rev := resp.Header.Revision
	t.Run("put", func(t *testing.T) {
		resp, err := svcKv.Put(ctx, &internal.PutRequest{
			Key:   []byte(`test-key-patch`),
			Value: []byte(`----------------------------------------0----------------------------------------`),
		})
		require.Nil(t, err, err)
		assert.NotNil(t, resp)
		assert.Greater(t, resp.Header.Revision, rev)
	})
	t.Run("revision", func(t *testing.T) {
		resp, err := svcKv.Range(ctx, &internal.RangeRequest{
			Key:      []byte(`test-key-patch`),
			Revision: rev,
		})
		require.Nil(t, err, err)
		assert.NotNil(t, resp)
		assert.Greater(t, resp.Header.Revision, int64(0))
		require.Equal(t, 1, len(resp.Kvs))
		assert.Equal(t, []byte(`test-key-patch`), resp.Kvs[0].Key, string(resp.Kvs[0].Key))
		assert.Equal(t, 80, len(resp.Kvs[0].Value), string(resp.Kvs[0].Value))
	})
}

func testDelete(t *testing.T) {
	t.Run("one", func(t *testing.T) {
		var k = []byte(`test-key-delete-one`)
		var v = []byte(`test-val`)
		_, err := svcKv.Put(ctx, &internal.PutRequest{Key: k, Value: v})
		require.Nil(t, err, err)
		resp, err := svcKv.Range(ctx, &internal.RangeRequest{Key: k})
		require.Nil(t, err, err)
		assert.Equal(t, 1, len(resp.Kvs))
		resp2, err := svcKv.DeleteRange(ctx, &internal.DeleteRangeRequest{
			Key: k,
		})
		require.Nil(t, err, err)
		assert.EqualValues(t, 1, resp2.Deleted)
		resp, err = svcKv.Range(ctx, &internal.RangeRequest{
			Key: k,
		})
		require.Nil(t, err, err)
		assert.Equal(t, 0, len(resp.Kvs))
	})
	t.Run("range", func(t *testing.T) {
		for i := range 10 {
			_, err = svcKv.Put(ctx, &internal.PutRequest{
				Key:   []byte(fmt.Sprintf(`test-key-delete-%02d`, i)),
				Value: []byte(`-------------------`),
			})
			require.Nil(t, err, err)
		}
		resp, err := svcKv.Range(ctx, &internal.RangeRequest{
			Key:      []byte(`test-key-delete-00`),
			RangeEnd: []byte(`test-key-delete-10`),
		})
		require.Nil(t, err, err)
		assert.Equal(t, 10, len(resp.Kvs))
		resp2, err := svcKv.DeleteRange(ctx, &internal.DeleteRangeRequest{
			Key:      []byte(`test-key-delete-00`),
			RangeEnd: []byte(`test-key-delete-10`),
		})
		require.Nil(t, err, err)
		assert.EqualValues(t, 10, resp2.Deleted)
		resp, err = svcKv.Range(ctx, &internal.RangeRequest{
			Key:      []byte(`test-key-delete-00`),
			RangeEnd: []byte(`test-key-delete-10`),
		})
		require.Nil(t, err, err)
		assert.Equal(t, 0, len(resp.Kvs))
	})
	t.Run("missing", func(t *testing.T) {
		var k = []byte(`test-key-delete-one`)
		resp, err := svcKv.Range(ctx, &internal.RangeRequest{Key: k})
		require.Nil(t, err, err)
		assert.Equal(t, 0, len(resp.Kvs))
		resp2, err := svcKv.DeleteRange(ctx, &internal.DeleteRangeRequest{
			Key: k,
		})
		require.Nil(t, err, err)
		assert.EqualValues(t, 0, resp2.Deleted)
	})
}

func testCompact(t *testing.T) {
	var revs []int64
	for i := range 10 {
		resp, err := svcKv.Put(ctx, &internal.PutRequest{
			Key:   []byte(fmt.Sprintf(`test-key-compact-%02d`, i)),
			Value: []byte(`-------------------`),
		})
		require.Nil(t, err, err)
		revs = append(revs, resp.Header.Revision)
	}
	resp, err := svcKv.Range(ctx, &internal.RangeRequest{
		Key:      []byte(`test-key-compact-00`),
		RangeEnd: []byte(`test-key-compact-10`),
		Revision: revs[1],
	})
	require.Nil(t, err, err)
	assert.Equal(t, 2, len(resp.Kvs))
	for i := range 10 {
		resp, err := svcKv.DeleteRange(ctx, &internal.DeleteRangeRequest{
			Key: []byte(fmt.Sprintf(`test-key-compact-%02d`, i)),
		})
		require.Nil(t, err, err)
		revs = append(revs, resp.Header.Revision)
	}
	resp, err = svcKv.Range(ctx, &internal.RangeRequest{
		Key:      []byte(`test-key-compact-00`),
		RangeEnd: []byte(`test-key-compact-10`),
	})
	require.Nil(t, err, err)
	assert.Equal(t, 0, len(resp.Kvs))
	resp, err = svcKv.Range(ctx, &internal.RangeRequest{
		Key:      []byte(`test-key-compact-00`),
		RangeEnd: []byte(`test-key-compact-10`),
		Revision: revs[10],
	})
	require.Nil(t, err, err)
	assert.Equal(t, 9, len(resp.Kvs))
	_, err = svcKv.Compact(ctx, &internal.CompactionRequest{
		Revision: revs[10],
	})
	require.Nil(t, err, err)
	resp2, err := svcKv.Range(ctx, &internal.RangeRequest{
		Key:      []byte(`test-key-compact-00`),
		RangeEnd: []byte(`test-key-compact-10`),
		Revision: revs[10],
	})
	require.Nil(t, err, err)
	assert.Equal(t, 9, len(resp2.Kvs))
	resp2, err = svcKv.Range(ctx, &internal.RangeRequest{
		Key:      []byte(`test-key-compact-00`),
		RangeEnd: []byte(`test-key-compact-10`),
		Revision: revs[9],
	})
	require.NotNil(t, err, err)
	resp2, err = svcKv.Range(ctx, &internal.RangeRequest{
		Key:      []byte(`test-key-compact-00`),
		RangeEnd: []byte(`test-key-compact-10`),
		Revision: 1e10,
	})
	require.NotNil(t, err, err)
}

func testTransaction(t *testing.T) {
	t.Run("basic", func(t *testing.T) {
		_, err = svcKv.Put(ctx, &internal.PutRequest{
			Key:   []byte(`test-txn-00`),
			Value: []byte(`-----------`),
		})
		require.Nil(t, err, err)
		req := &internal.TxnRequest{
			Compare: []*internal.Compare{
				{
					Key:    []byte(`test-txn-00`),
					Result: internal.Compare_EQUAL,
					Target: internal.Compare_VALUE,
					TargetUnion: &internal.Compare_Value{
						Value: []byte(`-----------`),
					},
				},
			},
			Success: []*internal.RequestOp{
				putOp(&internal.PutRequest{
					Key:   []byte(`test-txn-01`),
					Value: []byte(`-----------`),
				}),
				putOp(&internal.PutRequest{
					Key:   []byte(`test-txn-02`),
					Value: []byte(`-----------`),
				}),
				delOp(&internal.DeleteRangeRequest{
					Key: []byte(`test-txn-00`),
				}),
				rangeOp(&internal.RangeRequest{
					Key:      []byte(`test-txn-00`),
					RangeEnd: []byte(`test-txn-10`),
				}),
			},
			Failure: []*internal.RequestOp{
				putOp(&internal.PutRequest{
					Key:   []byte(`test-txn-00`),
					Value: []byte(`-----------`),
				}),
				rangeOp(&internal.RangeRequest{
					Key:      []byte(`test-txn-00`),
					RangeEnd: []byte(`test-txn-10`),
				}),
			},
		}
		resp, err := svcKv.Txn(ctx, req)
		require.Nil(t, err, err)
		assert.True(t, resp.Succeeded)
		assert.Len(t, resp.Responses, len(req.Success))
		assert.Len(t, resp.Responses[3].Response.(*internal.ResponseOp_ResponseRange).ResponseRange.Kvs, 2)
		resp, err = svcKv.Txn(ctx, req)
		require.Nil(t, err, err)
		assert.False(t, resp.Succeeded)
		assert.Len(t, resp.Responses, len(req.Failure))
		assert.Len(t, resp.Responses[1].Response.(*internal.ResponseOp_ResponseRange).ResponseRange.Kvs, 3)
	})
	leaseResp, err := svcLease.LeaseGrant(ctx, &internal.LeaseGrantRequest{TTL: 600})
	require.Nil(t, err)
	resp, err := svcKv.Put(ctx, &internal.PutRequest{
		Key:   []byte(`test-txn-00`),
		Value: []byte(`b`),
		Lease: leaseResp.ID,
	})
	require.Nil(t, err, err)
	rev := resp.Header.Revision
	resp2, err := svcKv.Range(ctx, &internal.RangeRequest{
		Key: []byte(`test-txn-00`),
	})
	require.Nil(t, err, err)
	assert.Equal(t, 1, len(resp2.Kvs))
	item := resp2.Kvs[0]
	valueCompare := func(result internal.Compare_CompareResult, b []byte) (*internal.TxnResponse, error) {
		return svcKv.Txn(ctx, &internal.TxnRequest{
			Compare: []*internal.Compare{
				{
					Key:    []byte(`test-txn-00`),
					Result: result,
					Target: internal.Compare_VALUE,
					TargetUnion: &internal.Compare_Value{
						Value: b,
					},
				},
			},
		})
	}
	t.Run("value", func(t *testing.T) {
		resp3, err := valueCompare(internal.Compare_EQUAL, []byte(`b`))
		require.Nil(t, err, err)
		require.True(t, resp3.Succeeded)
		resp3, err = valueCompare(internal.Compare_EQUAL, []byte(`c`))
		require.Nil(t, err, err)
		require.False(t, resp3.Succeeded)
		resp3, err = valueCompare(internal.Compare_GREATER, []byte(`a`))
		require.Nil(t, err, err)
		require.True(t, resp3.Succeeded)
		resp3, err = valueCompare(internal.Compare_GREATER, []byte(`b`))
		require.Nil(t, err, err)
		require.False(t, resp3.Succeeded)
		resp3, err = valueCompare(internal.Compare_GREATER, []byte(`c`))
		require.Nil(t, err, err)
		require.False(t, resp3.Succeeded)
		resp3, err = valueCompare(internal.Compare_LESS, []byte(`c`))
		require.Nil(t, err, err)
		require.True(t, resp3.Succeeded)
		resp3, err = valueCompare(internal.Compare_LESS, []byte(`b`))
		require.Nil(t, err, err)
		require.False(t, resp3.Succeeded)
		resp3, err = valueCompare(internal.Compare_LESS, []byte(`a`))
		require.Nil(t, err, err)
		require.False(t, resp3.Succeeded)
		resp3, err = valueCompare(internal.Compare_NOT_EQUAL, []byte(`a`))
		require.Nil(t, err, err)
		require.True(t, resp3.Succeeded)
		resp3, err = valueCompare(internal.Compare_NOT_EQUAL, []byte(`b`))
		require.Nil(t, err, err)
		require.False(t, resp3.Succeeded)
	})
	intCompare := func(fn func(result internal.Compare_CompareResult, val int64) (*internal.TxnResponse, error), val int64) {
		resp3, err := fn(internal.Compare_EQUAL, val)
		require.Nil(t, err, err)
		require.True(t, resp3.Succeeded)
		resp3, err = fn(internal.Compare_EQUAL, 0)
		require.Nil(t, err, err)
		require.False(t, resp3.Succeeded)
		resp3, err = fn(internal.Compare_GREATER, 0)
		require.Nil(t, err, err)
		require.True(t, resp3.Succeeded)
		resp3, err = fn(internal.Compare_GREATER, val)
		require.Nil(t, err, err)
		require.False(t, resp3.Succeeded)
		resp3, err = fn(internal.Compare_GREATER, val+1)
		require.Nil(t, err, err)
		require.False(t, resp3.Succeeded)
		resp3, err = fn(internal.Compare_LESS, val+1)
		require.Nil(t, err, err)
		require.True(t, resp3.Succeeded)
		resp3, err = fn(internal.Compare_LESS, val)
		require.Nil(t, err, err)
		require.False(t, resp3.Succeeded)
		resp3, err = fn(internal.Compare_LESS, 0)
		require.Nil(t, err, err)
		require.False(t, resp3.Succeeded)
		resp3, err = fn(internal.Compare_NOT_EQUAL, 0)
		require.Nil(t, err, err)
		require.True(t, resp3.Succeeded)
		resp3, err = fn(internal.Compare_NOT_EQUAL, val)
		require.Nil(t, err, err)
		require.False(t, resp3.Succeeded)
	}
	t.Run("version", func(t *testing.T) {
		intCompare(func(result internal.Compare_CompareResult, val int64) (*internal.TxnResponse, error) {
			return svcKv.Txn(ctx, &internal.TxnRequest{
				Compare: []*internal.Compare{
					{
						Key:    []byte(`test-txn-00`),
						Result: result,
						Target: internal.Compare_VERSION,
						TargetUnion: &internal.Compare_Version{
							Version: val,
						},
					},
				},
			})
		}, int64(item.Version))
	})
	t.Run("revision", func(t *testing.T) {
		intCompare(func(result internal.Compare_CompareResult, val int64) (*internal.TxnResponse, error) {
			return svcKv.Txn(ctx, &internal.TxnRequest{
				Compare: []*internal.Compare{
					{
						Key:    []byte(`test-txn-00`),
						Result: result,
						Target: internal.Compare_MOD,
						TargetUnion: &internal.Compare_ModRevision{
							ModRevision: val,
						},
					},
				},
			})
		}, rev)
	})
	t.Run("created", func(t *testing.T) {
		intCompare(func(result internal.Compare_CompareResult, val int64) (*internal.TxnResponse, error) {
			return svcKv.Txn(ctx, &internal.TxnRequest{
				Compare: []*internal.Compare{
					{
						Key:    []byte(`test-txn-00`),
						Result: result,
						Target: internal.Compare_CREATE,
						TargetUnion: &internal.Compare_CreateRevision{
							CreateRevision: val,
						},
					},
				},
			})
		}, item.CreateRevision)
	})
	t.Run("lease", func(t *testing.T) {
		intCompare(func(result internal.Compare_CompareResult, val int64) (*internal.TxnResponse, error) {
			return svcKv.Txn(ctx, &internal.TxnRequest{
				Compare: []*internal.Compare{
					{
						Key:    []byte(`test-txn-00`),
						Result: result,
						Target: internal.Compare_LEASE,
						TargetUnion: &internal.Compare_Lease{
							Lease: val,
						},
					},
				},
			})
		}, item.Lease)
	})
	t.Run("no-compare", func(t *testing.T) {
		resp, err := svcKv.Txn(ctx, &internal.TxnRequest{
			Success: []*internal.RequestOp{
				putOp(&internal.PutRequest{
					Key:   []byte(`test-txn-03`),
					Value: []byte(`-----------`),
				}),
			},
		})
		require.Nil(t, err, err)
		assert.True(t, resp.Succeeded)
	})
	t.Run("multi-write", func(t *testing.T) {
		withGlobal(&ICARUS_TXN_MULTI_WRITE_ENABLED, false, func() {
			resp, err := svcKv.Txn(ctx, &internal.TxnRequest{
				Success: []*internal.RequestOp{
					putOp(&internal.PutRequest{
						Key:   []byte(`test-txn-03`),
						Value: []byte(`-----------`),
					}),
					putOp(&internal.PutRequest{
						Key:   []byte(`test-txn-03`),
						Value: []byte(`-----------`),
					}),
				},
			})
			require.NotNil(t, err, err)
			assert.Nil(t, resp)
			resp, err = svcKv.Txn(ctx, &internal.TxnRequest{
				Success: []*internal.RequestOp{
					putOp(&internal.PutRequest{
						Key:   []byte(`test-txn-03`),
						Value: []byte(`-----------`),
					}),
					delOp(&internal.DeleteRangeRequest{
						Key: []byte(`test-txn-03`),
					}),
				},
			})
			require.NotNil(t, err, err)
			assert.Nil(t, resp)
		})
		withGlobal(&ICARUS_TXN_MULTI_WRITE_ENABLED, true, func() {
			if parity {
				return
			}
			for _, k := range []string{
				`test-txn-03`, // Existent
				`test-txn-04`, // Non-existent
			} {
				resp, err := svcKv.Txn(ctx, &internal.TxnRequest{
					Success: []*internal.RequestOp{
						putOp(&internal.PutRequest{
							Key:   []byte(k),
							Value: []byte(`a`),
						}),
						putOp(&internal.PutRequest{
							Key:   []byte(k),
							Value: []byte(`b`),
						}),
					},
				})
				require.Nil(t, err, err)
				assert.Len(t, resp.Responses, 2)
				resp2, err := svcKv.Range(ctx, &internal.RangeRequest{
					Key: []byte(k),
				})
				require.Nil(t, err, err)
				assert.Equal(t, 1, len(resp2.Kvs))
				assert.Equal(t, []byte(`b`), resp2.Kvs[0].Value)
			}
			resp, err := svcKv.Txn(ctx, &internal.TxnRequest{
				Success: []*internal.RequestOp{
					putOp(&internal.PutRequest{
						Key:   []byte(`test-txn-03`),
						Value: []byte(`a`),
					}),
					delOp(&internal.DeleteRangeRequest{
						Key: []byte(`test-txn-03`),
					}),
				},
			})
			require.Nil(t, err, err)
			assert.Len(t, resp.Responses, 2)
			resp2, err := svcKv.Range(ctx, &internal.RangeRequest{
				Key: []byte(`test-txn-03`),
			})
			require.Nil(t, err, err)
			assert.Equal(t, 0, len(resp2.Kvs))
		})
	})
}

func testLeaseGrant(t *testing.T) {
	var id int64
	t.Run("success", func(t *testing.T) {
		resp, err := svcLease.LeaseGrant(ctx, &internal.LeaseGrantRequest{
			TTL: 600,
		})
		require.Nil(t, err, err)
		assert.NotNil(t, resp)
		assert.Empty(t, resp.Error)
		assert.EqualValues(t, 600, resp.TTL)
		assert.Greater(t, resp.ID, int64(0))
		id = resp.ID
	})
	t.Run("failure", func(t *testing.T) {
		resp, err := svcLease.LeaseGrant(ctx, &internal.LeaseGrantRequest{
			ID:  id,
			TTL: 600,
		})
		require.NotNil(t, err, err)
		assert.Nil(t, resp)
	})
	t.Run("custom", func(t *testing.T) {
		resp, err := svcLease.LeaseGrant(ctx, &internal.LeaseGrantRequest{
			ID:  3,
			TTL: 600,
		})
		require.Nil(t, err, err)
		require.NotNil(t, resp)
		assert.Equal(t, int64(3), resp.ID)
		resp, err = svcLease.LeaseGrant(ctx, &internal.LeaseGrantRequest{
			TTL: 600,
		})
		require.Nil(t, err, err)
		require.NotNil(t, resp)
		assert.Greater(t, resp.ID, int64(3))
	})
	t.Run("put-missing", func(t *testing.T) {
		_, err := svcKv.Put(ctx, &internal.PutRequest{
			Key:   []byte(`test-no-lease-00`),
			Value: []byte(`test-no-lease-value-00`),
			Lease: 1e10,
		})
		require.NotNil(t, err, err)
	})
}

func testLeaseRevoke(t *testing.T) {
	resp, err := svcLease.LeaseGrant(ctx, &internal.LeaseGrantRequest{
		TTL: 600,
	})
	require.Nil(t, err, err)
	var id = resp.ID
	t.Run("success", func(t *testing.T) {
		resp, err := svcLease.LeaseRevoke(ctx, &internal.LeaseRevokeRequest{
			ID: id,
		})
		require.Nil(t, err, err)
		assert.NotNil(t, resp)
	})
	t.Run("failure", func(t *testing.T) {
		resp, err := svcLease.LeaseRevoke(ctx, &internal.LeaseRevokeRequest{
			ID: id,
		})
		require.NotNil(t, err, err)
		assert.Nil(t, resp)
	})
	resp, err = svcLease.LeaseGrant(ctx, &internal.LeaseGrantRequest{
		TTL: 600,
	})
	require.Nil(t, err, err)
	id = resp.ID
	var rev int64
	t.Run("keys", func(t *testing.T) {
		resp2, err := svcKv.Put(ctx, &internal.PutRequest{
			Key:   []byte(`test-lease-revoke-00`),
			Value: []byte(`test-lease-revoke-value-00`),
			Lease: id,
		})
		require.Nil(t, err, err)
		rev = resp2.Header.Revision
		t.Run("added", func(t *testing.T) {
			resp, err := svcKv.Range(ctx, &internal.RangeRequest{
				Key: []byte(`test-lease-revoke-00`),
			})
			require.Nil(t, err, err)
			assert.NotNil(t, resp)
			require.Equal(t, 1, len(resp.Kvs))
		})
		t.Run("ignore-lease", func(t *testing.T) {
			_, err = svcKv.Put(ctx, &internal.PutRequest{
				Key:         []byte(`test-lease-revoke-00`),
				Value:       []byte(`test-lease-revoke-value-01`),
				Lease:       54321,
				IgnoreLease: true,
			})
			require.NotNil(t, err, err)
			_, err = svcKv.Put(ctx, &internal.PutRequest{
				Key:         []byte(`test-lease-revoke-00`),
				Value:       []byte(`test-lease-revoke-value-02`),
				IgnoreLease: true,
			})
			require.Nil(t, err, err)
			resp, err := svcKv.Range(ctx, &internal.RangeRequest{
				Key: []byte(`test-lease-revoke-00`),
			})
			require.Nil(t, err, err)
			assert.NotNil(t, resp)
			require.Equal(t, 1, len(resp.Kvs))
			assert.Equal(t, id, resp.Kvs[0].Lease)
			assert.Equal(t, []byte(`test-lease-revoke-value-02`), resp.Kvs[0].Value)
		})
		_, err = svcLease.LeaseGrant(ctx, &internal.LeaseGrantRequest{
			ID:  54321,
			TTL: 600,
		})
		require.Nil(t, err, err)
		t.Run("overwrite-lease", func(t *testing.T) {
			_, err = svcKv.Put(ctx, &internal.PutRequest{
				Key:   []byte(`test-lease-revoke-01`),
				Value: []byte(`test-lease-revoke-value-01`),
				Lease: id,
			})
			require.Nil(t, err, err)
			resp, err := svcKv.Range(ctx, &internal.RangeRequest{
				Key: []byte(`test-lease-revoke-01`),
			})
			require.Nil(t, err, err)
			require.NotNil(t, resp)
			require.Equal(t, 1, len(resp.Kvs))
			assert.Equal(t, id, resp.Kvs[0].Lease)
			_, err = svcKv.Put(ctx, &internal.PutRequest{
				Key:   []byte(`test-lease-revoke-01`),
				Value: []byte(`test-lease-revoke-value-02`),
				Lease: 54321,
			})
			require.Nil(t, err, err)
			resp, err = svcKv.Range(ctx, &internal.RangeRequest{
				Key: []byte(`test-lease-revoke-01`),
			})
			require.Nil(t, err, err)
			require.NotNil(t, resp)
			require.Equal(t, 1, len(resp.Kvs))
			assert.EqualValues(t, 54321, resp.Kvs[0].Lease)
		})
		t.Run("removed", func(t *testing.T) {
			resp, err := svcKv.Range(ctx, &internal.RangeRequest{
				Key: []byte(`test-lease-revoke-00`),
			})
			require.Nil(t, err, err)
			assert.NotNil(t, resp)
			require.Equal(t, 1, len(resp.Kvs))
			require.EqualValues(t, 1, resp.Count)
			resp2, err := svcLease.LeaseRevoke(ctx, &internal.LeaseRevokeRequest{
				ID: id,
			})
			require.Nil(t, err, err)
			assert.NotNil(t, resp2)
			resp, err = svcKv.Range(ctx, &internal.RangeRequest{
				Key: []byte(`test-lease-revoke-00`),
			})
			require.Nil(t, err, err)
			assert.NotNil(t, resp)
			require.Equal(t, 0, len(resp.Kvs))
			require.EqualValues(t, 0, resp.Count)
			resp, err = svcKv.Range(ctx, &internal.RangeRequest{
				Key: []byte(`test-lease-revoke-01`),
			})
			require.Nil(t, err, err)
			assert.NotNil(t, resp)
			require.Equal(t, 1, len(resp.Kvs))
			require.EqualValues(t, 1, resp.Count)
		})
		t.Run("still-visible-at-revision", func(t *testing.T) {
			resp, err := svcKv.Range(ctx, &internal.RangeRequest{
				Key:      []byte(`test-lease-revoke-00`),
				Revision: rev,
			})
			require.Nil(t, err, err)
			assert.NotNil(t, resp)
			require.Equal(t, 1, len(resp.Kvs))
		})
		resp, err = svcLease.LeaseGrant(ctx, &internal.LeaseGrantRequest{
			TTL: 600,
		})
		require.Nil(t, err, err)
		newLease := resp.ID
		t.Run("delete-overwrite-remove", func(t *testing.T) {
			_, err = svcKv.Put(ctx, &internal.PutRequest{
				Key:   []byte(`test-lease-revoke-02`),
				Value: []byte(`test-lease-revoke-value-00`),
				Lease: newLease,
			})
			require.Nil(t, err, err)
			delResp, err := svcKv.DeleteRange(ctx, &internal.DeleteRangeRequest{
				Key:    []byte(`test-lease-revoke-02`),
				PrevKv: true,
			})
			require.Nil(t, err, err)
			require.NotNil(t, delResp)
			require.Equal(t, 1, len(delResp.PrevKvs))
			resp, err := svcKv.Range(ctx, &internal.RangeRequest{
				Key: []byte(`test-lease-revoke-02`),
			})
			require.Nil(t, err, err)
			require.NotNil(t, resp)
			require.Equal(t, 0, len(resp.Kvs))
			_, err = svcKv.Put(ctx, &internal.PutRequest{
				Key:   []byte(`test-lease-revoke-02`),
				Value: []byte(`test-lease-revoke-value-01`),
				Lease: 54321,
			})
			_, err = svcLease.LeaseRevoke(ctx, &internal.LeaseRevokeRequest{
				ID: newLease,
			})
			require.Nil(t, err, err)
			resp, err = svcKv.Range(ctx, &internal.RangeRequest{
				Key: []byte(`test-lease-revoke-02`),
			})
			require.Nil(t, err, err)
			require.NotNil(t, resp)
			require.Equal(t, 1, len(resp.Kvs))
		})
	})
}

func testLeaseKeepAlive(t *testing.T) {
	var lease1 int64
	t.Run("success", func(t *testing.T) {
		resp, err := svcLease.LeaseGrant(ctx, &internal.LeaseGrantRequest{
			TTL: 600,
		})
		require.Nil(t, err, err)
		assert.NotNil(t, resp)
		assert.Empty(t, resp.Error)
		assert.EqualValues(t, 600, resp.TTL)
		assert.Greater(t, resp.ID, int64(0))
		lease1 = resp.ID
		s := &mockLeaseKeepAliveServer{
			req: &internal.LeaseKeepAliveRequest{ID: lease1},
		}
		err = svcLease.LeaseKeepAlive(s)
		require.Nil(t, err, err)
		require.NotNil(t, s.res)
		require.EqualValues(t, 600, s.res.TTL)
	})
	t.Run("failure", func(t *testing.T) {
		s := &mockLeaseKeepAliveServer{
			req: &internal.LeaseKeepAliveRequest{ID: 1e10},
		}
		err = svcLease.LeaseKeepAlive(s)
		require.Nil(t, err, err)
		require.EqualValues(t, 1e10, s.res.ID)
		require.EqualValues(t, 0, s.res.TTL)
	})
}

func testLeaseLeases(t *testing.T) {
	t.Run("success", func(t *testing.T) {
		resp, err := svcLease.LeaseLeases(ctx, &internal.LeaseLeasesRequest{})
		require.Nil(t, err, err)
		require.NotNil(t, resp)
		assert.Greater(t, len(resp.Leases), 0)
	})
}

func testLeaseTimeToLive(t *testing.T) {
	t.Run("success", func(t *testing.T) {
		resp, err := svcLease.LeaseGrant(ctx, &internal.LeaseGrantRequest{
			TTL: 600,
		})
		require.Nil(t, err, err)
		resp2, err := svcLease.LeaseTimeToLive(ctx, &internal.LeaseTimeToLiveRequest{
			ID: resp.ID,
		})
		require.Nil(t, err, err)
		require.NotNil(t, resp2)
		assert.Greater(t, resp2.TTL, int64(598))
	})
	t.Run("failure", func(t *testing.T) {
		resp2, err := svcLease.LeaseTimeToLive(ctx, &internal.LeaseTimeToLiveRequest{
			ID: 1e10,
		})
		require.Nil(t, err, err)
		require.NotNil(t, resp2)
		assert.EqualValues(t, -1, resp2.TTL)
	})
}

func testWatch(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	s := newMockWatchServer(ctx)
	done := make(chan bool)
	go func() {
		err = svcWatch.Watch(s)
		require.Nil(t, err, err)
		close(done)
	}()
	sendWatchCancel := func(id int64) {
		s.reqChan <- &internal.WatchRequest{
			RequestUnion: &internal.WatchRequest_CancelRequest{
				CancelRequest: &internal.WatchCancelRequest{
					WatchId: id,
				},
			},
		}
	}
	sendWatchCreate := func(req *internal.WatchCreateRequest) {
		s.reqChan <- &internal.WatchRequest{
			RequestUnion: &internal.WatchRequest_CreateRequest{
				CreateRequest: req,
			},
		}
	}
	var watchID int64
	t.Run("create", func(t *testing.T) {
		sendWatchCreate(&internal.WatchCreateRequest{
			Key:      []byte(`test-watch-000`),
			RangeEnd: []byte(`test-watch-100`),
		})
		res := <-s.resChan
		require.Equal(t, int64(0), res.WatchId, res)
		assert.True(t, res.Created)
		assert.False(t, res.Canceled)
		assert.Len(t, res.Events, 0)
		assert.EqualValues(t, 0, res.CompactRevision)
		watchID = res.WatchId
	})
	t.Run("receive", func(t *testing.T) {
		t.Run("put", func(t *testing.T) {
			req := &internal.PutRequest{
				Key:   []byte(`test-watch-000`),
				Value: []byte(`test-watch-value-000`),
			}
			_, err = svcKv.Put(ctx, req)
			require.Nil(t, err, err)
			res := <-s.resChan
			assert.Equal(t, watchID, res.WatchId, res)
			assert.False(t, res.Created)
			assert.False(t, res.Canceled)
			require.Len(t, res.Events, 1)
			assert.Equal(t, internal.Event_PUT, res.Events[0].Type)
			assert.Nil(t, res.Events[0].PrevKv)
			require.NotNil(t, req.Key, res.Events[0].Kv)
			assert.Equal(t, req.Key, res.Events[0].Kv.Key)
			assert.Equal(t, res.Header.Revision, res.Events[0].Kv.ModRevision)
		})
	})
	t.Run("cancel", func(t *testing.T) {
		sendWatchCancel(watchID)
		res := <-s.resChan
		require.Equal(t, watchID, res.WatchId)
		require.False(t, res.Created)
		require.True(t, res.Canceled)
	})
	t.Run("multiple", func(t *testing.T) {
		sendWatchCreate(&internal.WatchCreateRequest{
			Key:      []byte(`test-watch-100`),
			RangeEnd: []byte(`test-watch-200`),
			WatchId:  100,
		})
		res := <-s.resChan
		require.Equal(t, int64(100), res.WatchId, res)
		assert.True(t, res.Created)
		assert.False(t, res.Canceled)
		assert.Len(t, res.Events, 0)
		assert.EqualValues(t, 0, res.CompactRevision)
		watchID1 := res.WatchId
		sendWatchCreate(&internal.WatchCreateRequest{
			Key:      []byte(`test-watch-100`),
			RangeEnd: []byte(`test-watch-200`),
			WatchId:  101,
		})
		res = <-s.resChan
		require.Equal(t, int64(101), res.WatchId, res)
		assert.True(t, res.Created)
		watchID2 := res.WatchId
		t.Run("put", func(t *testing.T) {
			req := &internal.PutRequest{
				Key:   []byte(`test-watch-100`),
				Value: []byte(`test-watch-value-000`),
			}
			_, err = svcKv.Put(ctx, req)
			require.Nil(t, err, err)
			for range 2 {
				res := <-s.resChan
				assert.True(t, res.WatchId == watchID1 || res.WatchId == watchID2, res.WatchId)
				assert.False(t, res.Created)
				assert.False(t, res.Canceled)
				require.Len(t, res.Events, 1)
				assert.Equal(t, internal.Event_PUT, res.Events[0].Type)
				assert.Nil(t, res.Events[0].PrevKv)
				require.NotNil(t, req.Key, res.Events[0].Kv)
				assert.Equal(t, req.Key, res.Events[0].Kv.Key)
			}
		})
		t.Run("cancel", func(t *testing.T) {
			sendWatchCancel(watchID1)
			res := <-s.resChan
			require.Equal(t, watchID1, res.WatchId)
			require.False(t, res.Created)
			require.True(t, res.Canceled)
			sendWatchCancel(watchID2)
			res = <-s.resChan
			require.Equal(t, watchID2, res.WatchId)
			require.False(t, res.Created)
			require.True(t, res.Canceled)
		})
	})
	t.Run("filter", func(t *testing.T) {
		t.Run("noput", func(t *testing.T) {
			sendWatchCreate(&internal.WatchCreateRequest{
				Key:      []byte(`test-watch-000`),
				RangeEnd: []byte(`test-watch-100`),
				Filters: []internal.WatchCreateRequest_FilterType{
					internal.WatchCreateRequest_NOPUT,
				},
			})
			res := <-s.resChan // WatchCreated
			require.Greater(t, res.WatchId, int64(0), res)
			assert.True(t, res.Created)
			watchID = res.WatchId
			req := &internal.PutRequest{
				Key:   []byte(`test-watch-001`),
				Value: []byte(`test-watch-value-001`),
			}
			_, err = svcKv.Put(ctx, req)
			require.Nil(t, err, err)
			_, err = svcKv.DeleteRange(ctx, &internal.DeleteRangeRequest{
				Key: []byte(`test-watch-001`),
			})
			require.Nil(t, err, err)
			res = <-s.resChan // Delete
			assert.Equal(t, watchID, res.WatchId, res)
			assert.False(t, res.Created)
			assert.False(t, res.Canceled)
			require.Len(t, res.Events, 1)
			assert.Equal(t, internal.Event_DELETE, res.Events[0].Type, res)
			sendWatchCancel(watchID)
			res = <-s.resChan
			require.Equal(t, watchID, res.WatchId)
			require.False(t, res.Created)
			require.True(t, res.Canceled)
		})
		t.Run("nodelete", func(t *testing.T) {
			sendWatchCreate(&internal.WatchCreateRequest{
				Key:      []byte(`test-watch-200`),
				RangeEnd: []byte(`test-watch-300`),
				Filters: []internal.WatchCreateRequest_FilterType{
					internal.WatchCreateRequest_NODELETE,
				},
			})
			res := <-s.resChan // WatchCreated
			require.Greater(t, res.WatchId, int64(0), res)
			assert.True(t, res.Created)
			watchID = res.WatchId
			req := &internal.PutRequest{
				Key:   []byte(`test-watch-200`),
				Value: []byte(`test-watch-value-000`),
			}
			_, err = svcKv.Put(ctx, req)
			require.Nil(t, err, err)
			res = <-s.resChan // Put
			assert.Equal(t, watchID, res.WatchId, res)
			assert.False(t, res.Created)
			assert.False(t, res.Canceled)
			require.Len(t, res.Events, 1)
			assert.Equal(t, internal.Event_PUT, res.Events[0].Type, res)
			_, err = svcKv.DeleteRange(ctx, &internal.DeleteRangeRequest{
				Key: []byte(`test-watch-200`),
			})
			require.Nil(t, err, err)
			// No delete message received
			var ok bool
			select {
			case msg := <-s.resChan: // Delete (not received)
				t.Logf("Received: %#v", msg)
				t.Logf("Event: %#v", msg.Events[0])
				ok = false
			case <-time.After(100 * time.Millisecond):
				ok = true
			}
			assert.True(t, ok)
			sendWatchCancel(watchID)
			res = <-s.resChan
			require.Equal(t, watchID, res.WatchId)
			require.False(t, res.Created)
			require.True(t, res.Canceled)
		})
	})
	t.Run("alert", func(t *testing.T) {
		sendWatchCreate(&internal.WatchCreateRequest{
			Key:      []byte(`test-watch-alert-000`),
			RangeEnd: []byte(`test-watch-alert-999`),
			Filters: []internal.WatchCreateRequest_FilterType{
				internal.WatchCreateRequest_NODELETE,
			},
		})
		res := <-s.resChan // WatchCreated
		require.Greater(t, res.WatchId, int64(0), res)
		assert.True(t, res.Created)
		watchID = res.WatchId
		for i := range 10 {
			req := &internal.PutRequest{
				Key:   []byte(fmt.Sprintf(`test-watch-alert-%03d`, i)),
				Value: []byte(fmt.Sprintf(`test-watch-alert-value-%03d`, i)),
			}
			_, err = svcKv.Put(ctx, req)
			require.Nil(t, err, err)
		}
		for j := 0; j < 10; {
			res = <-s.resChan
			assert.True(t, res.WatchId == watchID, res.WatchId)
			assert.False(t, res.Created)
			assert.False(t, res.Canceled)
			require.Greater(t, len(res.Events), 0, res)
			// t.Log("---", j, i, res.Events[i])
			for i := range res.Events {
				assert.Equal(t, internal.Event_PUT, res.Events[i].Type)
				assert.Nil(t, res.Events[i].PrevKv)
				assert.Equal(t, fmt.Sprintf(`test-watch-alert-%03d`, j), string(res.Events[i].Kv.Key))
				j++
			}
		}
		sendWatchCancel(watchID)
		res = <-s.resChan
		require.Equal(t, watchID, res.WatchId)
		require.False(t, res.Created)
		require.True(t, res.Canceled)
	})
	t.Run("revision", func(t *testing.T) {
		var revs []int64
		for i := range 10 {
			req := &internal.PutRequest{
				Key:   []byte(fmt.Sprintf(`test-watch-rev-%03d`, i)),
				Value: []byte(fmt.Sprintf(`test-watch-rev-value-%03d`, i)),
			}
			resp, err := svcKv.Put(ctx, req)
			require.Nil(t, err, err)
			revs = append(revs, resp.Header.Revision)
		}
		sendWatchCreate(&internal.WatchCreateRequest{
			Key:           []byte(`test-watch-rev-000`),
			RangeEnd:      []byte(`test-watch-rev-999`),
			StartRevision: revs[5],
		})
		res := <-s.resChan // WatchCreated
		require.Greater(t, res.WatchId, int64(0), res)
		assert.True(t, res.Created)
		watchID = res.WatchId

		for j := 0; j < 5; {
			res := <-s.resChan
			assert.True(t, res.WatchId == watchID, res.WatchId)
			assert.False(t, res.Created)
			assert.False(t, res.Canceled)
			require.Greater(t, len(res.Events), 0)
			assert.Equal(t, res.Header.Revision, res.Events[len(res.Events)-1].Kv.ModRevision)
			assert.Equal(t, res.Header.Revision, res.Events[len(res.Events)-1].Kv.CreateRevision)
			for i := range res.Events {
				assert.Equal(t, internal.Event_PUT, res.Events[i].Type)
				assert.Nil(t, res.Events[i].PrevKv)
				assert.Equal(t, fmt.Sprintf(`test-watch-rev-%03d`, j+5), string(res.Events[i].Kv.Key))
				j++
			}
		}
		sendWatchCancel(watchID)
		res = <-s.resChan
		require.Equal(t, watchID, res.WatchId)
		require.False(t, res.Created)
		require.True(t, res.Canceled)
	})
	t.Run("progress", func(t *testing.T) {
		// Connection level progress notifications return watchId: -1 and a single cursor when all watches are "synced"
	})
	cancel()
	assert.True(t, await(1, 100, func() bool {
		<-done
		return true
	}))
}

func delOp(req *internal.DeleteRangeRequest) *internal.RequestOp {
	return &internal.RequestOp{
		Request: &internal.RequestOp_RequestDeleteRange{
			RequestDeleteRange: req,
		},
	}
}

func putOp(req *internal.PutRequest) *internal.RequestOp {
	return &internal.RequestOp{
		Request: &internal.RequestOp_RequestPut{
			RequestPut: req,
		},
	}
}

func rangeOp(req *internal.RangeRequest) *internal.RequestOp {
	return &internal.RequestOp{
		Request: &internal.RequestOp_RequestRange{
			RequestRange: req,
		},
	}
}

func await(d, n time.Duration, fn func() bool) bool {
	for i := 0; i < int(n); i++ {
		if fn() {
			return true
		}
		time.Sleep(d * time.Second / n)
	}
	return false
}

type parityKvService struct {
	internal.UnimplementedKVServer

	client internal.KVClient
}

func newParityKvService(conn grpc.ClientConnInterface) internal.KVServer {
	return &parityKvService{client: internal.NewKVClient(conn)}
}
func (svc parityKvService) Range(ctx context.Context, req *internal.RangeRequest) (*internal.RangeResponse, error) {
	return svc.client.Range(ctx, req)
}
func (svc parityKvService) Put(ctx context.Context, req *internal.PutRequest) (*internal.PutResponse, error) {
	return svc.client.Put(ctx, req)
}
func (svc parityKvService) DeleteRange(ctx context.Context, req *internal.DeleteRangeRequest) (*internal.DeleteRangeResponse, error) {
	return svc.client.DeleteRange(ctx, req)
}
func (svc parityKvService) Txn(ctx context.Context, req *internal.TxnRequest) (*internal.TxnResponse, error) {
	return svc.client.Txn(ctx, req)
}
func (svc parityKvService) Compact(ctx context.Context, req *internal.CompactionRequest) (*internal.CompactionResponse, error) {
	return svc.client.Compact(ctx, req)
}

type parityLeaseService struct {
	internal.UnimplementedLeaseServer

	client internal.LeaseClient
}

func newParityLeaseService(conn grpc.ClientConnInterface) internal.LeaseServer {
	return &parityLeaseService{client: internal.NewLeaseClient(conn)}
}

func (svc parityLeaseService) LeaseGrant(ctx context.Context, req *internal.LeaseGrantRequest) (*internal.LeaseGrantResponse, error) {
	return svc.client.LeaseGrant(ctx, req)
}

func (svc parityLeaseService) LeaseRevoke(ctx context.Context, req *internal.LeaseRevokeRequest) (*internal.LeaseRevokeResponse, error) {
	return svc.client.LeaseRevoke(ctx, req)
}

func (svc parityLeaseService) LeaseKeepAlive(server internal.Lease_LeaseKeepAliveServer) (err error) {
	client, err := svc.client.LeaseKeepAlive(server.Context())
	if err != nil {
		return
	}
	for {
		req, err := server.Recv()
		if err == io.EOF {
			return nil
		}
		if err != nil {
			return err
		}
		err = client.Send(req)
		if err != nil {
			break
		}
		res, err := client.Recv()
		if err == io.EOF {
			return nil
		}
		if err != nil {
			return err
		}
		err = server.Send(res)
		if err != nil {
			break
		}
		return nil
	}
	return
}

func (svc parityLeaseService) LeaseTimeToLive(
	ctx context.Context,
	req *internal.LeaseTimeToLiveRequest,
) (res *internal.LeaseTimeToLiveResponse, err error) {
	return svc.client.LeaseTimeToLive(ctx, req)
}

func (svc parityLeaseService) LeaseLeases(
	ctx context.Context,
	req *internal.LeaseLeasesRequest,
) (res *internal.LeaseLeasesResponse, err error) {
	return svc.client.LeaseLeases(ctx, req)
}

type mockLeaseKeepAliveServer struct {
	grpc.ServerStream
	req *internal.LeaseKeepAliveRequest
	res *internal.LeaseKeepAliveResponse
}

func (s *mockLeaseKeepAliveServer) Send(res *internal.LeaseKeepAliveResponse) (err error) {
	s.res = res
	return
}

func (s *mockLeaseKeepAliveServer) Recv() (req *internal.LeaseKeepAliveRequest, err error) {
	if s.res == nil {
		req = s.req
	} else {
		err = io.EOF
	}
	return
}

func (s *mockLeaseKeepAliveServer) Context() context.Context {
	return context.Background()
}

var _ internal.Lease_LeaseKeepAliveServer = new(mockLeaseKeepAliveServer)

type parityWatchService struct {
	internal.UnimplementedWatchServer

	client internal.WatchClient
}

func newParityWatchService(conn grpc.ClientConnInterface) internal.WatchServer {
	return &parityWatchService{client: internal.NewWatchClient(conn)}
}

var _ internal.WatchServer = new(parityWatchService)

func (svc *parityWatchService) Watch(server internal.Watch_WatchServer) (err error) {
	client, err := svc.client.Watch(server.Context())
	if err != nil {
		return
	}
	go func() {
		defer slog.Debug("Server watch ending")
		for {
			slog.Debug("Server watch Recving")
			req, err := server.Recv()
			slog.Debug("Server watch Recvd")
			if err == io.EOF {
				return
			}
			if err != nil {
				return
			}
			err = client.Send(req)
			if err != nil {
				break
			}
		}
	}()
	defer slog.Debug("Client watch ending")
	for {
		slog.Debug("Client watch Recving")
		res, err := client.Recv()
		slog.Debug("Client watch Recvd")
		if err == io.EOF {
			break
		}
		if err != nil {
			break
		}
		err = server.Send(res)
		if err != nil {
			break
		}
	}
	return nil
}

func (s *parityWatchService) Context() context.Context {
	return context.Background()
}

type mockWatchServer struct {
	grpc.ServerStream
	ctx     context.Context
	reqChan chan *internal.WatchRequest
	resChan chan *internal.WatchResponse
}

func newMockWatchServer(ctx context.Context) *mockWatchServer {
	return &mockWatchServer{
		ctx:     ctx,
		reqChan: make(chan *internal.WatchRequest),
		resChan: make(chan *internal.WatchResponse),
	}
}

func (s *mockWatchServer) Send(res *internal.WatchResponse) (err error) {
	s.resChan <- res
	return
}

func (s *mockWatchServer) Recv() (req *internal.WatchRequest, err error) {
	select {
	case req = <-s.reqChan:
	case <-s.ctx.Done():
		err = io.EOF
	}
	return
}

func (s *mockWatchServer) Context() context.Context {
	return s.ctx
}

var _ internal.Watch_WatchServer = new(mockWatchServer)
