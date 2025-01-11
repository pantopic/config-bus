//go:build !unit

package icarus

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"testing"
	"time"

	"github.com/logbn/zongzi"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/logbn/icarus/internal"
)

func TestService(t *testing.T) {
	var err error
	var (
		agents = make([]*zongzi.Agent, 3)
		ctx    = context.Background()
		dir    = "/tmp/icarus/test"
		host   = "127.0.0.1"
		log    = slog.Default()
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
	require.True(t, await(5, 100, func() bool {
		agents[0].State(ctx, func(s *zongzi.State) {
			if found, ok := s.Shard(shard.ID); ok {
				shard = found
			}
		})
		return shard.Leader > 0
	}))
	svc := NewKvService(agents[0].Client(shard.ID))
	var rev int64 = 0
	t.Run("insert", func(t *testing.T) {
		put := &internal.PutRequest{
			Key:   []byte(`test-key`),
			Value: []byte(`test-value`),
		}
		t.Run("put", func(t *testing.T) {
			resp, err := svc.Put(ctx, put)
			require.Nil(t, err, err)
			assert.NotNil(t, resp)
			assert.Greater(t, resp.Header.Revision, rev)
			rev = resp.Header.Revision
		})
		t.Run("get", func(t *testing.T) {
			resp, err := svc.Range(ctx, &internal.RangeRequest{
				Key: put.Key,
			})
			require.Nil(t, err, err)
			assert.NotNil(t, resp)
			assert.Greater(t, resp.Header.Revision, int64(0))
			require.Greater(t, len(resp.Kvs), 0)
			assert.Equal(t, put.Key, resp.Kvs[0].Key)
			assert.Equal(t, put.Value, resp.Kvs[0].Value)
		})
	})
	var rev1 = rev
	t.Run("update", func(t *testing.T) {
		put := &internal.PutRequest{
			Key:   []byte(`test-key`),
			Value: []byte(`test-value-2`),
		}
		t.Run("put", func(t *testing.T) {
			resp, err := svc.Put(ctx, put)
			require.Nil(t, err, err)
			assert.NotNil(t, resp)
			assert.Greater(t, resp.Header.Revision, rev)
			rev = resp.Header.Revision
		})
		t.Run("get", func(t *testing.T) {
			resp, err := svc.Range(ctx, &internal.RangeRequest{
				Key: put.Key,
			})
			require.Nil(t, err, err)
			assert.NotNil(t, resp)
			assert.Greater(t, resp.Header.Revision, int64(0))
			require.Greater(t, len(resp.Kvs), 0)
			assert.Equal(t, put.Key, resp.Kvs[0].Key)
			assert.Equal(t, put.Value, resp.Kvs[0].Value)
		})
		t.Run("prev", func(t *testing.T) {
			resp, err := svc.Put(ctx, &internal.PutRequest{
				Key:    []byte(`test-key`),
				Value:  []byte(`test-value-3`),
				PrevKv: true,
			})
			require.Nil(t, err, err)
			assert.NotNil(t, resp)
			assert.Greater(t, resp.Header.Revision, rev)
			assert.Equal(t, resp.PrevKv.Value, put.Value)
			rev = resp.Header.Revision
		})
	})
	t.Run("range", func(t *testing.T) {
		_, err := svc.Put(ctx, &internal.PutRequest{
			Key:   []byte(`test-range-key-1`),
			Value: []byte(`test-range-value-2`),
		})
		require.Nil(t, err, err)
		_, err = svc.Put(ctx, &internal.PutRequest{
			Key:   []byte(`test-range-key-2`),
			Value: []byte(`test-range-value-2`),
		})
		require.Nil(t, err, err)
		t.Run("basic", func(t *testing.T) {
			resp, err := svc.Range(ctx, &internal.RangeRequest{
				Key:      []byte(`test-range-key-1`),
				RangeEnd: []byte(`test-range-key-2`),
			})
			require.Nil(t, err, err)
			assert.NotNil(t, resp)
			assert.Greater(t, resp.Header.Revision, int64(0))
			require.Equal(t, 2, len(resp.Kvs))
			assert.Equal(t, []byte(`test-range-key-1`), resp.Kvs[0].Key)
			assert.Equal(t, []byte(`test-range-key-2`), resp.Kvs[1].Key)
		})
		t.Run("count", func(t *testing.T) {
			resp, err := svc.Range(ctx, &internal.RangeRequest{
				Key:       []byte(`test-range-key-1`),
				RangeEnd:  []byte(`test-range-key-2`),
				CountOnly: true,
			})
			require.Nil(t, err, err)
			assert.NotNil(t, resp)
			assert.Greater(t, resp.Header.Revision, int64(0))
			require.Equal(t, int64(2), resp.Count)
		})
		t.Run("revision", func(t *testing.T) {
			resp, err := svc.Range(ctx, &internal.RangeRequest{
				Key:      []byte(`test-key`),
				RangeEnd: []byte(`test-range-key-2`),
				Revision: rev1,
			})
			require.Nil(t, err, err)
			assert.NotNil(t, resp)
			assert.Greater(t, resp.Header.Revision, int64(0))
			require.Equal(t, 1, len(resp.Kvs))
			assert.Equal(t, []byte(`test-key`), resp.Kvs[0].Key, string(resp.Kvs[0].Key))
			assert.Equal(t, []byte(`test-value`), resp.Kvs[0].Value, string(resp.Kvs[0].Value))
		})
		t.Run("next", func(t *testing.T) {
			resp, err := svc.Range(ctx, &internal.RangeRequest{
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
			resp, err := svc.Range(ctx, &internal.RangeRequest{
				Key: []byte(`rest`),
			})
			require.Nil(t, err, err)
			assert.NotNil(t, resp)
			assert.Greater(t, resp.Header.Revision, int64(0))
			require.Equal(t, 0, len(resp.Kvs))
		})
	})
	t.Run("patch", func(t *testing.T) {
		resp, err := svc.Put(ctx, &internal.PutRequest{
			Key:   []byte(`test-key-patch`),
			Value: []byte(`--------------------------------------------------------------------------------`),
		})
		require.Nil(t, err, err)
		rev = resp.Header.Revision
		t.Run("put", func(t *testing.T) {
			resp, err := svc.Put(ctx, &internal.PutRequest{
				Key:   []byte(`test-key-patch`),
				Value: []byte(`----------------------------------------0----------------------------------------`),
			})
			require.Nil(t, err, err)
			assert.NotNil(t, resp)
			assert.Greater(t, resp.Header.Revision, rev)
		})
		t.Run("revision", func(t *testing.T) {
			resp, err := svc.Range(ctx, &internal.RangeRequest{
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
	})
	t.Run("delete", func(t *testing.T) {
		t.Run("one", func(t *testing.T) {
			var k = []byte(`test-key-delete-one`)
			var v = []byte(`test-val`)
			_, err := svc.Put(ctx, &internal.PutRequest{Key: k, Value: v})
			require.Nil(t, err, err)
			resp, err := svc.Range(ctx, &internal.RangeRequest{Key: k})
			require.Nil(t, err, err)
			assert.Equal(t, 1, len(resp.Kvs))
			resp2, err := svc.DeleteRange(ctx, &internal.DeleteRangeRequest{
				Key: k,
			})
			require.Nil(t, err, err)
			assert.EqualValues(t, 1, resp2.Deleted)
			resp, err = svc.Range(ctx, &internal.RangeRequest{
				Key: k,
			})
			require.Nil(t, err, err)
			assert.Equal(t, 0, len(resp.Kvs))
		})
		t.Run("range", func(t *testing.T) {
			for i := range 10 {
				_, err = svc.Put(ctx, &internal.PutRequest{
					Key:   []byte(fmt.Sprintf(`test-key-delete-%d`, i)),
					Value: []byte(`-------------------`),
				})
				require.Nil(t, err, err)
			}
			resp, err := svc.Range(ctx, &internal.RangeRequest{
				Key:      []byte(`test-key-delete-0`),
				RangeEnd: []byte(`test-key-delete-9`),
			})
			require.Nil(t, err, err)
			assert.Equal(t, 10, len(resp.Kvs))
			resp2, err := svc.DeleteRange(ctx, &internal.DeleteRangeRequest{
				Key:      []byte(`test-key-delete-0`),
				RangeEnd: []byte(`test-key-delete-9`),
			})
			require.Nil(t, err, err)
			assert.EqualValues(t, 10, resp2.Deleted)
			resp, err = svc.Range(ctx, &internal.RangeRequest{
				Key:      []byte(`test-key-delete-0`),
				RangeEnd: []byte(`test-key-delete-9`),
			})
			require.Nil(t, err, err)
			assert.Equal(t, 0, len(resp.Kvs))
		})
		t.Run("missing", func(t *testing.T) {
			var k = []byte(`test-key-delete-one`)
			resp, err := svc.Range(ctx, &internal.RangeRequest{Key: k})
			require.Nil(t, err, err)
			assert.Equal(t, 0, len(resp.Kvs))
			resp2, err := svc.DeleteRange(ctx, &internal.DeleteRangeRequest{
				Key: k,
			})
			require.Nil(t, err, err)
			assert.EqualValues(t, 0, resp2.Deleted)
		})
	})
	t.Run("compact", func(t *testing.T) {
		var revs []int64
		for i := range 10 {
			resp, err := svc.Put(ctx, &internal.PutRequest{
				Key:   []byte(fmt.Sprintf(`test-key-compact-%d`, i)),
				Value: []byte(`-------------------`),
			})
			require.Nil(t, err, err)
			revs = append(revs, resp.Header.Revision)
		}
		resp, err := svc.Range(ctx, &internal.RangeRequest{
			Key:      []byte(`test-key-compact-0`),
			RangeEnd: []byte(`test-key-compact-9`),
			Revision: revs[1],
		})
		require.Nil(t, err, err)
		assert.Equal(t, 2, len(resp.Kvs))
		for i := range 10 {
			resp, err := svc.DeleteRange(ctx, &internal.DeleteRangeRequest{
				Key: []byte(fmt.Sprintf(`test-key-compact-%d`, i)),
			})
			require.Nil(t, err, err)
			revs = append(revs, resp.Header.Revision)
		}
		resp, err = svc.Range(ctx, &internal.RangeRequest{
			Key:      []byte(`test-key-compact-0`),
			RangeEnd: []byte(`test-key-compact-9`),
		})
		require.Nil(t, err, err)
		assert.Equal(t, 0, len(resp.Kvs))
		resp, err = svc.Range(ctx, &internal.RangeRequest{
			Key:      []byte(`test-key-compact-0`),
			RangeEnd: []byte(`test-key-compact-9`),
			Revision: revs[10],
		})
		require.Nil(t, err, err)
		assert.Equal(t, 9, len(resp.Kvs))
		_, err = svc.Compact(ctx, &internal.CompactionRequest{
			Revision: revs[10],
		})
		require.Nil(t, err, err)
		resp2, err := svc.Range(ctx, &internal.RangeRequest{
			Key:      []byte(`test-key-compact-0`),
			RangeEnd: []byte(`test-key-compact-9`),
			Revision: revs[10],
		})
		require.Nil(t, err, err)
		assert.Equal(t, 9, len(resp2.Kvs))
		resp2, err = svc.Range(ctx, &internal.RangeRequest{
			Key:      []byte(`test-key-compact-0`),
			RangeEnd: []byte(`test-key-compact-9`),
			Revision: revs[9],
		})
		require.NotNil(t, err, err)
	})

	t.Run("transaction", func(t *testing.T) {
		_, err = svc.Put(ctx, &internal.PutRequest{
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
					RangeEnd: []byte(`test-txn-02`),
				}),
			},
			Failure: []*internal.RequestOp{
				putOp(&internal.PutRequest{
					Key:   []byte(`test-txn-00`),
					Value: []byte(`-----------`),
				}),
				rangeOp(&internal.RangeRequest{
					Key:      []byte(`test-txn-00`),
					RangeEnd: []byte(`test-txn-02`),
				}),
			},
		}
		resp, err := svc.Txn(ctx, req)
		require.Nil(t, err, err)
		assert.True(t, resp.Succeeded)
		assert.Len(t, resp.Responses, len(req.Success))
		assert.Len(t, resp.Responses[3].Response.(*internal.ResponseOp_ResponseRange).ResponseRange.Kvs, 2)
		resp, err = svc.Txn(ctx, req)
		require.Nil(t, err, err)
		assert.False(t, resp.Succeeded)
		assert.Len(t, resp.Responses, len(req.Failure))
		assert.Len(t, resp.Responses[1].Response.(*internal.ResponseOp_ResponseRange).ResponseRange.Kvs, 3)
	})

	// ✅ Put
	// ✅ Range
	// ✅ Delete
	// ✅ Compact
	// ✅ Txn
	// Watch
	// LeaseGrant
	// LeaseRevoke
	// LeaseKeepAlive
	// LeaseTimeToLive
	// LeaseLeases

	// Status
	// Defragment
	// Hash
	// HashKV
	// MemberList
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
