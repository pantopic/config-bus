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

func Test(t *testing.T) {
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
			t.Log("REVISION", rev)
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
