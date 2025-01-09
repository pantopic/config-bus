package icarus

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"os"
	"time"

	"github.com/PowerDNS/lmdb-go/lmdb"
	"github.com/benbjohnson/clock"
	"github.com/logbn/zongzi"
	"google.golang.org/protobuf/proto"

	"github.com/logbn/icarus/internal"
)

const Uri = "zongzi://github.com/logbn/icarus"

type stateMachine struct {
	dbKv       dbKv
	dbKvEvent  dbKvEvent
	dbLeaseExp dbLeaseExp
	dbLeaseKey dbLeaseKey
	dbMeta     dbMeta
	dbStats    dbStats
	env        *lmdb.Env
	envPath    string
	log        *slog.Logger
	replicaID  uint64
	shardID    uint64
	clock      clock.Clock

	statUpdates int
	statEntries int
	statTime    time.Duration
	statPatched int
}

func NewStateMachineFactory(logger *slog.Logger, dataDir string) zongzi.StateMachinePersistentFactory {
	return func(shardID uint64, replicaID uint64) zongzi.StateMachinePersistent {
		return &stateMachine{
			shardID:   shardID,
			replicaID: replicaID,
			envPath:   fmt.Sprintf("%s/%08x/env", dataDir, replicaID),
			log:       logger,
			clock:     clock.New(),
		}
	}
}

var _ zongzi.StateMachinePersistentFactory = NewStateMachineFactory(nil, "")

func (sm *stateMachine) Open(stopc <-chan struct{}) (index uint64, err error) {
	err = os.MkdirAll(sm.envPath, 0700)
	if err != nil {
		return
	}
	sm.env, err = lmdb.NewEnv()
	sm.env.SetMaxDBs(255)
	sm.env.SetMapSize(int64(1 << 33)) // 8 GB
	sm.env.Open(sm.envPath, uint(lmdbEnvFlags), 0700)
	err = sm.env.Update(func(txn *lmdb.Txn) (err error) {
		if sm.dbMeta, index, err = newDbMeta(txn); err != nil {
			return
		}
		if sm.dbStats, err = newDbStats(txn); err != nil {
			return
		}
		if sm.dbKv, err = newDbKv(txn); err != nil {
			return
		}
		if sm.dbKvEvent, err = newDbKvEvent(txn); err != nil {
			return
		}
		if sm.dbLeaseExp, err = newDbLeaseExp(txn); err != nil {
			return
		}
		if sm.dbLeaseKey, err = newDbLeaseKey(txn); err != nil {
			return
		}
		return
	})
	return
}

func (sm *stateMachine) Update(entries []Entry) []Entry {
	var t = sm.clock.Now()
	var req = &internal.PutRequest{}
	var res = &internal.PutResponse{}
	sm.statUpdates++
	sm.statEntries += len(entries)
	if sm.statUpdates > 100 {
		sm.log.Info("StateMachine Stats",
			"entries", sm.statEntries,
			"average", sm.statEntries/sm.statUpdates,
			"diffed", sm.statPatched,
			"uTime", int(sm.statTime.Microseconds())/sm.statUpdates,
			"eTime", int(sm.statTime.Microseconds())/sm.statEntries)
		sm.statPatched = 0
		sm.statEntries = 0
		sm.statTime = 0
		sm.statUpdates = 0
	}
	if err := sm.env.Update(func(txn *lmdb.Txn) (err error) {
		for i, ent := range entries {
			switch ent.Cmd[len(ent.Cmd)-1] {
			case CMD_KV_PUT:
				if err = proto.Unmarshal(ent.Cmd[:len(ent.Cmd)-1], req); err != nil {
					sm.log.Error("Invalid command", "cmd", fmt.Sprintf("%x", ent.Cmd))
					continue
				}
				prev, _, patched, err := sm.dbKv.put(txn, ent.Index, uint64(req.Lease), req.Key, req.Value)
				if err != nil {
					return err
				}
				if patched {
					sm.statPatched++
				}
				// TODO Replace timestamp w/ epoch
				err = sm.dbKvEvent.put(txn, ent.Index, uint64(t.Unix()), req.Key)
				if err != nil {
					return err
				}
				res.Reset()
				res.Header = sm.responseHeader(ent.Index)
				if req.PrevKv {
					res.PrevKv = prev.ToProto()
				}
				entries[i].Result.Data, err = proto.Marshal(res)
				if err != nil {
					return err
				}
				entries[i].Result.Value = 1
				// TODO: Insert kv event
			}
		}
		sm.dbMeta.setRevision(txn, entries[len(entries)-1].Index)
		return
	}); err != nil {
		// TODO: Identify and log transient errors
		sm.log.Error(err.Error(), "index", entries[0].Index)
		panic("Storage error: " + err.Error())
	}
	sm.statTime += sm.clock.Since(t)
	// TODO: Notify watchers
	return entries
}

func (sm *stateMachine) Query(ctx context.Context, query []byte) (res *Result) {
	res = zongzi.GetResult()
	switch query[len(query)-1] {
	case QUERY_KV_RANGE:
		var req = &internal.RangeRequest{}
		if err := proto.Unmarshal(query[:len(query)-1], req); err != nil {
			sm.log.Error("Invalid query", "query", fmt.Sprintf("%x", query))
			return
		}
		if err := sm.env.View(func(txn *lmdb.Txn) (err error) {
			index, err := sm.dbMeta.getRevision(txn)
			if err != nil {
				return
			}
			if req.Revision > 0 {
				min, err := sm.dbMeta.getRevisionMin(txn)
				if err != nil {
					return err
				}
				if req.Revision < int64(min) {
					sm.log.Info("Revision compacted", "rev", req.Revision, "min", min)
					res.Data = []byte(internal.ErrGRPCCompacted.Error())
					return nil
				}
			}
			resp := &internal.RangeResponse{
				Header: sm.responseHeader(index),
			}
			if req.CountOnly {
				_, count, _, err := sm.dbKv.getRange(txn,
					req.Key,
					req.RangeEnd,
					uint64(req.Revision),
					uint64(req.MinModRevision),
					uint64(req.MaxModRevision),
					uint64(req.MinCreateRevision),
					uint64(req.MaxCreateRevision),
					uint64(req.Limit),
					req.CountOnly,
					req.KeysOnly,
				)
				if err != nil {
					return err
				}
				resp.Count = int64(count)
				if res.Data, err = proto.Marshal(resp); err != nil {
					return err
				}
				res.Value = 1
			} else {
				data, _, more, err := sm.dbKv.getRange(txn,
					req.Key,
					req.RangeEnd,
					uint64(req.Revision),
					uint64(req.MinModRevision),
					uint64(req.MaxModRevision),
					uint64(req.MinCreateRevision),
					uint64(req.MaxCreateRevision),
					uint64(req.Limit),
					req.CountOnly,
					req.KeysOnly,
				)
				if err != nil {
					return err
				}
				for _, kv := range data {
					resp.Kvs = append(resp.Kvs, kv.ToProto())
				}
				resp.More = more
				if res.Data, err = proto.Marshal(resp); err != nil {
					return err
				}
				res.Value = 1
			}
			return
		}); err != nil {
			// TODO: Identify and log transient errors
			sm.log.Error("Unexpected error", "err", err.Error())
			panic(err)
		}
	}
	return
}

func (sm *stateMachine) Watch(ctx context.Context, query []byte, result chan<- *Result) {
	return
}

func (sm *stateMachine) PrepareSnapshot() (cursor any, err error) {
	return sm.env.BeginTxn(nil, lmdb.Readonly)
}

func (sm *stateMachine) SaveSnapshot(cursor any, w io.Writer, close <-chan struct{}) (err error) {
	defer cursor.(*lmdb.Txn).Abort()
	f, err := os.OpenFile(sm.envPath, os.O_RDONLY, 0700)
	if err != nil {
		return
	}
	_, err = io.Copy(w, f)
	f.Close()
	return
}

func (sm *stateMachine) RecoverFromSnapshot(r io.Reader, close <-chan struct{}) (err error) {
	f, err := os.OpenFile(sm.envPath, os.O_WRONLY|os.O_CREATE, 0700)
	if err != nil {
		return
	}
	_, err = io.Copy(f, r)
	f.Close()
	return
}

func (sm *stateMachine) Sync() error {
	return sm.env.Sync(true)
}

func (sm *stateMachine) Close() error {
	return sm.env.Close()
}

func (sm *stateMachine) responseHeader(revision uint64) *internal.ResponseHeader {
	return &internal.ResponseHeader{
		Revision:  int64(revision),
		ClusterId: sm.shardID,
		MemberId:  sm.replicaID,
		// TODO: Get access to Term through IRaftEventListener.LeaderInfo
		RaftTerm: 1,
	}
}
