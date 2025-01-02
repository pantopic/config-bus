package icarus

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"os"

	"github.com/PowerDNS/lmdb-go/lmdb"
	"github.com/logbn/zongzi"
	"google.golang.org/protobuf/proto"

	"github.com/logbn/icarus/internal"
)

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
}

func NewStateMachineFactory(logger *slog.Logger, dataDir string) zongzi.StateMachinePersistentFactory {
	return func(shardID uint64, replicaID uint64) zongzi.StateMachinePersistent {
		return &stateMachine{
			envPath: fmt.Sprintf("%s/data.mdb", dataDir),
			log:     logger,
		}
	}
}

var _ zongzi.StateMachinePersistentFactory = NewStateMachineFactory(nil, "")

func (sm *stateMachine) Open(stopc <-chan struct{}) (index uint64, err error) {
	err = os.MkdirAll(sm.envPath, 0700)
	if err != nil {
		return
	}
	env, err := lmdb.NewEnv()
	env.SetMaxDBs(255)
	env.SetMapSize(int64(1 << 33)) // 8 GB
	env.Open(sm.envPath, uint(lmdbEnvFlags), 0700)
	err = env.Update(func(txn *lmdb.Txn) (err error) {
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
		if sm.dbMeta, err = newDbMeta(txn); err != nil {
			return
		}
		if sm.dbStats, err = newDbStats(txn); err != nil {
			return
		}
		index, err = sm.dbMeta.getRevision(txn)
		if lmdb.IsNotFound(err) {
			sm.dbMeta.setRevision(txn, 0)
			sm.dbMeta.setRevisionMin(txn, 0)
		}
		return
	})
	return
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

func (sm *stateMachine) Update(entries []Entry) []Entry {
	var cmd uint8
	var header = sm.responseHeader(0)
	err := sm.env.Update(func(txn *lmdb.Txn) (err error) {
		for i, ent := range entries {
			cmd = ent.Cmd[0]
			switch cmd {
			case CMD_KV_PUT:
				var req = &internal.PutRequest{}
				if err = proto.Unmarshal(ent.Cmd[1:], req); err != nil {
					slog.Error("Invalid command", "cmd", fmt.Sprintf("%x", ent.Cmd))
					continue
				}
				prev, _, err := sm.dbKv.put(txn, ent.Index, uint64(req.Lease), req.Key, req.Value)
				if err != nil {
					// TODO: Catch transient errors
					panic("Storage error: " + err.Error())
				}
				header.Revision = int64(ent.Index)
				var res = &internal.PutResponse{
					Header: header,
				}
				if req.PrevKv {
					res.PrevKv = prev.ToProto()
				}
				entries[i].Result.Data, err = proto.Marshal(res)
				if err = proto.Unmarshal(ent.Cmd[1:], req); err != nil {
					slog.Error("Invalid command", "cmd", fmt.Sprintf("%x", ent.Cmd))
					continue
				}
				entries[i].Result.Value = 1
			}
		}
		sm.dbMeta.setRevision(txn, entries[len(entries)-1].Index)
		return
	})
	if err != nil {
		slog.Error(err.Error(), "index", entries[0].Index)
	}
	return entries
}

func (sm *stateMachine) Query(ctx context.Context, query []byte) (res *Result) {
	res = zongzi.GetResult()
	switch query[0] {
	case QUERY_KV_RANGE:
		var req = &internal.RangeRequest{}
		if err := proto.Unmarshal(query[1:], req); err != nil {
			slog.Error("Invalid query", "query", fmt.Sprintf("%x", query))
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
				if uint64(req.Revision) > min {
					res.Data = []byte(internal.ErrGRPCCompacted.Error())
					return nil
				}
			}
			resp := &internal.RangeResponse{
				Header: sm.responseHeader(index),
			}
			if req.CountOnly {
				count, err := sm.dbKv.count(txn,
					req.Key,
					req.RangeEnd,
					uint64(req.Revision),
					uint64(req.MinModRevision),
					uint64(req.MaxModRevision),
					uint64(req.MinCreateRevision),
					uint64(req.MaxCreateRevision),
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
				data, more, err := sm.dbKv.getRange(txn,
					req.Key,
					req.RangeEnd,
					uint64(req.Revision),
					uint64(req.MinModRevision),
					uint64(req.MaxModRevision),
					uint64(req.MinCreateRevision),
					uint64(req.MaxCreateRevision),
					uint64(req.Limit),
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
			slog.Error("Invalid query", "query", fmt.Sprintf("%x", query))
			return
		}
	}
	return res
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
	return nil
}
