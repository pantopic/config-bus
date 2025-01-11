package icarus

import (
	"bytes"
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
	// TODO - Replace timestamp w/ real epoch
	var epoch = uint64(t.Unix())
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
				// TODO - Add sync pool for protobuf messages
				var reqPut = &internal.PutRequest{}
				if err = proto.Unmarshal(ent.Cmd[:len(ent.Cmd)-1], reqPut); err != nil {
					sm.log.Error("Invalid command", "cmd", fmt.Sprintf("%x", ent.Cmd))
					continue
				}
				resPut, err := sm.put(txn, ent.Index, epoch, reqPut)
				if err != nil {
					return err
				}
				entries[i].Result.Data, err = proto.Marshal(resPut)
				if err != nil {
					return err
				}
				entries[i].Result.Value = 1
			case CMD_KV_DELETE_RANGE:
				var reqDel = &internal.DeleteRangeRequest{}
				if err = proto.Unmarshal(ent.Cmd[:len(ent.Cmd)-1], reqDel); err != nil {
					sm.log.Error("Invalid command", "cmd", fmt.Sprintf("%x", ent.Cmd))
					continue
				}
				resDel, err := sm.deleteRange(txn, ent.Index, epoch, reqDel)
				if err != nil {
					return err
				}
				entries[i].Result.Data, err = proto.Marshal(resDel)
				if err != nil {
					return err
				}
				entries[i].Result.Value = 1
			case CMD_KV_COMPACT:
				var req = &internal.CompactionRequest{}
				if err = proto.Unmarshal(ent.Cmd[:len(ent.Cmd)-1], req); err != nil {
					sm.log.Error("Invalid command", "cmd", fmt.Sprintf("%x", ent.Cmd))
					continue
				}
				// TODO - Add support for asynchronous compaction w/ req.Physical
				batch, index, err := sm.dbKvEvent.compact(txn, uint64(req.Revision))
				if err != nil {
					return err
				}
				if _, err = sm.dbKv.compact(txn, batch); err != nil {
					return err
				}
				var res = &internal.CompactionResponse{}
				res.Header = sm.responseHeader(ent.Index)
				entries[i].Result.Data, err = proto.Marshal(res)
				if err != nil {
					return err
				}
				if err := sm.dbMeta.setRevisionMin(txn, uint64(req.Revision)); err != nil {
					return err
				}
				if err := sm.dbMeta.setRevisionCompacted(txn, index); err != nil {
					return err
				}
				entries[i].Result.Value = 1
			case CMD_KV_TXN:
				var req = &internal.TxnRequest{}
				if err = proto.Unmarshal(ent.Cmd[:len(ent.Cmd)-1], req); err != nil {
					sm.log.Error("Invalid command", "cmd", fmt.Sprintf("%x", ent.Cmd))
					continue
				}
				var success bool
				success, err = sm.runCompare(txn, req.Compare)
				if err != nil {
					return
				}
				var res = &internal.TxnResponse{
					Succeeded: success,
				}
				if success {
					res.Responses, err = sm.runOps(txn, ent.Index, epoch, req.Success)
				} else {
					res.Responses, err = sm.runOps(txn, ent.Index, epoch, req.Failure)
				}
				if err == internal.ErrGRPCDuplicateKey {
					entries[i].Result.Data = []byte(err.Error())
					err = nil
				} else if err != nil {
					return
				} else {
					res.Header = sm.responseHeader(ent.Index)
					entries[i].Result.Data, err = proto.Marshal(res)
					entries[i].Result.Value = 1
				}
			}
		}
		sm.dbMeta.setRevision(txn, entries[len(entries)-1].Index)
		return
	}); err != nil {
		// TODO - Identify and log transient errors
		sm.log.Error(err.Error(), "index", entries[0].Index)
		panic("Storage error: " + err.Error())
	}
	sm.statTime += sm.clock.Since(t)
	// TODO - Notify watchers
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
		var resp *internal.RangeResponse
		err := sm.env.View(func(txn *lmdb.Txn) (err error) {
			index, err := sm.dbMeta.getRevision(txn)
			if err != nil {
				return
			}
			resp, err = sm.rangeReq(txn, index, req)
			return
		})
		if err == internal.ErrGRPCCompacted || err == internal.ErrGRPCFutureRev {
			res.Data = []byte(err.Error())
			err = nil
		} else if err != nil {
			return
		} else {
			if res.Data, err = proto.Marshal(resp); err != nil {
				sm.log.Error("Invalid response", "res", fmt.Sprintf("%x", query))
				return nil
			}
			res.Value = 1
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
	}
}

func (sm *stateMachine) put(txn *lmdb.Txn, index, epoch uint64, req *internal.PutRequest) (res *internal.PutResponse, err error) {
	prev, _, patched, err := sm.dbKv.put(txn, index, uint64(req.Lease), req.Key, req.Value)
	if err != nil {
		return
	}
	if patched {
		sm.statPatched++
	}
	// TODO - Replace timestamp w/ epoch
	err = sm.dbKvEvent.put(txn, index, epoch, req.Key)
	if err != nil {
		return
	}
	// TODO - Insert lease keys
	res = &internal.PutResponse{}
	res.Header = sm.responseHeader(index)
	if req.PrevKv {
		res.PrevKv = prev.ToProto()
	}
	return
}

func (sm *stateMachine) deleteRange(txn *lmdb.Txn, index, epoch uint64, req *internal.DeleteRangeRequest) (res *internal.DeleteRangeResponse, err error) {
	prev, n, err := sm.dbKv.deleteRange(txn, index, req.Key, req.RangeEnd)
	if err != nil {
		return
	}
	var keys = make([][]byte, len(prev))
	for _, item := range prev {
		keys = append(keys, item.key)
	}

	err = sm.dbKvEvent.delete(txn, index, epoch, keys)
	if err != nil {
		return
	}
	res = &internal.DeleteRangeResponse{}
	res.Deleted = n
	res.Header = sm.responseHeader(index)
	if req.PrevKv {
		for _, item := range prev {
			res.PrevKvs = append(res.PrevKvs, item.ToProto())
		}
	}
	return
}

func (sm *stateMachine) rangeReq(txn *lmdb.Txn, index uint64, req *internal.RangeRequest) (res *internal.RangeResponse, err error) {
	if req.Revision > 0 {
		min, err := sm.dbMeta.getRevisionMin(txn)
		if err != nil {
			return nil, err
		}
		if req.Revision < int64(min) {
			return nil, internal.ErrGRPCCompacted
		}
		if req.Revision > int64(index) {
			return nil, internal.ErrGRPCFutureRev
		}
	}
	res = &internal.RangeResponse{
		Header: sm.responseHeader(index),
	}
	data, count, more, err := sm.dbKv.getRange(txn,
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
		return nil, err
	}
	if req.CountOnly || ICARUS_KV_FULL_COUNT_ENABLED {
		res.Count = int64(count)
	}
	if !req.CountOnly {
		for _, kv := range data {
			res.Kvs = append(res.Kvs, kv.ToProto())
		}
		res.More = more
	}
	return
}

func (sm *stateMachine) runOps(txn *lmdb.Txn, index, epoch uint64, ops []*internal.RequestOp) (res []*internal.ResponseOp, err error) {
	for _, op := range ops {
		switch op.Request.(type) {
		case *internal.RequestOp_RequestPut:
			putReq := op.Request.(*internal.RequestOp_RequestPut).RequestPut
			putRes, err := sm.put(txn, index, epoch, putReq)
			if err != nil {
				return nil, err
			}
			res = append(res, &internal.ResponseOp{
				Response: &internal.ResponseOp_ResponsePut{
					ResponsePut: putRes,
				},
			})
		case *internal.RequestOp_RequestDeleteRange:
			delReq := op.Request.(*internal.RequestOp_RequestDeleteRange).RequestDeleteRange
			delRes, err := sm.deleteRange(txn, index, epoch, delReq)
			if err != nil {
				return nil, err
			}
			res = append(res, &internal.ResponseOp{
				Response: &internal.ResponseOp_ResponseDeleteRange{
					ResponseDeleteRange: delRes,
				},
			})
		case *internal.RequestOp_RequestRange:
			rangeReq := op.Request.(*internal.RequestOp_RequestRange).RequestRange
			rangeRes, err := sm.rangeReq(txn, index, rangeReq)
			if err != nil {
				return nil, err
			}
			res = append(res, &internal.ResponseOp{
				Response: &internal.ResponseOp_ResponseRange{
					ResponseRange: rangeRes,
				},
			})
		}
	}
	return
}
func (sm *stateMachine) runCompare(txn *lmdb.Txn, conds []*internal.Compare) (success bool, err error) {
	success = true
	var item kv
	for _, cond := range conds {
		if item, err = sm.dbKv.get(txn, cond.Key); err != nil {
			success = false
			break
		}
		switch cond.Target {
		case internal.Compare_VERSION:
			success = intCompare(cond.Result, int64(item.version), cond.TargetUnion.(*internal.Compare_Version).Version)
		case internal.Compare_CREATE:
			success = intCompare(cond.Result, int64(item.created), cond.TargetUnion.(*internal.Compare_CreateRevision).CreateRevision)
		case internal.Compare_MOD:
			success = intCompare(cond.Result, int64(item.revision), cond.TargetUnion.(*internal.Compare_ModRevision).ModRevision)
		case internal.Compare_LEASE:
			success = intCompare(cond.Result, int64(item.lease), cond.TargetUnion.(*internal.Compare_Lease).Lease)
		case internal.Compare_VALUE:
			v := cond.TargetUnion.(*internal.Compare_Value).Value
			switch cond.Result {
			case internal.Compare_EQUAL:
				success = bytes.Equal(item.val, v)
			case internal.Compare_GREATER:
				success = bytes.Compare(item.val, v) > 0
			case internal.Compare_LESS:
				success = bytes.Compare(item.val, v) < 0
			case internal.Compare_NOT_EQUAL:
				success = !bytes.Equal(item.val, v)
			}
		}
		if !success {
			break
		}
	}
	return
}
func intCompare(cond internal.Compare_CompareResult, a, b int64) bool {
	switch cond {
	case internal.Compare_EQUAL:
		return a == b
	case internal.Compare_GREATER:
		return a > b
	case internal.Compare_LESS:
		return a < b
	case internal.Compare_NOT_EQUAL:
		return a != b
	}
	return false
}
