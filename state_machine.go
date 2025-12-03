package krv

import (
	"bytes"
	"context"
	"encoding/binary"
	"fmt"
	"io"
	"log/slog"
	"os"
	"time"

	"github.com/PowerDNS/lmdb-go/lmdb"
	"github.com/benbjohnson/clock"
	"github.com/logbn/byteinterval"
	"github.com/logbn/zongzi"
	"google.golang.org/grpc/codes"
	"google.golang.org/protobuf/proto"

	"github.com/pantopic/krv/internal"
)

const Uri = "zongzi://github.com/pantopic/krv"

type stateMachine struct {
	clock      clock.Clock
	dbKv       dbKv
	dbLease    dbLease
	dbLeaseExp dbLeaseExp
	dbLeaseKey dbLeaseKey
	dbMeta     dbMeta
	dbStats    dbStats
	env        *lmdb.Env
	envPath    string
	log        *slog.Logger
	proto      proto.MarshalOptions
	replicaID  uint64
	shardID    uint64
	watches    *byteinterval.Tree[chan uint64]

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
			envPath:   fmt.Sprintf("%s/%08x/%08x", dataDir, shardID, replicaID),
			log:       logger,
			clock:     clock.New(),
			watches:   byteinterval.New[chan uint64](),
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
	sm.env.SetMapSize(int64(64 << 30)) // 64 GiB
	if err = sm.env.Open(sm.envPath+`/data.mdb`, uint(lmdbEnvFlags), 0700); err != nil {
		panic(err)
	}
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
		if sm.dbLease, err = newDbLease(txn); err != nil {
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
	var rev, newRev uint64
	sm.statUpdates++
	sm.statEntries += len(entries)
	if sm.statUpdates > 100 {
		sm.log.Debug("StateMachine Stats",
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
	var keys [][]byte
	if err := sm.env.Update(func(txn *lmdb.Txn) (err error) {
		epoch, err := sm.dbMeta.getEpoch(txn)
		if err != nil {
			return
		}
		rev, err = sm.dbMeta.getRevision(txn)
		if err != nil {
			return
		}
		newRev = rev
		for i, ent := range entries {
			switch ent.Cmd[len(ent.Cmd)-1] {
			case CMD_KV_PUT:
				// TODO - Add sync pool for protobuf messages
				var req = &internal.PutRequest{}
				if err = proto.Unmarshal(ent.Cmd[:len(ent.Cmd)-1], req); err != nil {
					sm.log.Error("Invalid command", "cmd", fmt.Sprintf("%x", ent.Cmd))
					continue
				}
				if req.IgnoreLease && req.Lease != 0 {
					entries[i].Result.Data = []byte(internal.ErrGRPCLeaseProvided.Error())
					continue
				}
				if req.IgnoreValue && len(req.Value) != 0 {
					entries[i].Result.Data = []byte(internal.ErrGRPCValueProvided.Error())
					continue
				}
				var item lease
				if req.Lease != 0 {
					item, err = sm.dbLease.get(txn, uint64(req.Lease))
					if err != nil {
						return
					}
					if item.id == 0 {
						entries[i].Result.Data = []byte(internal.ErrGRPCLeaseNotFound.Error())
						continue
					}
				}
				res, val, affected, err := sm.cmdPut(txn, newRev+1, 0, epoch, req)
				if err == internal.ErrGRPCKeyTooLong ||
					err == internal.ErrGRPCEmptyKey {
					entries[i].Result.Data = []byte(err.Error())
					err = nil
					continue
				} else if err != nil {
					return err
				}
				if len(affected) > 0 {
					newRev++
				}
				res.Header = sm.responseHeader(newRev)
				entries[i].Result.Data, err = proto.Marshal(res)
				if err != nil {
					return err
				}
				entries[i].Result.Value = val
				keys = append(keys, affected...)
			case CMD_KV_DELETE_RANGE:
				var reqDel = &internal.DeleteRangeRequest{}
				if err = proto.Unmarshal(ent.Cmd[:len(ent.Cmd)-1], reqDel); err != nil {
					sm.log.Error("Invalid command", "cmd", fmt.Sprintf("%x", ent.Cmd))
					continue
				}
				resDel, affected, err := sm.cmdDeleteRange(txn, newRev+1, 0, epoch, reqDel)
				if err != nil {
					return err
				}
				if len(affected) > 0 {
					newRev++
				}
				resDel.Header = sm.responseHeader(newRev)
				entries[i].Result.Data, err = proto.Marshal(resDel)
				if err != nil {
					return err
				}
				entries[i].Result.Value = 1
				keys = append(keys, affected...)
			case CMD_KV_COMPACT:
				var req = &internal.CompactionRequest{}
				if err = proto.Unmarshal(ent.Cmd[:len(ent.Cmd)-1], req); err != nil {
					sm.log.Error("Invalid command", "cmd", fmt.Sprintf("%x", ent.Cmd))
					continue
				}
				// TODO - Add support for asynchronous compaction w/ req.Physical
				rev, err := sm.dbKv.compact(txn, uint64(req.Revision))
				if err != nil {
					return err
				}
				entries[i].Result.Data, err = proto.Marshal(&internal.CompactionResponse{
					Header: sm.responseHeader(newRev),
				})
				if err != nil {
					return err
				}
				if err := sm.dbMeta.setRevisionMin(txn, uint64(req.Revision)); err != nil {
					return err
				}
				if err := sm.dbMeta.setRevisionCompacted(txn, rev); err != nil {
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
				success, err = sm.txnCompare(txn, req.Compare)
				if err != nil {
					return
				}
				var res = &internal.TxnResponse{
					Succeeded: success,
				}
				var affected [][]byte
				if success {
					res.Responses, affected, err = sm.txnOps(txn, newRev+1, epoch, req.Success)
				} else {
					res.Responses, affected, err = sm.txnOps(txn, newRev+1, epoch, req.Failure)
				}
				if len(affected) > 0 {
					newRev++
				}
				if err == internal.ErrGRPCDuplicateKey ||
					err == internal.ErrGRPCKeyTooLong ||
					err == internal.ErrGRPCEmptyKey {
					entries[i].Result.Data = []byte(err.Error())
					err = nil
				} else if err != nil {
					return
				} else {
					res.Header = sm.responseHeader(newRev)
					entries[i].Result.Data, err = proto.Marshal(res)
					entries[i].Result.Value = 1
					keys = append(keys, affected...)
				}
			case CMD_LEASE_GRANT:
				var req = &internal.LeaseGrantRequest{}
				if err = proto.Unmarshal(ent.Cmd[:len(ent.Cmd)-1], req); err != nil {
					sm.log.Error("Invalid command", "cmd", fmt.Sprintf("%x", ent.Cmd))
					continue
				}
				res, val, err := sm.cmdLeaseGrant(txn, epoch, req)
				if err != nil {
					return err
				}
				res.Header = sm.responseHeader(newRev)
				entries[i].Result.Data, err = proto.Marshal(res)
				entries[i].Result.Value = val
				if err != nil {
					return err
				}
			case CMD_LEASE_REVOKE:
				var req = &internal.LeaseRevokeRequest{}
				if err = proto.Unmarshal(ent.Cmd[:len(ent.Cmd)-1], req); err != nil {
					sm.log.Error("Invalid command", "cmd", fmt.Sprintf("%x", ent.Cmd))
					continue
				}
				affected, val, err := sm.cmdLeaseRevoke(txn, newRev+1, epoch, uint64(req.ID))
				if err != nil {
					return err
				}
				if len(affected) > 0 {
					newRev++
				}
				entries[i].Result.Data, err = proto.Marshal(&internal.LeaseRevokeResponse{
					Header: sm.responseHeader(newRev),
				})
				entries[i].Result.Value = val
				if err != nil {
					return err
				}
				keys = append(keys, affected...)
			case CMD_LEASE_KEEP_ALIVE:
				var req = &internal.LeaseKeepAliveRequest{}
				if err = proto.Unmarshal(ent.Cmd[:len(ent.Cmd)-1], req); err != nil {
					sm.log.Error("Invalid command", "cmd", fmt.Sprintf("%x", ent.Cmd))
					continue
				}
				res, val, err := sm.cmdLeaseKeepAlive(txn, epoch, req)
				if err != nil {
					return err
				}
				res.Header = sm.responseHeader(newRev)
				entries[i].Result.Data, err = proto.Marshal(res)
				entries[i].Result.Value = val
				if err != nil {
					return err
				}
			case CMD_INTERNAL_TICK:
				var req = &internal.TickRequest{}
				if err = proto.Unmarshal(ent.Cmd[:len(ent.Cmd)-1], req); err != nil {
					sm.log.Error("Invalid command", "cmd", fmt.Sprintf("%x", ent.Cmd))
					continue
				}
				term, err := sm.dbMeta.getTerm(txn)
				if err != nil {
					return err
				}
				if term > req.Term {
					entries[i].Result.Data = []byte(ErrTermExpired.Error())
					continue
				}
				epoch++
				if err = sm.dbMeta.setEpoch(txn, epoch); err != nil {
					return err
				}
				for id := range sm.dbLeaseExp.scan(txn, epoch) {
					affected, _, err := sm.cmdLeaseRevoke(txn, newRev+1, epoch, id)
					if err != nil {
						return err
					}
					if len(affected) > 0 {
						newRev++
						keys = append(keys, affected...)
					}
					sm.log.Debug("Lease Expired", "term", term, "epoch", epoch, "id", id, "keys", len(affected))
				}
				entries[i].Result.Data, err = proto.Marshal(&internal.TickResponse{
					Epoch: epoch,
				})
				if err != nil {
					return err
				}
				entries[i].Result.Value = ent.Index
			case CMD_INTERNAL_TERM:
				var req = &internal.TermRequest{}
				if err = proto.Unmarshal(ent.Cmd[:len(ent.Cmd)-1], req); err != nil {
					sm.log.Error("Invalid command", "cmd", fmt.Sprintf("%x", ent.Cmd))
					continue
				}
				term, err := sm.dbMeta.getTerm(txn)
				if err != nil {
					return err
				}
				if term > req.Term {
					entries[i].Result.Data = []byte(ErrTermExpired.Error())
					continue
				}
				if err = sm.dbMeta.setTerm(txn, req.Term); err != nil {
					return err
				}
				entries[i].Result.Data, err = proto.Marshal(&internal.TermResponse{})
				if err != nil {
					return err
				}
				entries[i].Result.Value = ent.Index
			}
		}
		if err = sm.dbMeta.setIndex(txn, entries[len(entries)-1].Index); err != nil {
			return
		}
		if newRev > rev {
			err = sm.dbMeta.setRevision(txn, newRev)
		}
		return
	}); err != nil {
		// TODO - Identify and log transient errors
		sm.log.Error(err.Error(), "index", entries[0].Index)
		panic("Storage error: " + err.Error())
	}
	sm.statTime += sm.clock.Since(t)
	if rev != newRev {
		localRevision.Store(newRev)
	}
	for _, w := range sm.watches.FindAny(keys...) {
		w <- newRev
	}
	return entries
}

func (sm *stateMachine) Query(ctx context.Context, query []byte) (res *Result) {
	res = zongzi.GetResult()
	var rev uint64
	switch query[len(query)-1] {
	case QUERY_KV_RANGE:
		var req = &internal.RangeRequest{}
		if err := proto.Unmarshal(query[:len(query)-1], req); err != nil {
			sm.log.Error("Invalid query", "query", fmt.Sprintf("%x", query))
			return
		}
		var resp *internal.RangeResponse
		err := sm.env.View(func(txn *lmdb.Txn) (err error) {
			rev, err = sm.dbMeta.getRevision(txn)
			if err != nil {
				return
			}
			resp, err = sm.queryRange(txn, rev, req)
			return
		})
		if err == internal.ErrGRPCCompacted || err == internal.ErrGRPCFutureRev {
			res.Data = []byte(err.Error())
			err = nil
		} else if err != nil {
			res.Data = []byte(err.Error())
			return
		} else {
			if res.Data, err = proto.Marshal(resp); err != nil {
				sm.log.Error("Invalid response", "res", query)
				return nil
			}
			res.Value = 1
		}
	case QUERY_LEASE_LEASES:
		var req = &internal.LeaseLeasesRequest{}
		if err := proto.Unmarshal(query[:len(query)-1], req); err != nil {
			sm.log.Error("Invalid query", "query", query)
			return
		}
		var resp *internal.LeaseLeasesResponse
		err := sm.env.View(func(txn *lmdb.Txn) (err error) {
			rev, err = sm.dbMeta.getRevision(txn)
			if err != nil {
				return
			}
			resp, err = sm.queryLeaseLeases(txn, req)
			return
		})
		if err != nil {
			sm.log.Error("Unknown error", "err", err)
			res.Data = []byte(err.Error())
			return
		}
		resp.Header = sm.responseHeader(rev)
		res.Data, err = proto.Marshal(resp)
		if err != nil {
			sm.log.Error("Invalid response", "resp", resp, "err", err)
			res.Data = []byte(err.Error())
			return
		}
		res.Value = 1
	case QUERY_LEASE_TIME_TO_LIVE:
		var req = &internal.LeaseTimeToLiveRequest{}
		if err := proto.Unmarshal(query[:len(query)-1], req); err != nil {
			sm.log.Error("Invalid query", "query", query)
			return
		}
		var resp *internal.LeaseTimeToLiveResponse
		err := sm.env.View(func(txn *lmdb.Txn) (err error) {
			rev, err = sm.dbMeta.getRevision(txn)
			if err != nil {
				return
			}
			resp, err = sm.queryLeaseTimeToLive(txn, req)
			return
		})
		if err != nil {
			sm.log.Error("Unknown error", "err", err)
			res.Data = []byte(err.Error())
			return
		}
		resp.Header = sm.responseHeader(rev)
		res.Data, err = proto.Marshal(resp)
		if err != nil {
			sm.log.Error("Invalid response", "resp", resp, "err", err)
			res.Data = []byte(err.Error())
			return
		}
		res.Value = 1
	case QUERY_WATCH_PROGRESS:
		err := sm.env.View(func(txn *lmdb.Txn) (err error) {
			rev, err = sm.dbMeta.getRevision(txn)
			if err != nil {
				return
			}
			return
		})
		resp := sm.responseHeader(rev)
		res.Data, err = proto.Marshal(resp)
		if err != nil {
			sm.log.Error("Invalid response", "resp", resp, "err", err)
			res.Data = []byte(err.Error())
			return
		}
		res.Value = 1
	}
	return
}

func (sm *stateMachine) Watch(ctx context.Context, query []byte, result chan<- *Result) {
	var req = &internal.WatchCreateRequest{}
	if err := proto.Unmarshal(query, req); err != nil {
		sm.log.Error("Invalid query", "query", query)
		return
	}
	// slog.Info(`sm Watch`, `req`, req)
	var err error
	var since = uint64(req.StartRevision)
	var min uint64
	err = sm.env.View(func(txn *lmdb.Txn) (err error) {
		if min, err = sm.dbMeta.getRevisionMin(txn); err != nil {
			return
		}
		// slog.Info(`sm Watch`, `since`, since, `min`, min)
		if since > 0 && min > since {
			err = internal.ErrGRPCCompacted
		}
		return
	})
	if err == internal.ErrGRPCCompacted {
		sm.log.Info("Watch compacted", "since", since, "min", min)
		res := zongzi.GetResult()
		res.Data = append(res.Data, WatchMessageType_ERR_COMPACTED)
		res.Data, err = sm.proto.MarshalAppend(res.Data, sm.responseHeader(min))
		if err != nil {
			sm.log.Error("Error notifying progress", "err", err)
			return
		}
		result <- res
		return
	} else if err != nil {
		sm.log.Error("Error checking min revision", "err", err)
		return
	}
	var filtered = map[uint8]bool{}
	for _, f := range req.Filters {
		filtered[uint8(f)] = true
	}
	scan := func() (rev uint64, sent int, err error) {
		// slog.Info(`sm Watch`, `req`, req)
		err = sm.env.View(func(txn *lmdb.Txn) (err error) {
			rev, err = sm.dbMeta.getRevision(txn)
			if err != nil {
				return
			}
			if since == 0 {
				return
			}
			for evt := range sm.dbKv.scan(txn, since) {
				if !bytes.Equal(evt.key, req.Key) {
					if len(req.RangeEnd) == 0 || bytes.Equal(req.Key, req.RangeEnd) {
						continue
					}
					if bytes.Compare(evt.key, req.Key) < 0 {
						continue
					}
					if bytes.Compare(evt.key, req.RangeEnd) >= 0 {
						continue
					}
				}
				if _, ok := filtered[evt.etype()]; ok {
					continue
				}
				var current, prev kv
				if evt.rev.isdel() {
					current = kv{key: evt.key, rev: evt.rev}
					if req.PrevKv {
						_, prev, err = sm.dbKv.getRev(txn, evt.key, evt.rev.upper(), req.PrevKv)
					}
				} else {
					current, prev, err = sm.dbKv.getRev(txn, evt.key, evt.rev.upper(), req.PrevKv)
					if err != nil {
						sm.log.Error("Error getting event kv", "key", evt.key, "rev", evt.rev.upper(), "prevKv", req.PrevKv)
						return
					}
				}
				var resp = &internal.Event{
					Type: internal.Event_EventType(evt.etype()),
				}
				if current.rev.upper() > 0 {
					resp.Kv = current.ToProto()
				}
				if prev.rev.upper() > 0 {
					resp.PrevKv = prev.ToProto()
				}
				res := zongzi.GetResult()
				res.Data = append(res.Data, WatchMessageType_EVENT)
				res.Data = binary.LittleEndian.AppendUint64(res.Data, rev)
				if res.Data, err = sm.proto.MarshalAppend(res.Data, resp); err != nil {
					sm.log.Error("Error serializing event kv", "err", err)
					return
				}
				result <- res
				sent++
			}
			if sent > 0 {
				res := zongzi.GetResult()
				res.Data = append(res.Data, WatchMessageType_SYNC)
				res.Data, err = sm.proto.MarshalAppend(res.Data, sm.responseHeader(rev))
				if err != nil {
					sm.log.Error("Error marshaling header", "err", err)
					return
				}
				result <- res
			}
			return
		})
		since = rev + 1
		return
	}
	// Send INIT
	res := zongzi.GetResult()
	res.Data = append(res.Data, WatchMessageType_INIT)
	res.Data, err = sm.proto.MarshalAppend(res.Data, sm.responseHeader(0))
	if err != nil {
		sm.log.Error("Error sending init", "err", err)
		return
	}
	result <- res
	var rev uint64
	// Event scan 1
	if rev, _, err = scan(); err != nil {
		sm.log.Error("Error scanning", "err", err)
		return
	}
	// Watch Start
	var alert = make(chan uint64, 1e3)
	if intv := sm.watches.Insert(req.Key, req.RangeEnd, alert); intv != nil {
		defer intv.Remove()
	} else {
		sm.log.Warn(`Invalid watch range`, `Key`, string(req.Key), `RangeEnd`, string(req.RangeEnd))
	}
	// Event scan 2
	if rev, _, err = scan(); err != nil {
		sm.log.Error("Error scanning", "err", err)
		return
	}
	var sent int
	var alertRev uint64
loop:
	for {
		select {
		case <-ctx.Done():
			sm.log.Debug("Watcher done", "id", req.WatchId)
			break loop
		case alertRev = <-alert:
		alertLoop:
			for {
				select {
				case alertRev = <-alert:
					sm.log.Debug("Watch Alert Drain", `alertRev`, alertRev)
				default:
					break alertLoop
				}
			}
			if alertRev <= rev {
				// Skip scan for alerts received between Watch start and Event scan 2
				sm.log.Debug("Watch Alert Skip", "rev", rev, "alertRev", alertRev)
				continue
			}
			rev, sent, err = scan()
			if err != nil {
				sm.log.Error("Error reading events", "err", err)
				break loop
			}
			if sent == 0 && req.ProgressNotify {
				res := zongzi.GetResult()
				res.Data = append(res.Data, WatchMessageType_NOTIFY)
				res.Data, err = sm.proto.MarshalAppend(res.Data, sm.responseHeader(rev))
				if err != nil {
					sm.log.Error("Error notifying progress", "err", err)
					break loop
				}
				result <- res
			}
		}
	}
}

func (sm *stateMachine) PrepareSnapshot() (cursor any, err error) {
	slog.Info(`PrepareSnapshot`)
	cursor, err = sm.env.BeginTxn(nil, lmdb.Readonly)
	slog.Info(`PrepareSnapshot done`, `err`, err)
	return
}

func (sm *stateMachine) SaveSnapshot(cursor any, w io.Writer, close <-chan struct{}) (err error) {
	slog.Info(`SaveSnapshot`)
	defer cursor.(*lmdb.Txn).Abort()
	f, err := os.OpenFile(sm.envPath+`/data.mdb`, os.O_RDONLY, 0700)
	if err != nil {
		return
	}
	_, err = io.Copy(w, f)
	f.Close()
	slog.Info(`SaveSnapshot done`, `err`, err)
	return
}

func (sm *stateMachine) RecoverFromSnapshot(r io.Reader, close <-chan struct{}) (err error) {
	slog.Info(`RecoverFromSnapshot`)
	f, err := os.OpenFile(sm.envPath+`/data.mdb`, os.O_WRONLY|os.O_CREATE, 0700)
	if err != nil {
		return
	}
	_, err = io.Copy(f, r)
	f.Close()
	slog.Info(`RecoverFromSnapshot done`, `err`, err)
	return
}

func (sm *stateMachine) Sync() error {
	return sm.env.Sync(true)
}

func (sm *stateMachine) Close() error {
	return sm.env.Close()
}

func (sm *stateMachine) cmdPut(
	txn *lmdb.Txn, rev, subrev, epoch uint64,
	req *internal.PutRequest,
) (res *internal.PutResponse, val uint64, affected [][]byte, err error) {
	res = &internal.PutResponse{}
	prev, _, patched, err := sm.dbKv.put(txn, rev, subrev, uint64(req.Lease), epoch, req.Key, req.Value, req.IgnoreValue, req.IgnoreLease)
	if err != nil {
		return
	}
	if patched {
		sm.statPatched++
	}
	if !req.IgnoreLease && int64(prev.lease) != req.Lease {
		if req.Lease > 0 {
			if err = sm.dbLeaseKey.put(txn, uint64(req.Lease), req.Key); err != nil {
				return
			}
		}
		if prev.lease > 0 {
			if err = sm.dbLeaseKey.del(txn, prev.lease, req.Key); err != nil {
				return
			}
		}
	}
	if req.PrevKv {
		res.PrevKv = prev.ToProto()
	}
	affected = append(affected, req.Key)
	val = 1
	return
}

func (sm *stateMachine) cmdDeleteRange(
	txn *lmdb.Txn, rev, subrev, epoch uint64,
	req *internal.DeleteRangeRequest,
) (res *internal.DeleteRangeResponse, keys [][]byte, err error) {
	res = &internal.DeleteRangeResponse{}
	prev, n, err := sm.dbKv.deleteRange(txn, rev, subrev, epoch, req.Key, req.RangeEnd)
	if err != nil {
		return
	}
	keys = make([][]byte, len(prev))
	var item kv
	for _, krec := range prev {
		keys = append(keys, krec.key)
		if krec.lease != 0 {
			if err = sm.dbLeaseKey.del(txn, krec.lease, krec.key); err != nil {
				return
			}
		}
	}
	res.Deleted = n
	res.Header = sm.responseHeader(rev)
	if req.PrevKv {
		for _, prec := range prev {
			item, _, err = sm.dbKv.getRev(txn, prec.key, prec.rev.upper(), false)
			res.PrevKvs = append(res.PrevKvs, item.ToProto())
		}
	}
	return
}

func (sm *stateMachine) cmdLeaseGrant(
	txn *lmdb.Txn, epoch uint64,
	req *internal.LeaseGrantRequest,
) (res *internal.LeaseGrantResponse, val uint64, err error) {
	res = &internal.LeaseGrantResponse{}
	item := lease{id: uint64(req.ID)}
	if item.id == 0 {
		if item.id, err = sm.dbMeta.getLeaseID(txn); err != nil {
			return
		}
		var found lease
		for {
			item.id++
			if found, err = sm.dbLease.get(txn, item.id); err != nil {
				return
			}
			if found.id == 0 {
				break
			}
		}
		if err = sm.dbMeta.setLeaseID(txn, item.id); err != nil {
			return
		}
	} else {
		if item, err = sm.dbLease.get(txn, item.id); err != nil {
			return
		}
		item.id = uint64(req.ID)
	}
	if item.expires > 0 {
		res.Error = internal.ErrGRPCDuplicateKey.Error()
	} else {
		item.renewed = epoch
		item.expires = epoch + uint64(req.TTL)
		if err = sm.dbLease.put(txn, item); err != nil {
			return
		}
		if err = sm.dbLeaseExp.put(txn, item); err != nil {
			return
		}
		res.ID = int64(item.id)
		res.TTL = req.TTL
	}
	val = 1
	return
}

func (sm *stateMachine) cmdLeaseRevoke(
	txn *lmdb.Txn, rev, epoch, id uint64,
) (keys [][]byte, val uint64, err error) {
	var item lease
	if item, err = sm.dbLease.get(txn, uint64(id)); err != nil {
		return
	}
	if item.id == 0 {
		val = uint64(codes.NotFound)
		return
	}
	var batch = make([][]byte, 100)
	for {
		if batch, err = sm.dbLeaseKey.sweep(txn, item.id, batch[:0]); err != nil {
			return
		}
		if len(batch) == 0 {
			break
		}
		if err = sm.dbKv.deleteBatch(txn, rev, 0, epoch, batch); err != nil {
			return
		}
		keys = append(keys, batch...)
	}
	if err = sm.dbLeaseExp.del(txn, item); err != nil {
		return
	}
	if err = sm.dbLease.del(txn, item.id); err != nil {
		return
	}
	return
}

func (sm *stateMachine) cmdLeaseKeepAlive(
	txn *lmdb.Txn, epoch uint64,
	req *internal.LeaseKeepAliveRequest,
) (res *internal.LeaseKeepAliveResponse, val uint64, err error) {
	res = &internal.LeaseKeepAliveResponse{ID: req.ID}
	val = 1
	var item lease
	if item, err = sm.dbLease.get(txn, uint64(req.ID)); err != nil {
		return
	}
	if item.id == 0 {
		return
	}
	res.TTL = int64(item.expires - item.renewed)
	item.expires = epoch + uint64(res.TTL)
	item.renewed = epoch
	if err = sm.dbLease.put(txn, item); err != nil {
		return
	}
	if err = sm.dbLeaseExp.put(txn, item); err != nil {
		return
	}
	return
}

func (sm *stateMachine) queryRange(
	txn *lmdb.Txn, rev uint64,
	req *internal.RangeRequest,
) (res *internal.RangeResponse, err error) {
	res = &internal.RangeResponse{
		Header: sm.responseHeader(rev),
	}
	if req.Revision > 0 {
		min, err := sm.dbMeta.getRevisionMin(txn)
		if err != nil {
			slog.Error(`Query err: ` + err.Error())
			return nil, err
		}
		if req.Revision < int64(min) {
			return nil, internal.ErrGRPCCompacted
		}
		if req.Revision > int64(rev) {
			return nil, internal.ErrGRPCFutureRev
		}
	}
	items, count, more, err := sm.dbKv.getRange(txn,
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
	if req.CountOnly || KRV_RANGE_COUNT_FULL || KRV_RANGE_COUNT_FAKE {
		res.Count = int64(count)
	}
	if !req.CountOnly {
		for _, kv := range items {
			res.Kvs = append(res.Kvs, kv.ToProto())
		}
		res.More = more
	}
	return
}

func (sm *stateMachine) queryLeaseLeases(
	txn *lmdb.Txn,
	_ *internal.LeaseLeasesRequest,
) (res *internal.LeaseLeasesResponse, err error) {
	res = &internal.LeaseLeasesResponse{}
	items, err := sm.dbLease.all(txn)
	if err != nil {
		return
	}
	for _, item := range items {
		res.Leases = append(res.Leases, &internal.LeaseStatus{ID: int64(item.id)})
	}
	return
}

func (sm *stateMachine) queryLeaseTimeToLive(
	txn *lmdb.Txn,
	req *internal.LeaseTimeToLiveRequest,
) (res *internal.LeaseTimeToLiveResponse, err error) {
	res = &internal.LeaseTimeToLiveResponse{}
	epoch, err := sm.dbMeta.getEpoch(txn)
	if err != nil {
		return
	}
	item, err := sm.dbLease.get(txn, uint64(req.ID))
	if err != nil {
		return
	}
	if item.expires > 0 {
		res.TTL = int64(item.expires - epoch)
	} else {
		res.TTL = -1
	}
	return
}

func (sm *stateMachine) txnOps(
	txn *lmdb.Txn, rev, epoch uint64,
	ops []*internal.RequestOp,
) (res []*internal.ResponseOp, keys [][]byte, err error) {
	err = txn.Sub(func(txn *lmdb.Txn) (err error) {
		for i, op := range ops {
			switch op.Request.(type) {
			case *internal.RequestOp_RequestPut:
				putReq := op.Request.(*internal.RequestOp_RequestPut).RequestPut
				putRes, _, affected, err := sm.cmdPut(txn, rev, uint64(i), epoch, putReq)
				if err != nil {
					return err
				}
				res = append(res, &internal.ResponseOp{
					Response: &internal.ResponseOp_ResponsePut{
						ResponsePut: putRes,
					},
				})
				keys = append(keys, affected...)
			case *internal.RequestOp_RequestDeleteRange:
				delReq := op.Request.(*internal.RequestOp_RequestDeleteRange).RequestDeleteRange
				delRes, affected, err := sm.cmdDeleteRange(txn, rev, uint64(i), epoch, delReq)
				if err != nil {
					return err
				}
				res = append(res, &internal.ResponseOp{
					Response: &internal.ResponseOp_ResponseDeleteRange{
						ResponseDeleteRange: delRes,
					},
				})
				keys = append(keys, affected...)
			case *internal.RequestOp_RequestRange:
				rangeReq := op.Request.(*internal.RequestOp_RequestRange).RequestRange
				rangeRes, err := sm.queryRange(txn, rev, rangeReq)
				if err != nil {
					return err
				}
				res = append(res, &internal.ResponseOp{
					Response: &internal.ResponseOp_ResponseRange{
						ResponseRange: rangeRes,
					},
				})
			}
		}
		return
	})
	return
}

func (sm *stateMachine) txnCompare(txn *lmdb.Txn, conds []*internal.Compare) (success bool, err error) {
	success = true
	var item kv
	for _, cond := range conds {
		if item, err = sm.dbKv.get(txn, cond.Key); err != nil {
			return
		}
		if len(item.key) > 0 && !bytes.Equal(cond.Key, item.key) {
			success = false
			break
		}
		switch cond.Target {
		case internal.Compare_VERSION:
			success = txnIntCompare(cond.Result, int64(item.version), cond.TargetUnion.(*internal.Compare_Version).Version)
		case internal.Compare_CREATE:
			success = txnIntCompare(cond.Result, int64(item.created), cond.TargetUnion.(*internal.Compare_CreateRevision).CreateRevision)
		case internal.Compare_MOD:
			success = txnIntCompare(cond.Result, int64(item.rev.upper()), cond.TargetUnion.(*internal.Compare_ModRevision).ModRevision)
		case internal.Compare_LEASE:
			success = txnIntCompare(cond.Result, int64(item.lease), cond.TargetUnion.(*internal.Compare_Lease).Lease)
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

func (sm *stateMachine) responseHeader(rev uint64) *internal.ResponseHeader {
	return &internal.ResponseHeader{
		Revision:  int64(rev),
		ClusterId: sm.shardID,
		MemberId:  sm.replicaID,
	}
}

func txnIntCompare(cond internal.Compare_CompareResult, a, b int64) bool {
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
