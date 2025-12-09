package krv

import (
	"bytes"
	"context"
	"encoding/binary"
	"fmt"
	"io"
	"log/slog"
	"os"

	"github.com/PowerDNS/lmdb-go/lmdb"
	"github.com/benbjohnson/clock"
	"github.com/logbn/byteinterval"
	"github.com/logbn/zongzi"
	"google.golang.org/grpc/codes"
	"google.golang.org/protobuf/proto"

	"github.com/pantopic/krv/internal"
)

const UriLeaseExp = "zongzi://github.com/pantopic/krv"

type stateMachineLeaseExp struct {
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
}

func NewStateMachineLeaseExpFactory(logger *slog.Logger, dataDir string) zongzi.StateMachineFactory {
	return func(shardID uint64, replicaID uint64) zongzi.StateMachine {
		return &stateMachineLeaseExp{
			shardID:   shardID,
			replicaID: replicaID,
			envPath:   fmt.Sprintf("%s/%08x/%08x", dataDir, shardID, replicaID),
			log:       logger,
			clock:     clock.New(),
			watches:   byteinterval.New[chan uint64](),
		}
	}
}

var _ zongzi.StateMachineFactory = NewStateMachineLeaseExpFactory(nil, "")

func (sm *stateMachineLeaseExp) Update(entries []Entry) []Entry {
	var rev, newRev uint64
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
			case CMD_LEASE_KEEP_ALIVE_BATCH:
				var req = &internal.LeaseKeepAliveBatchRequest{}
				if err = proto.Unmarshal(ent.Cmd[:len(ent.Cmd)-1], req); err != nil {
					sm.log.Error("Invalid command", "cmd", fmt.Sprintf("%x", ent.Cmd))
					continue
				}
				res, val, err := sm.cmdLeaseKeepAliveBatch(txn, epoch, req)
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
	if rev != newRev {
		localRevision.Store(newRev)
	}
	for _, w := range sm.watches.FindAny(keys...) {
		w <- newRev
	}
	return entries
}

func (sm *stateMachineLeaseExp) Query(ctx context.Context, query []byte) (res *Result) {
	res = zongzi.GetResult()
	var rev uint64
	switch query[len(query)-1] {
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
	}
	return
}

func (sm *stateMachineLeaseExp) Watch(ctx context.Context, query []byte, result chan<- *Result) {
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

func (sm *stateMachineLeaseExp) PrepareSnapshot() (cursor any, err error) {
	slog.Info(`PrepareSnapshot`)
	cursor, err = sm.env.BeginTxn(nil, lmdb.Readonly)
	slog.Info(`PrepareSnapshot done`, `err`, err)
	return
}

func (sm *stateMachineLeaseExp) SaveSnapshot(cursor any, w io.Writer, close <-chan struct{}) (err error) {
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

func (sm *stateMachineLeaseExp) RecoverFromSnapshot(r io.Reader, sf []zongzi.SnapshotFile, close <-chan struct{}) {
	slog.Info(`RecoverFromSnapshot`)
	// read data
	slog.Info(`RecoverFromSnapshot done`)
}

func (sm *stateMachineLeaseExp) Close() error {
	return sm.env.Close()
}

func (sm *stateMachineLeaseExp) cmdLeaseGrant(
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

func (sm *stateMachineLeaseExp) cmdLeaseRevoke(
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

func (sm *stateMachineLeaseExp) cmdLeaseKeepAlive(
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

func (sm *stateMachineLeaseExp) cmdLeaseKeepAliveBatch(
	txn *lmdb.Txn, epoch uint64,
	req *internal.LeaseKeepAliveBatchRequest,
) (res *internal.LeaseKeepAliveBatchResponse, val uint64, err error) {
	res = &internal.LeaseKeepAliveBatchResponse{}
	val = 1
	for _, id := range req.IDs {
		var item lease
		if item, err = sm.dbLease.get(txn, uint64(id)); err != nil {
			return
		}
		if item.id == 0 {
			res.TTLs = append(res.TTLs, 0)
			continue
		}
		ttl := int64(item.expires - item.renewed)
		res.TTLs = append(res.TTLs, ttl)
		item.expires = epoch + uint64(ttl)
		item.renewed = epoch
		if err = sm.dbLease.put(txn, item); err != nil {
			return
		}
		if err = sm.dbLeaseExp.put(txn, item); err != nil {
			return
		}
	}
	return
}

func (sm *stateMachineLeaseExp) queryLeaseLeases(
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

func (sm *stateMachineLeaseExp) queryLeaseTimeToLive(
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

func (sm *stateMachineLeaseExp) responseHeader(rev uint64) *internal.ResponseHeader {
	return &internal.ResponseHeader{
		Revision:  int64(rev),
		ClusterId: sm.shardID,
		MemberId:  sm.replicaID,
	}
}
