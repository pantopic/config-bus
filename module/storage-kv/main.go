package main

import (
	"bytes"

	"github.com/pantopic/wazero-atomic/sdk-go"
	"github.com/pantopic/wazero-lmdb/sdk-go"
	"github.com/pantopic/wazero-range-watch/sdk-go"
	"github.com/pantopic/wazero-small-cache/sdk-go"
	"github.com/pantopic/wazero-state-machine/sdk-go"

	internal "github.com/pantopic/turbokube/module/storage-kv/internal"
)

const (
	codeNotFound uint64 = 5
)
const (
	ATOMIC_UINT64_SET_GLOBAL = iota
	ATOMIC_UINT64_SET_WATCH_REV
)
const (
	ATOMIC_UINT64_GLOBAL_WATCH_ID_SEQ = iota
	ATOMIC_UINT64_GLOBAL_WATCH_PROGRESS
)
const (
	SMALL_CACHE_WATCH_CREATE_REQ = iota
)

var (
	epoch         uint64
	newIndex      uint64
	newRev        uint64
	oldRev        uint64
	txn           lmdb.Txn
	watchCache    = small_cache.NewLocal(SMALL_CACHE_WATCH_CREATE_REQ)
	watchID       = atomic.NewUint64Set(ATOMIC_UINT64_SET_GLOBAL).Find(ATOMIC_UINT64_GLOBAL_WATCH_ID_SEQ)
	watchProgress = atomic.NewUint64Set(ATOMIC_UINT64_SET_GLOBAL).Find(ATOMIC_UINT64_GLOBAL_WATCH_PROGRESS)
	watchRev      = atomic.NewUint64Set(ATOMIC_UINT64_SET_WATCH_REV)
)

func init() {
	statemachine.Persistent(open, update, finish, read)
	statemachine.Streaming(streamOpen, streamRecv, streamClosed)
	range_watch.Receive(rangeWatchRecv)
}

func main() {}

func open() (index uint64) {
	if err := lmdb.Update(func(txn lmdb.Txn) (err error) {
		index = dbMeta.init(txn)
		dbStats.init(txn)
		kvStore.init(txn)
		dbLease.init(txn)
		dbLeaseExp.init(txn)
		dbLeaseKey.init(txn)
		return nil
	}); err != nil {
		panic(`Unable to open env ` + err.Error())
	}
	return
}

func update(index uint64, cmd []byte) (value uint64, data []byte) {
	newIndex = index
	var err error
	var rev uint64
	if txn == 0 {
		txn, err = lmdb.Begin(0)
		if err != nil {
			panic(`Unable to open txn: ` + err.Error())
		}
		epoch, err = dbMeta.getEpoch(txn)
		if err != nil {
			panic(`Unable to get epoch: ` + err.Error())
		}
		rev, err = dbMeta.getRevision(txn)
		if err != nil {
			panic(`Unable to get revision: ` + err.Error())
		}
		newRev = rev
		oldRev = rev
	}
	switch cmd[len(cmd)-1] {
	case CMD_KV_PUT:
		// TODO - Add sync pool for protobuf messages
		var req = &internal.PutRequest{}
		if err = req.UnmarshalVT(cmd[:len(cmd)-1]); err != nil {
			data = []byte(`Invalid command: ` + string(cmd))
			return
		}
		if req.IgnoreLease && req.Lease != 0 {
			data = []byte(ErrGRPCLeaseProvided.Error())
			return
		}
		if req.IgnoreValue && len(req.Value) != 0 {
			data = []byte(ErrGRPCValueProvided.Error())
			return
		}
		var item lease
		if req.Lease != 0 {
			item, err = dbLease.get(txn, uint64(req.Lease))
			if err != nil {
				return
			}
			if item.id == 0 {
				data = []byte(ErrGRPCLeaseNotFound.Error())
				return
			}
		}
		res, val, affected, err := cmdPut(txn, newRev+1, 0, epoch, req)
		if err == ErrGRPCKeyTooLong ||
			err == ErrGRPCEmptyKey {
			data = []byte(err.Error())
			return
		} else if err != nil {
			panic(`Unable to put: ` + err.Error())
		}
		if len(affected) > 0 {
			newRev++
			range_watch.Queue(newRev, affected)
		}
		res.Header = responseHeader(newRev)
		data, err = res.MarshalVT()
		if err != nil {
			panic(`Unable to marshal response: ` + err.Error())
		}
		value = val
	case CMD_KV_DELETE_RANGE:
		var req = &internal.DeleteRangeRequest{}
		if err = req.UnmarshalVT(cmd[:len(cmd)-1]); err != nil {
			data = []byte(`Invalid command: ` + string(cmd))
			return
		}
		resDel, affected, err := cmdDeleteRange(txn, newRev+1, 0, epoch, req)
		if err != nil {
			panic(`Unable to delete range: ` + err.Error())
		}
		if len(affected) > 0 {
			newRev++
			range_watch.Queue(newRev, affected)
		}
		resDel.Header = responseHeader(newRev)
		data, err = resDel.MarshalVT()
		if err != nil {
			panic(`Unable to marshal response: ` + err.Error())
		}
		value = 1
	case CMD_KV_COMPACT:
		var req = &internal.CompactionRequest{}
		if err = req.UnmarshalVT(cmd[:len(cmd)-1]); err != nil {
			data = []byte(`Invalid command: ` + string(cmd))
			return
		}
		data, err = (&internal.CompactionResponse{
			Header: responseHeader(newRev),
		}).MarshalVT()
		if err != nil {
			data = []byte(err.Error())
			return
		}
		if err := dbMeta.setRevisionMin(txn, uint64(req.Revision)); err != nil {
			data = []byte(err.Error())
			return
		}
		value = 1
	case CMD_KV_TXN:
		var req = &internal.TxnRequest{}
		if err = req.UnmarshalVT(cmd[:len(cmd)-1]); err != nil {
			data = []byte(`Invalid command: ` + string(cmd))
			return
		}
		var success bool
		success, err = txnCompare(txn, req.Compare)
		if err != nil {
			println(`txn compare fail`)
			return
		}
		var res = &internal.TxnResponse{
			Succeeded: success,
		}
		var affected [][]byte
		if success {
			res.Responses, affected, err = txnOps(txn, newRev+1, epoch, req.Success)
		} else {
			res.Responses, affected, err = txnOps(txn, newRev+1, epoch, req.Failure)
		}
		if len(affected) > 0 {
			newRev++
			range_watch.Queue(newRev, affected)
		}
		if err == ErrGRPCDuplicateKey ||
			err == ErrGRPCKeyTooLong ||
			err == ErrGRPCEmptyKey {
			data = []byte(err.Error())
			err = nil
		} else if err != nil {
			return
		} else {
			res.Header = responseHeader(newRev)
			data, err = res.MarshalVT()
			if err != nil {
				panic(`Unable to marshal response: ` + err.Error())
			}
			value = 1
		}
	case CMD_LEASE_GRANT:
		var req = &internal.LeaseGrantRequest{}
		if err = req.UnmarshalVT(cmd[:len(cmd)-1]); err != nil {
			data = []byte(`Invalid command: ` + string(cmd))
			return
		}
		res, val, err := cmdLeaseGrant(txn, epoch, req)
		if err != nil {
			panic(`Unable to grant lease: ` + err.Error())
		}
		res.Header = responseHeader(newRev)
		data, err = res.MarshalVT()
		value = val
		if err != nil {
			panic(`Unable to marshal response: ` + err.Error())
		}
	case CMD_LEASE_REVOKE:
		var req = &internal.LeaseRevokeRequest{}
		if err = req.UnmarshalVT(cmd[:len(cmd)-1]); err != nil {
			data = []byte(`Invalid command: ` + string(cmd))
			return
		}
		affected, val, err := cmdLeaseRevoke(txn, newRev+1, epoch, uint64(req.ID))
		if err != nil {
			panic(`Unable to revoke lease: ` + err.Error())
		}
		if len(affected) > 0 {
			newRev++
			range_watch.Queue(newRev, affected)
		}
		data, err = (&internal.LeaseRevokeResponse{
			Header: responseHeader(newRev),
		}).MarshalVT()
		if err != nil {
			panic(`Unable to marshal response: ` + err.Error())
		}
		value = val
	case CMD_LEASE_KEEP_ALIVE:
		var req = &internal.LeaseKeepAliveRequest{}
		if err = req.UnmarshalVT(cmd[:len(cmd)-1]); err != nil {
			data = []byte(`Invalid command: ` + string(cmd))
			return
		}
		res, val, err := cmdLeaseKeepAlive(txn, epoch, req)
		if err != nil {
			panic(`Unable to keep lease alive: ` + err.Error())
		}
		res.Header = responseHeader(newRev)
		data, err = res.MarshalVT()
		if err != nil {
			panic(`Unable to marshal response: ` + err.Error())
		}
		value = val
	case CMD_LEASE_KEEP_ALIVE_BATCH:
		var req = &internal.LeaseKeepAliveBatchRequest{}
		if err = req.UnmarshalVT(cmd[:len(cmd)-1]); err != nil {
			data = []byte(`Invalid command: ` + string(cmd))
			return
		}
		res, val, err := cmdLeaseKeepAliveBatch(txn, epoch, req)
		if err != nil {
			panic(`Unable to keep lease alive batch: ` + err.Error())
		}
		res.Header = responseHeader(newRev)
		data, err = res.MarshalVT()
		if err != nil {
			panic(`Unable to marshal response: ` + err.Error())
		}
		value = val
	case CMD_INTERNAL_TICK:
		var req = &internal.TickRequest{}
		if err = req.UnmarshalVT(cmd[:len(cmd)-1]); err != nil {
			data = []byte(`Invalid command: ` + string(cmd))
			return
		}
		term, err := dbMeta.getTerm(txn)
		if err != nil {
			panic(`Unable to get term: ` + err.Error())
		}
		if term > req.Term {
			data = []byte(ErrTermExpired.Error())
			return
		}
		epoch++
		if err = dbMeta.setEpoch(txn, epoch); err != nil {
			panic(`Unable to set epoch: ` + err.Error())
		}
		// lease expire
		for id := range dbLeaseExp.scan(txn, epoch) {
			affected, _, err := cmdLeaseRevoke(txn, newRev+1, epoch, id)
			if err != nil {
				panic(`Unable to revoke lease: ` + err.Error())
			}
			if len(affected) > 0 {
				newRev++
				range_watch.Queue(newRev, affected)
			}
		}
		// revision compact
		min, err := dbMeta.getRevisionMin(txn)
		if err != nil {
			data = []byte(err.Error())
			return
		}
		rev, err := kvStore.compact(txn, min)
		if err != nil {
			data = []byte(err.Error())
			return
		}
		if err := dbMeta.setRevisionCompacted(txn, rev); err != nil {
			data = []byte(err.Error())
			return
		}
		data, err = (&internal.TickResponse{
			Epoch: epoch,
		}).MarshalVT()
		if err != nil {
			panic(`Unable to marshal response: ` + err.Error())
		}
		value = index
	case CMD_INTERNAL_TERM:
		var req = &internal.TermRequest{}
		if err = req.UnmarshalVT(cmd[:len(cmd)-1]); err != nil {
			data = []byte(`Invalid command: ` + string(cmd))
			return
		}
		term, err := dbMeta.getTerm(txn)
		if err != nil {
			panic(`Unable to get term: ` + err.Error())
		}
		if term > req.Term {
			data = []byte(ErrTermExpired.Error())
			return
		}
		if err = dbMeta.setTerm(txn, req.Term); err != nil {
			panic(`Unable to set term: ` + err.Error())
		}
		data, err = (&internal.TermResponse{}).MarshalVT()
		if err != nil {
			panic(`Unable to marshal response: ` + err.Error())
		}
		value = index
	}
	return
}

func finish() {
	var err error
	if err = dbMeta.setIndex(txn, newIndex); err != nil {
		panic(`Unable to set index: ` + err.Error())
	}
	if newRev > oldRev {
		err = dbMeta.setRevision(txn, newRev)
		if err != nil {
			panic(`Unable to set revision: ` + err.Error())
		}
	}
	if err := txn.Commit(); err != nil {
		panic(`Unable to commit transaction: ` + err.Error())
	}
	range_watch.Flush()
	txn = 0
}

func read(query []byte) (value uint64, data []byte) {
	var rev uint64
	switch query[len(query)-1] {
	case QUERY_KV_RANGE:
		var req = &internal.RangeRequest{}
		if err := req.UnmarshalVT(query[:len(query)-1]); err != nil {
			data = []byte("Invalid query: " + string(query))
			return
		}
		var resp *internal.RangeResponse
		err := lmdb.View(func(txn lmdb.Txn) (err error) {
			rev, err = dbMeta.getRevision(txn)
			if err != nil {
				return
			}
			resp, err = queryRange(txn, rev, req)
			return
		})
		if err == ErrGRPCCompacted || err == ErrGRPCFutureRev {
			data = []byte(err.Error())
			err = nil
		} else if err != nil {
			data = []byte(err.Error())
			return
		} else {
			if data, err = resp.MarshalVT(); err != nil {
				data = []byte(err.Error())
				return
			}
			value = 1
		}
	case QUERY_LEASE_LEASES:
		var req = &internal.LeaseLeasesRequest{}
		if err := req.UnmarshalVT(query[:len(query)-1]); err != nil {
			data = []byte("Invalid query: " + string(query))
			return
		}
		var resp *internal.LeaseLeasesResponse
		err := lmdb.View(func(txn lmdb.Txn) (err error) {
			rev, err = dbMeta.getRevision(txn)
			if err != nil {
				return
			}
			resp, err = queryLeaseLeases(txn, req)
			return
		})
		if err != nil {
			data = []byte(err.Error())
			return
		}
		resp.Header = responseHeader(rev)
		data, err = resp.MarshalVT()
		if err != nil {
			data = []byte(err.Error())
			return
		}
		value = 1
	case QUERY_LEASE_TIME_TO_LIVE:
		var req = &internal.LeaseTimeToLiveRequest{}
		if err := req.UnmarshalVT(query[:len(query)-1]); err != nil {
			data = []byte("Invalid query: " + string(query))
			return
		}
		var resp *internal.LeaseTimeToLiveResponse
		err := lmdb.View(func(txn lmdb.Txn) (err error) {
			rev, err = dbMeta.getRevision(txn)
			if err != nil {
				return
			}
			resp, err = queryLeaseTimeToLive(txn, req)
			return
		})
		if err != nil {
			data = []byte(err.Error())
			return
		}
		resp.Header = responseHeader(rev)
		data, err = resp.MarshalVT()
		if err != nil {
			data = []byte(err.Error())
			return
		}
		value = 1
	case QUERY_WATCH_PROGRESS:
		var err error
		resp := responseHeader(watchProgress.Load())
		data, err = resp.MarshalVT()
		if err != nil {
			data = []byte(err.Error())
			return
		}
		value = 1
	case QUERY_HEADER:
		err := lmdb.View(func(txn lmdb.Txn) (err error) {
			rev, err = dbMeta.getRevision(txn)
			if err != nil {
				return
			}
			return
		})
		resp := responseHeader(rev)
		data, err = resp.MarshalVT()
		if err != nil {
			data = []byte(err.Error())
			return
		}
		value = 1
	}
	return
}

func cmdPut(
	txn lmdb.Txn, rev, subrev, epoch uint64,
	req *internal.PutRequest,
) (res *internal.PutResponse, val uint64, affected [][]byte, err error) {
	res = &internal.PutResponse{}
	prev, _, _, err := kvStore.put(txn, rev, subrev, uint64(req.Lease), epoch, req.Key, req.Value, req.IgnoreValue, req.IgnoreLease)
	if err != nil {
		println(`put err ` + err.Error())
		return
	}
	if !req.IgnoreLease && int64(prev.lease) != req.Lease {
		if req.Lease > 0 {
			if err = dbLeaseKey.put(txn, uint64(req.Lease), req.Key); err != nil {
				return
			}
		}
		if prev.lease > 0 {
			if err = dbLeaseKey.del(txn, prev.lease, req.Key); err != nil {
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

func cmdDeleteRange(
	txn lmdb.Txn, rev, subrev, epoch uint64,
	req *internal.DeleteRangeRequest,
) (res *internal.DeleteRangeResponse, keys [][]byte, err error) {
	res = &internal.DeleteRangeResponse{}
	prev, n, err := kvStore.deleteRange(txn, rev, subrev, epoch, req.Key, req.RangeEnd)
	if err != nil {
		return
	}
	keys = make([][]byte, len(prev))
	var item kv
	for _, krec := range prev {
		keys = append(keys, krec.key)
		if krec.lease != 0 {
			if err = dbLeaseKey.del(txn, krec.lease, krec.key); err != nil {
				return
			}
		}
	}
	res.Deleted = n
	res.Header = responseHeader(rev)
	if req.PrevKv {
		for _, prec := range prev {
			item, _, err = kvStore.getRev(txn, prec.key, prec.rev.upper(), false)
			res.PrevKvs = append(res.PrevKvs, item.ToProto())
		}
	}
	return
}

var subTxn = new(lmdb.Txn)

func txnOps(
	txn lmdb.Txn, rev, epoch uint64,
	ops []*internal.RequestOp,
) (res []*internal.ResponseOp, keys [][]byte, err error) {
	err = txn.Sub(func(txn lmdb.Txn) (err error) {
		for i, op := range ops {
			switch req := op.Request.(type) {
			case *internal.RequestOp_RequestPut:
				putReq := req.RequestPut
				putRes, _, affected, err := cmdPut(txn, rev, uint64(i), epoch, putReq)
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
				delReq := req.RequestDeleteRange
				delRes, affected, err := cmdDeleteRange(txn, rev, uint64(i), epoch, delReq)
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
				rangeReq := req.RequestRange
				rangeRes, err := queryRange(txn, rev, rangeReq)
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

func txnCompare(txn lmdb.Txn, conds []*internal.Compare) (success bool, err error) {
	success = true
	var item kv
	for _, cond := range conds {
		if item, err = kvStore.get(txn, cond.Key); err != nil {
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

func cmdLeaseGrant(
	txn lmdb.Txn, epoch uint64,
	req *internal.LeaseGrantRequest,
) (res *internal.LeaseGrantResponse, val uint64, err error) {
	res = &internal.LeaseGrantResponse{}
	item := lease{id: uint64(req.ID)}
	if item.id == 0 {
		if item.id, err = dbMeta.getLeaseID(txn); err != nil {
			return
		}
		var found lease
		for {
			item.id++
			if found, err = dbLease.get(txn, item.id); err != nil {
				return
			}
			if found.id == 0 {
				break
			}
		}
		if err = dbMeta.setLeaseID(txn, item.id); err != nil {
			return
		}
	} else {
		if item, err = dbLease.get(txn, item.id); err != nil {
			return
		}
		item.id = uint64(req.ID)
	}
	if item.expires > 0 {
		res.Error = ErrGRPCLeaseExist.Error()
		return
	} else {
		item.renewed = epoch
		item.expires = epoch + uint64(req.TTL)
		if err = dbLease.put(txn, item); err != nil {
			return
		}
		if err = dbLeaseExp.put(txn, item); err != nil {
			return
		}
		res.ID = int64(item.id)
		res.TTL = req.TTL
	}
	val = 1
	return
}

func cmdLeaseRevoke(
	txn lmdb.Txn, rev, epoch, id uint64,
) (keys [][]byte, val uint64, err error) {
	var item lease
	if item, err = dbLease.get(txn, uint64(id)); err != nil {
		return
	}
	if item.id == 0 {
		val = uint64(codeNotFound)
		return
	}
	var batch = make([][]byte, 100)
	for {
		if batch, err = dbLeaseKey.sweep(txn, item.id, batch[:0]); err != nil {
			return
		}
		if len(batch) == 0 {
			break
		}
		if err = kvStore.deleteBatch(txn, rev, 0, epoch, batch); err != nil {
			return
		}
		keys = append(keys, batch...)
	}
	if err = dbLeaseExp.del(txn, item); err != nil {
		return
	}
	if err = dbLease.del(txn, item.id); err != nil {
		return
	}
	val = 1
	return
}

func cmdLeaseKeepAlive(
	txn lmdb.Txn, epoch uint64,
	req *internal.LeaseKeepAliveRequest,
) (res *internal.LeaseKeepAliveResponse, val uint64, err error) {
	res = &internal.LeaseKeepAliveResponse{ID: req.ID}
	val = 1
	var item lease
	if item, err = dbLease.get(txn, uint64(req.ID)); err != nil {
		return
	}
	if item.id == 0 {
		return
	}
	res.TTL = int64(item.expires - item.renewed)
	item.expires = epoch + uint64(res.TTL)
	item.renewed = epoch
	if err = dbLease.put(txn, item); err != nil {
		return
	}
	if err = dbLeaseExp.put(txn, item); err != nil {
		return
	}
	return
}

func cmdLeaseKeepAliveBatch(
	txn lmdb.Txn, epoch uint64,
	req *internal.LeaseKeepAliveBatchRequest,
) (res *internal.LeaseKeepAliveBatchResponse, val uint64, err error) {
	res = &internal.LeaseKeepAliveBatchResponse{}
	val = 1
	for _, id := range req.IDs {
		var item lease
		if item, err = dbLease.get(txn, uint64(id)); err != nil {
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
		if err = dbLease.put(txn, item); err != nil {
			return
		}
		if err = dbLeaseExp.put(txn, item); err != nil {
			return
		}
	}
	return
}

func queryRange(
	txn lmdb.Txn, rev uint64,
	req *internal.RangeRequest,
) (res *internal.RangeResponse, err error) {
	res = &internal.RangeResponse{
		Header: responseHeader(rev),
	}
	if req.Revision > 0 {
		min, err := dbMeta.getRevisionMin(txn)
		if err != nil {
			return nil, err
		}
		if req.Revision < int64(min) {
			return nil, ErrGRPCCompacted
		}
		if req.Revision > int64(rev) {
			return nil, ErrGRPCFutureRev
		}
	}
	items, count, more, err := kvStore.getRange(txn,
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
	if req.CountOnly || RANGE_COUNT_FULL() || RANGE_COUNT_FAKE() {
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

func queryLeaseLeases(
	txn lmdb.Txn,
	_ *internal.LeaseLeasesRequest,
) (res *internal.LeaseLeasesResponse, err error) {
	res = &internal.LeaseLeasesResponse{}
	items, err := dbLease.all(txn)
	if err != nil {
		return
	}
	for _, item := range items {
		res.Leases = append(res.Leases, &internal.LeaseStatus{ID: int64(item.id)})
	}
	return
}

func queryLeaseTimeToLive(
	txn lmdb.Txn,
	req *internal.LeaseTimeToLiveRequest,
) (res *internal.LeaseTimeToLiveResponse, err error) {
	res = &internal.LeaseTimeToLiveResponse{}
	epoch, err := dbMeta.getEpoch(txn)
	if err != nil {
		return
	}
	item, err := dbLease.get(txn, uint64(req.ID))
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

func responseHeader(revision uint64) *internal.ResponseHeader {
	return &internal.ResponseHeader{
		Revision:  int64(revision),
		ClusterId: statemachine.ShardID,
		MemberId:  statemachine.ReplicaID,
	}
}
