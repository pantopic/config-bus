package main

import (
	"bytes"

	sm "github.com/pantopic/cluster/guest/state_machine"
	"github.com/pantopic/wazero-lmdb/lmdb-go"

	internal "github.com/pantopic/krv/module/storage/internal"
)

const (
	codeNotFound uint64 = 5
)

var (
	txn       *lmdb.Txn
	epoch     uint64
	rev       uint64
	newRev    uint64
	newIndex  uint64
	shardID   uint64
	replicaID uint64
	keys      [][]byte
)

func main() {
	sm.RegisterPersistent(
		open,
		update,
		finish,
		read,
	)
}

func open() (index uint64) {
	lmdb.Update(func(txn *lmdb.Txn) (err error) {
		index = dbMeta.init(txn)
		dbStats.init(txn)
		kvStore.init(txn)
		dbLease.init(txn)
		dbLeaseExp.init(txn)
		dbLeaseKey.init(txn)
		return nil
	})
	return
}

func update(index uint64, cmd []byte) (value uint64, data []byte) {
	newIndex = index
	var err error
	if txn == nil {
		txn, err = lmdb.BeginTxn(nil, 0)
		if err != nil {
			panic(err)
		}
		epoch, err = dbMeta.getEpoch(txn)
		if err != nil {
			panic(err)
		}
		rev, err = dbMeta.getRevision(txn)
		if err != nil {
			panic(err)
		}
		keys = keys[:0]
		// shardID, replicaID = sm.ShardMeta()
	}
	switch cmd[len(cmd)-1] {
	case CMD_KV_PUT:
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
				data = []byte(err.Error())
				return
			}
			if item.id == 0 {
				data = []byte(ErrGRPCLeaseNotFound.Error())
				return
			}
		}
		res, val, affected, err := cmdPut(txn, index, 0, epoch, req)
		if err == ErrGRPCKeyTooLong {
			data = []byte(err.Error())
			return
		} else if err != nil {
			panic(err)
		}
		res.Header = responseHeader(index)
		data, err = res.MarshalVT()
		if err != nil {
			panic(err)
		}
		value = val
		newRev = index
		keys = append(keys, affected...)
	case CMD_KV_DELETE_RANGE:
		var req = &internal.DeleteRangeRequest{}
		if err = req.UnmarshalVT(cmd[:len(cmd)-1]); err != nil {
			data = []byte(`Invalid command: ` + string(cmd))
			return
		}
		resDel, affected, err := cmdDeleteRange(txn, index, 0, epoch, req)
		if err != nil {
			panic(err)
		}
		data, err = resDel.MarshalVT()
		if err != nil {
			panic(err)
		}
		value = 1
		newRev = index
		keys = append(keys, affected...)
	case CMD_KV_COMPACT:
		var req = &internal.CompactionRequest{}
		if err = req.UnmarshalVT(cmd[:len(cmd)-1]); err != nil {
			data = []byte(`Invalid command: ` + string(cmd))
			return
		}
		// TODO - Add support for asynchronous compaction w/ req.Physical
		index, err := kvStore.compact(txn, uint64(req.Revision))
		if err != nil {
			data = []byte(err.Error())
			return
		}
		data, err = (&internal.CompactionResponse{
			Header: responseHeader(index),
		}).MarshalVT()
		if err != nil {
			data = []byte(err.Error())
			return
		}
		if err := dbMeta.setRevisionMin(txn, uint64(req.Revision)); err != nil {
			data = []byte(err.Error())
			return
		}
		if err := dbMeta.setRevisionCompacted(txn, index); err != nil {
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
			return
		}
		var res = &internal.TxnResponse{
			Succeeded: success,
		}
		var affected [][]byte
		if success {
			res.Responses, affected, err = txnOps(txn, index, epoch, req.Success)
		} else {
			res.Responses, affected, err = txnOps(txn, index, epoch, req.Failure)
		}
		if err == ErrGRPCDuplicateKey ||
			err == ErrGRPCKeyTooLong {
			data = []byte(err.Error())
			err = nil
		} else if err != nil {
			return
		} else {
			res.Header = responseHeader(index)
			data, err = res.MarshalVT()
			value = 1
			newRev = index
			keys = append(keys, affected...)
		}
	case CMD_LEASE_GRANT:
		var req = &internal.LeaseGrantRequest{}
		if err = req.UnmarshalVT(cmd[:len(cmd)-1]); err != nil {
			data = []byte(`Invalid command: ` + string(cmd))
			return
		}
		res, val, err := cmdLeaseGrant(txn, epoch, req)
		if err != nil {
			panic(err)
		}
		res.Header = responseHeader(index)
		data, err = res.MarshalVT()
		value = val
		if err != nil {
			panic(err)
		}
	case CMD_LEASE_REVOKE:
		var req = &internal.LeaseRevokeRequest{}
		if err = req.UnmarshalVT(cmd[:len(cmd)-1]); err != nil {
			data = []byte(`Invalid command: ` + string(cmd))
			return
		}
		affected, val, err := cmdLeaseRevoke(txn, index, epoch, uint64(req.ID))
		if err != nil {
			panic(err)
		}
		data, err = (&internal.LeaseRevokeResponse{
			Header: responseHeader(index),
		}).MarshalVT()
		value = val
		if err != nil {
			panic(err)
		}
		if len(affected) > 0 {
			newRev = index
		}
		keys = append(keys, affected...)
	case CMD_LEASE_KEEP_ALIVE:
		var req = &internal.LeaseKeepAliveRequest{}
		if err = req.UnmarshalVT(cmd[:len(cmd)-1]); err != nil {
			data = []byte(`Invalid command: ` + string(cmd))
			return
		}
		res, val, err := cmdLeaseKeepAlive(txn, epoch, req)
		if err != nil {
			panic(err)
		}
		res.Header = responseHeader(index)
		data, err = res.MarshalVT()
		value = val
		if err != nil {
			panic(err)
		}
	case CMD_INTERNAL_TICK:
		var req = &internal.TickRequest{}
		if err = req.UnmarshalVT(cmd[:len(cmd)-1]); err != nil {
			data = []byte(`Invalid command: ` + string(cmd))
			return
		}
		term, err := dbMeta.getTerm(txn)
		if err != nil {
			panic(err)
		}
		if term > req.Term {
			data = []byte(ErrTermExpired.Error())
			return
		}
		epoch++
		if err = dbMeta.setEpoch(txn, epoch); err != nil {
			panic(err)
		}
		for id := range dbLeaseExp.scan(txn, epoch) {
			affected, _, err := cmdLeaseRevoke(txn, index, epoch, id)
			if err != nil {
				panic(err)
			}
			if len(affected) > 0 {
				newRev = index
				keys = append(keys, affected...)
			}
		}
		data, err = (&internal.TickResponse{
			Epoch: epoch,
		}).MarshalVT()
		if err != nil {
			panic(err)
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
			panic(err)
		}
		if term > req.Term {
			data = []byte(ErrTermExpired.Error())
			return
		}
		if err = dbMeta.setTerm(txn, req.Term); err != nil {
			panic(err)
		}
		data, err = (&internal.TermResponse{}).MarshalVT()
		if err != nil {
			panic(err)
		}
		value = index
	}
	return
}

func finish() {
	var err error
	if err = dbMeta.setIndex(txn, newIndex); err != nil {
		panic(err)
	}
	if newRev > rev {
		err = dbMeta.setRevision(txn, newRev)
		if err != nil {
			panic(err)
		}
	}
	if err := txn.Commit(); err != nil {
		panic(err)
	}
}

func read(query []byte) (code uint64, res []byte) {
	// txn, err := lmdb.BeginTxn(nil, 0)
	// if err != nil {
	// 	panic(err)
	// }
	return
}

func cmdPut(
	txn *lmdb.Txn, index, subrev, epoch uint64,
	req *internal.PutRequest,
) (res *internal.PutResponse, val uint64, affected [][]byte, err error) {
	res = &internal.PutResponse{}
	if len(req.Key) > KRV_LIMIT_KEY_LENGTH {
		err = ErrGRPCKeyTooLong
		return
	}
	prev, _, _, err := kvStore.put(txn, index, subrev, uint64(req.Lease), epoch, req.Key, req.Value, req.IgnoreValue, req.IgnoreLease)
	if err != nil {
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
	txn *lmdb.Txn, index, subrev, epoch uint64,
	req *internal.DeleteRangeRequest,
) (res *internal.DeleteRangeResponse, keys [][]byte, err error) {
	res = &internal.DeleteRangeResponse{}
	prev, n, err := kvStore.deleteRange(txn, index, subrev, epoch, req.Key, req.RangeEnd)
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
	res.Header = responseHeader(index)
	if req.PrevKv {
		for _, prec := range prev {
			item, _, err = kvStore.getRev(txn, prec.key, prec.rev.upper(), false)
			res.PrevKvs = append(res.PrevKvs, item.ToProto())
		}
	}
	return
}

func txnOps(
	txn *lmdb.Txn, index, epoch uint64,
	ops []*internal.RequestOp,
) (res []*internal.ResponseOp, keys [][]byte, err error) {
	err = txn.Sub(func(txn *lmdb.Txn) (err error) {
		for i, op := range ops {
			switch op.Request.(type) {
			case *internal.RequestOp_RequestPut:
				putReq := op.Request.(*internal.RequestOp_RequestPut).RequestPut
				putRes, _, affected, err := cmdPut(txn, index, uint64(i), epoch, putReq)
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
				delRes, affected, err := cmdDeleteRange(txn, index, uint64(i), epoch, delReq)
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
				rangeRes, err := queryRange(txn, index, rangeReq)
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

func txnCompare(txn *lmdb.Txn, conds []*internal.Compare) (success bool, err error) {
	success = true
	var item kv
	for _, cond := range conds {
		if item, err = kvStore.get(txn, cond.Key); err != nil {
			return
		}
		if !bytes.Equal(cond.Key, item.key) {
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
	txn *lmdb.Txn, epoch uint64,
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
		res.Error = ErrGRPCDuplicateKey.Error()
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
	txn *lmdb.Txn, index, epoch, id uint64,
) (keys [][]byte, val uint64, err error) {
	var item lease
	if item, err = dbLease.get(txn, uint64(id)); err != nil {
		return
	}
	if item.id == 0 {
		val = codeNotFound
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
		if err = kvStore.deleteBatch(txn, index, 0, epoch, batch); err != nil {
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
	return
}

func queryRange(
	txn *lmdb.Txn, index uint64,
	req *internal.RangeRequest,
) (res *internal.RangeResponse, err error) {
	res = &internal.RangeResponse{
		Header: responseHeader(index),
	}
	if req.Revision > 0 {
		min, err := dbMeta.getRevisionMin(txn)
		if err != nil {
			return nil, err
		}
		if req.Revision < int64(min) {
			return nil, ErrGRPCCompacted
		}
		if req.Revision > int64(index) {
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

func cmdLeaseKeepAlive(
	txn *lmdb.Txn, epoch uint64,
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

func responseHeader(revision uint64) *internal.ResponseHeader {
	return &internal.ResponseHeader{
		Revision:  int64(revision),
		ClusterId: shardID,
		MemberId:  replicaID,
	}
}
