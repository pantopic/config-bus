package main

import (
	"github.com/pantopic/wazero-lmdb/sdk-go"
	"github.com/pantopic/wazero-state-machine/sdk-go"

	internal "github.com/pantopic/turbokube/module/storage-lease/internal"
)

const (
	codeNotFound uint64 = 5
)

var (
	epoch    uint64
	newIndex uint64
	newRev   uint64
	oldRev   uint64
	txn      lmdb.Txn
)

func init() {
	statemachine.Persistent(open, update, finish, read)
}

func main() {}

func open() (index uint64) {
	if err := lmdb.Update(func(txn lmdb.Txn) (err error) {
		index = dbMeta.init(txn)
		dbStats.init(txn)
		dbLease.init(txn)
		dbLeaseExp.init(txn)
		return nil
	}); err != nil {
		panic(`Unable to open env ` + err.Error())
	}
	return
}

func update(index uint64, cmd []byte) (value uint64, data []byte) {
	newIndex = index
	var err error
	if txn == 0 {
		txn, err = lmdb.Begin(0)
		if err != nil {
			panic(`Unable to open txn: ` + err.Error())
		}
		epoch, err = dbMeta.getEpoch(txn)
		if err != nil {
			panic(`Unable to get epoch: ` + err.Error())
		}
	}
	switch cmd[len(cmd)-1] {
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
	case CMD_INTERNAL_TICK_LEASE:
		var req = &internal.LeaseTickRequest{}
		if err = req.UnmarshalVT(cmd[:len(cmd)-1]); err != nil {
			data = []byte(`Invalid command: ` + string(cmd))
			return
		}
		epoch++
		for _, li := range req.LeasesNew {
			item := lease{
				id: uint64(li.ID),
			}
			item.renewed = epoch
			item.expires = epoch + uint64(li.TTL)
			if err = dbLease.put(txn, item); err != nil {
				panic(err)
			}
			if err = dbLeaseExp.put(txn, item); err != nil {
				panic(err)
			}
		}
		for _, li := range req.LeasesRevoked {
			var item lease
			if item, err = dbLease.get(txn, uint64(li.ID)); err != nil {
				return
			}
			if item.id == 0 {
				println(`lease not found on tick revoke:`, li.ID)
				continue
			}
			if err = dbLeaseExp.del(txn, item); err != nil {
				panic(err)
			}
			if err = dbLease.del(txn, item.id); err != nil {
				panic(err)
			}
		}
		if err = dbMeta.setEpoch(txn, epoch); err != nil {
			panic(`Unable to set epoch: ` + err.Error())
		}
		// lease expire
		var resp = &internal.LeaseTickResponse{
			Epoch: epoch,
		}
		for id := range dbLeaseExp.scan(txn, epoch) {
			resp.LeasesExpired = append(resp.LeasesExpired, &internal.LeaseItem{ID: int64(id)})
		}
		data, err = resp.MarshalVT()
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
	if err := txn.Commit(); err != nil {
		panic(`Unable to commit transaction: ` + err.Error())
	}
	txn = 0
}

func read(query []byte) (value uint64, data []byte) {
	var rev uint64
	switch query[len(query)-1] {
	case QUERY_LEASE_LEASES:
		var req = &internal.LeaseLeasesRequest{}
		if err := req.UnmarshalVT(query[:len(query)-1]); err != nil {
			data = []byte("Invalid query: " + string(query))
			return
		}
		var resp *internal.LeaseLeasesResponse
		err := lmdb.View(func(txn lmdb.Txn) (err error) {
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
			resp, err = queryLeaseTimeToLive(txn, req)
			return
		})
		if err != nil {
			data = []byte(err.Error())
			return
		}
		data, err = resp.MarshalVT()
		if err != nil {
			data = []byte(err.Error())
			return
		}
		value = 1
	case QUERY_LEASE_CHECK_BATCH:
		var req = &internal.LeaseCheckBatchRequest{}
		if err := req.UnmarshalVT(query[:len(query)-1]); err != nil {
			data = []byte("Invalid query: " + string(query))
			return
		}
		var resp *internal.LeaseCheckBatchResponse
		err := lmdb.View(func(txn lmdb.Txn) (err error) {
			epoch, err := dbMeta.getEpoch(txn)
			if err != nil {
				panic(`Unable to get epoch: ` + err.Error())
			}
			resp, err = queryLeaseCheckBatch(txn, epoch, req)
			return
		})
		if err != nil {
			data = []byte(err.Error())
			return
		}
		data, err = resp.MarshalVT()
		if err != nil {
			data = []byte(err.Error())
			return
		}
		value = 1
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
	txn lmdb.Txn, id uint64,
) (keys [][]byte, val uint64, err error) {
	var item lease
	if item, err = dbLease.get(txn, uint64(id)); err != nil {
		return
	}
	if item.id == 0 {
		val = uint64(codeNotFound)
		return
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
	if item.id == 0 || item.expires < epoch {
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
		if item.id == 0 || item.expires < epoch {
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

func queryLeaseCheckBatch(
	txn lmdb.Txn, epoch uint64,
	req *internal.LeaseCheckBatchRequest,
) (res *internal.LeaseCheckBatchResponse, err error) {
	res = &internal.LeaseCheckBatchResponse{}
	for _, id := range req.IDs {
		var item lease
		if item, err = dbLease.get(txn, uint64(id)); err != nil {
			return
		}
		if item.id == 0 {
			res.TTLs = append(res.TTLs, 0)
			continue
		}
		res.TTLs = append(res.TTLs, int64(item.expires-item.renewed))
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
