package main

import (
	"encoding/binary"
	"strconv"

	"github.com/pantopic/wazero-lmdb/sdk-go"
	"github.com/pantopic/wazero-range-watch/sdk-go"
	"github.com/pantopic/wazero-state-machine/sdk-go"

	internal "github.com/pantopic/turbokube/module/storage-kv/internal"
)

var (
	eventKvPrevResponse = &internal.KeyValue{}
	eventKvResponse     = &internal.KeyValue{}
	eventResponse       = &internal.Event{}
	watchCreateRequest  = &internal.WatchCreateRequest{}
	watchRequest        = &internal.WatchRequest{}
)

func rangeWatchRecv(watchIdBytes []byte, revs []uint64) {
	watchID := binary.BigEndian.Uint64(watchIdBytes)
	last := watchRev.Load(watchID)
	b := watchCache.Get(watchIdBytes)
	if len(b) == 0 {
		panic(`watchCache not found: ` + strconv.Itoa(int(watchID)) + ` ` + string(watchIdBytes))
	}
	watchCreateRequest.Reset()
	err := watchCreateRequest.UnmarshalVT(b)
	if err != nil {
		panic("Watch request malformed")
	}
	var rev uint64
	var i, sent int
	for i, rev = range revs {
		if rev > last {
			break
		}
	}
	if rev > last {
		last, sent, err = watchScanRevs(watchCreateRequest, revs[i:])
		if err != nil {
			panic("Error reading events: " + err.Error())
		}
		watchRev.Store(watchID, last)
		if sent == 0 && watchCreateRequest.ProgressNotify {
			sendCodeHeader(uint64(watchCreateRequest.WatchId), WatchMessageType_NOTIFY, last)
		}
	}
}

func streamOpen() {
	range_watch.GroupStart()
}

func streamClosed() {
	range_watch.GroupStop()
}

func streamRecv(data []byte) {
	watchRequest.Reset()
	if err := watchRequest.UnmarshalVT(data); err != nil {
		panic(`Invalid command: ` + string(data))
	}
	switch ut := watchRequest.RequestUnion.(type) {
	case *internal.WatchRequest_CreateRequest:
		watchStart(ut.CreateRequest)
	case *internal.WatchRequest_CancelRequest:
		req := ut.CancelRequest
		watchIdBytes := binary.BigEndian.AppendUint64([]byte(nil), uint64(req.WatchId))
		range_watch.Stop(watchIdBytes)
		watchCache.Del(watchIdBytes)
		watchRev.Del(uint64(req.WatchId))
		statemachine.StreamSend(uint64(req.WatchId), []byte{WatchMessageType_CANCELED})
	case *internal.WatchRequest_ProgressRequest:
		var rev uint64
		err := lmdb.View(func(txn lmdb.Txn) (err error) {
			rev, err = dbMeta.getRevision(txn)
			if err != nil {
				return
			}
			return
		})
		if err != nil {
			panic(`Unable to retrieve database revision: ` + err.Error())
		}
		var minWatchId uint64
		var minWatchIdBytes = watchCache.Min()
		if len(minWatchIdBytes) == 8 {
			minWatchId = binary.BigEndian.Uint64(minWatchIdBytes)
		}
		sendCodeHeader(minWatchId, WatchMessageType_NOTIFY, rev)
	}
}

var filtered = map[uint8]bool{}

func watchScan(req *internal.WatchCreateRequest, since uint64) (rev uint64, sent int, err error) {
	defer clear(filtered)
	for _, f := range req.Filters {
		filtered[uint8(f)] = true
	}
	err = lmdb.View(func(txn lmdb.Txn) (err error) {
		rev, err = dbMeta.getRevision(txn)
		if err != nil {
			return
		}
		if since == 0 {
			return
		}
		for evt := range kvStore.scan(txn, since) {
			if _, ok := filtered[evt.etype()]; ok {
				continue
			}
			var current, prev kv
			if evt.rev.isdel() {
				current = kv{key: evt.key, rev: evt.rev}
				if req.PrevKv {
					_, prev, err = kvStore.getRev(txn, evt.key, evt.rev.upper(), req.PrevKv)
				}
			} else {
				current, prev, err = kvStore.getRev(txn, evt.key, evt.rev.upper(), req.PrevKv)
				if err != nil {
					panic("Error getting event kv: " + string(evt.key))
				}
			}
			eventResponse.Reset()
			eventResponse.Type = internal.Event_EventType(evt.etype())
			if current.rev.upper() > 0 {
				eventResponse.Kv = current.ToProto(eventKvResponse)
			}
			if prev.rev.upper() > 0 {
				eventResponse.PrevKv = prev.ToProto(eventKvPrevResponse)
			}
			sendCodeRevMsg(uint64(req.WatchId), WatchMessageType_EVENT, rev, eventResponse)
			sent++
		}
		return
	})
	if sent > 0 {
		sendCodeHeader(uint64(req.WatchId), WatchMessageType_SYNC, rev)
	}
	return
}

func watchScanRevs(req *internal.WatchCreateRequest, revs []uint64) (rev uint64, sent int, err error) {
	defer clear(filtered)
	for _, f := range req.Filters {
		filtered[uint8(f)] = true
	}
	err = lmdb.View(func(txn lmdb.Txn) (err error) {
		rev, err = dbMeta.getRevision(txn)
		if err != nil {
			return
		}
		for evt := range kvStore.revScan(txn, revs) {
			if _, ok := filtered[evt.etype()]; ok {
				continue
			}
			var current, prev kv
			if evt.rev.isdel() {
				current = kv{key: evt.key, rev: evt.rev}
				if req.PrevKv {
					_, prev, err = kvStore.getRev(txn, evt.key, evt.rev.upper(), req.PrevKv)
				}
			} else {
				current, prev, err = kvStore.getRev(txn, evt.key, evt.rev.upper(), req.PrevKv)
				if err != nil {
					panic("Error getting event kv: " + string(evt.key))
				}
			}
			eventResponse.Reset()
			eventResponse.Type = internal.Event_EventType(evt.etype())
			if current.rev.upper() > 0 {
				eventResponse.Kv = current.ToProto(eventKvResponse)
			}
			if prev.rev.upper() > 0 {
				eventResponse.PrevKv = prev.ToProto(eventKvPrevResponse)
			}
			sendCodeRevMsg(uint64(req.WatchId), WatchMessageType_EVENT, rev, eventResponse)
			sent++
		}
		return
	})
	if sent > 0 {
		sendCodeHeader(uint64(req.WatchId), WatchMessageType_SYNC, rev)
	}
	rev = revs[len(revs)-1]
	return
}

func watchStart(req *internal.WatchCreateRequest) (err error) {
	var since = uint64(req.StartRevision)
	var min uint64
	var watchIdBytes = make([]byte, 8)
	if req.WatchId == 0 {
		for {
			req.WatchId = int64(watchID.Add(1))
			binary.BigEndian.PutUint64(watchIdBytes, uint64(req.WatchId))
			err = range_watch.Reserve(watchIdBytes)
			if err == nil {
				break
			}
		}
	} else {
		binary.BigEndian.PutUint64(watchIdBytes, uint64(req.WatchId))
		err = range_watch.Reserve(watchIdBytes)
		if err != nil {
			statemachine.StreamSend(1, append([]byte(nil), WatchMessageType_ERR_EXISTS))
			return
		}
	}
	err = lmdb.View(func(txn lmdb.Txn) (err error) {
		if min, err = dbMeta.getRevisionMin(txn); err != nil {
			return
		}
		if since > 0 && min > since {
			err = ErrGRPCCompacted
		}
		return
	})
	if err == ErrGRPCCompacted {
		sendCodeHeader(uint64(req.WatchId), WatchMessageType_ERR_COMPACTED, min)
		return
	} else if err != nil {
		panic("Error checking min revision: " + err.Error())
	}
	sendCodeHeader(uint64(req.WatchId), WatchMessageType_INIT, 0)
	rev, _, err := watchScan(req, since)
	if err != nil {
		panic("Error in event scan 1: " + err.Error())
	}
	if err = range_watch.Open(watchIdBytes, req.Key, req.RangeEnd); err != nil {
		panic("Error starting range watch: " + err.Error())
	}
	rev, _, err = watchScan(req, rev+1)
	if err != nil {
		panic("Error in event scan 2: " + err.Error())
	}
	b, err := req.MarshalVT()
	if err != nil {
		panic("Error marshaling watch create request: " + err.Error())
	}
	watchRev.Store(uint64(req.WatchId), rev)
	watchCache.Put(watchIdBytes, b)
	if err = range_watch.Start(watchIdBytes); err != nil {
		panic("Error starting range watch: " + err.Error())
	}
	return
}

func sendCodeHeader(val uint64, code byte, rev uint64) {
	h := responseHeader(rev)
	data := make([]byte, 1+h.SizeVT())
	data[0] = code
	_, err := h.MarshalToSizedBufferVT(data[1:])
	if err != nil {
		panic("Error marshaling header: " + err.Error())
	}
	statemachine.StreamSend(val, data)
}

func sendCodeRevMsg(val uint64, code byte, rev uint64, msg Message) {
	data := make([]byte, 1+8+msg.SizeVT())
	data[0] = code
	binary.BigEndian.PutUint64(data[1:], rev)
	if _, err := msg.MarshalToSizedBufferVT(data[9:]); err != nil {
		panic("Error serializing event kv: " + err.Error())
	}
	statemachine.StreamSend(val, data)
}
