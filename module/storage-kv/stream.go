package main

import (
	"bytes"
	"encoding/binary"

	"github.com/pantopic/wazero-lmdb/sdk-go"
	"github.com/pantopic/wazero-range-watch/sdk-go"
	"github.com/pantopic/wazero-state-machine/sdk-go"

	internal "github.com/pantopic/turbokube/module/storage-kv/internal"
)

var (
	eventKvPrevResponse = &internal.KeyValue{}
	eventKvResponse     = &internal.KeyValue{}
	eventResponse       = &internal.Event{}
	watchRequest        = &internal.WatchRequest{}
	watchEventBatch     = &internal.WatchEventBatch{Event: &internal.Event{}}
	watchSyncBatch      = &internal.WatchSyncBatch{}
)

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
	b, err := req.MarshalVT()
	if err != nil {
		panic("Error marshaling watch create request: " + err.Error())
	}
	rev, _, err := watchScan(req, since, true)
	if err != nil {
		panic("Error in event scan 1: " + err.Error())
	}
	if since == 0 {
		watchRev.Store(uint64(req.WatchId), rev)
		watchCache.Put(watchIdBytes, b)
		if err = range_watch.OpenStart(watchIdBytes, req.Key, req.RangeEnd); err != nil {
			panic("Error starting range watch: " + err.Error())
		}
	} else {
		if err = range_watch.Open(watchIdBytes, req.Key, req.RangeEnd); err != nil {
			panic("Error starting range watch: " + err.Error())
		}
		rev, _, err = watchScan(req, rev+1, false)
		if err != nil {
			panic("Error in event scan 2: " + err.Error())
		}
		watchRev.Store(uint64(req.WatchId), rev)
		watchCache.Put(watchIdBytes, b)
		if err = range_watch.Start(watchIdBytes); err != nil {
			panic("Error starting range watch: " + err.Error())
		}
	}
	return
}

var filtered = map[uint8]bool{}

func watchScan(req *internal.WatchCreateRequest, since uint64, start bool) (rev uint64, sent int, err error) {
	defer clear(filtered)
	for _, f := range req.Filters {
		filtered[uint8(f)] = true
	}
	err = lmdb.View(func(txn lmdb.Txn) (err error) {
		rev, err = dbMeta.getRevision(txn)
		if err != nil {
			return
		}
		if start {
			sendCodeHeader(uint64(req.WatchId), WatchMessageType_INIT, rev)
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
			sendCodeMsg(uint64(req.WatchId), WatchMessageType_EVENT, eventResponse)
			sent++
		}
		return
	})
	if sent > 0 {
		sendCodeHeader(uint64(req.WatchId), WatchMessageType_SYNC, rev)
	}
	return
}

func streamOpen() {
	range_watch.GroupStart()
}

func streamClosed() {
	range_watch.GroupStop()
}

func rangeWatchRecv(notices []range_watch.Notice) {
	watchSyncBatch.Reset()
	reqs := make(map[uint64]*internal.WatchCreateRequest)
	revs := make([]uint64, len(notices))
	prev := make([]int, len(notices))
	sent := make(map[uint64]int)
	mins := make(map[uint64]uint64)
	for i, n := range notices {
		revs[i] = n.Val
		for _, watchIdBytes := range n.IDs {
			watchID := binary.BigEndian.Uint64(watchIdBytes)
			watchSyncBatch.IDs = append(watchSyncBatch.IDs, int64(watchID))
			req, ok := reqs[watchID]
			if !ok {
				b := watchCache.Get(watchIdBytes)
				if len(b) == 0 {
					continue
					// panic(`watchCache not found: ` + strconv.Itoa(int(watchID)) + ` ` + string(watchIdBytes))
				}
				req = &internal.WatchCreateRequest{}
				err := req.UnmarshalVT(b)
				if err != nil {
					panic("Watch request malformed")
				}
				reqs[watchID] = req
				mins[watchID] = watchRev.Load(watchID)
			}
			if req.PrevKv {
				prev[i]++
			}
		}
	}
	var rev uint64
	err := lmdb.View(func(txn lmdb.Txn) (err error) {
		rev, err = dbMeta.getRevision(txn)
		if err != nil {
			return
		}
		var n uint64
		var i int
		for evt := range kvStore.revScan(txn, revs) {
			if n != evt.rev.upper() {
				if n > 0 {
					i++
				}
				n = evt.rev.upper()
			}
			var current, previous kv
			if evt.rev.isdel() {
				current = kv{key: evt.key, rev: evt.rev}
				if prev[i] > 0 {
					_, previous, err = kvStore.getRev(txn, evt.key, evt.rev.upper(), true)
				}
			} else {
				current, previous, err = kvStore.getRev(txn, evt.key, evt.rev.upper(), prev[i] > 0)
				if err != nil {
					panic("Error getting event kv: " + string(evt.key))
				}
			}
			kv := current.ToProto(eventKvResponse)
			notice := notices[i]
			e := watchEventBatch.Event
			e.Reset()
			watchEventBatch.Reset()
			e.Type = internal.Event_EventType(evt.etype())
			if current.rev.upper() > 0 {
				e.Kv = kv
			}
			if prev[i] > 0 && previous.rev.upper() > 0 {
				e.PrevKv = previous.ToProto(eventKvPrevResponse)
			}
			watchEventBatch.Event = e
			watchEventBatch.Revision = rev
		watches:
			for _, watchIdBytes := range notice.IDs {
				watchID := binary.BigEndian.Uint64(watchIdBytes)
				req, ok := reqs[watchID]
				if !ok {
					continue
				}
				min := mins[watchID]
				if min > 0 && evt.rev.upper() <= min {
					continue watches
				}
				for _, f := range req.Filters {
					if evt.etype() == uint8(f) {
						continue watches
					}
				}
				switch bytes.Compare(evt.key, req.Key) {
				case 1:
					if bytes.Compare(evt.key, req.RangeEnd) >= 0 {
						continue watches
					}
				case -1:
					continue watches
				}
				if !req.PrevKv {
					watchEventBatch.WatchIds = append(watchEventBatch.WatchIds, int64(watchID))
				} else {
					watchEventBatch.WatchIdsPrev = append(watchEventBatch.WatchIdsPrev, int64(watchID))
				}
				sent[watchID]++
			}
			if len(watchEventBatch.WatchIds) > 0 || len(watchEventBatch.WatchIdsPrev) > 0 {
				sendCodeMsg(0, WatchMessageType_EVENT_BATCH, watchEventBatch)
			}
		}
		return
	})
	if err != nil {
		panic("Error reading events: " + err.Error())
	}
	if len(watchSyncBatch.IDs) > 0 {
		watchSyncBatch.Revision = rev
		sendCodeMsg(0, WatchMessageType_SYNC_BATCH, watchSyncBatch)
	}
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

func sendCodeMsg(val uint64, code byte, msg Message) {
	data := make([]byte, 1+msg.SizeVT())
	data[0] = code
	if _, err := msg.MarshalToSizedBufferVT(data[1:]); err != nil {
		panic("Error serializing event kv: " + err.Error())
	}
	statemachine.StreamSend(val, data)
}
