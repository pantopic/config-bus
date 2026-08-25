package main

import (
	"encoding/binary"
	"errors"
	"sync"

	"github.com/pantopic/wazero-buffer-pool/sdk-go"
	"github.com/pantopic/wazero-grpc-server/sdk-go"

	internal "github.com/pantopic/turbokube/module/service-grpc/internal"
)

var evtPool = sync.Pool{New: func() any { return &internal.Event{} }}
var watchEventBatch = &internal.WatchEventBatch{Event: &internal.Event{}}
var watchEventSync = &internal.WatchEventSync{}

func shardRecv(_, data []byte, id uint64) {
	var err error
	if id == WATCH_ID_ERROR {
		println(`watch err ` + string(data))
		err = errors.New(string(data))
		return
	}
	switch data[0] {
	case WatchMessageType_INIT:
		watchResp.Reset()
		respHeader.Reset()
		if err = respHeader.UnmarshalVT(data[1:]); err != nil {
			panic(`Unable to unmarshal response header: ` + err.Error())
		}
		watchResp.Header = respHeader
		watchResp.WatchId = int64(id)
		watchResp.Created = true
		if data, err = watchResp.MarshalVT(); err != nil {
			panic(`Unable to marshal watch response: ` + err.Error())
		}
		grpc_server.Send(data)
	case WatchMessageType_EVENT_BATCH:
		watchEventBatch.Reset()
		if err = watchEventBatch.UnmarshalVT(data[1:]); err != nil {
			panic(`Unable to unmarshal watch event batch: ` + err.Error())
		}
		if len(watchEventBatch.WatchIdsPrev) > 0 {
			b, err := watchEventBatch.Event.MarshalVT()
			if err != nil {
				panic(`Unable to marshal watch event: ` + err.Error())
			}
			for _, id := range watchEventBatch.WatchIdsPrev {
				sendEvent(id, watchEventBatch.Revision, b)
			}
		}
		if len(watchEventBatch.WatchIds) > 0 {
			watchEventBatch.Event.PrevKv = nil
			b, err := watchEventBatch.Event.MarshalVT()
			if err != nil {
				panic(`Unable to marshal watch event: ` + err.Error())
			}
			for _, id := range watchEventBatch.WatchIds {
				sendEvent(id, watchEventBatch.Revision, b)
			}
		}
	case WatchMessageType_EVENT_SYNC:
		watchEventSync.Reset()
		if err = watchEventSync.UnmarshalVT(data[1:]); err != nil {
			panic(`Unable to unmarshal watch event batch: ` + err.Error())
		}
		for _, id := range watchEventSync.IDs {
			events := bufferPoolWatchEvent.Find(uint64(id))
			clearEvents(events, id, watchEventSync.Revision, true)
		}
	case WatchMessageType_NOTIFY:
		watchResp.Reset()
		respHeader.Reset()
		if err = respHeader.UnmarshalVT(data[1:]); err != nil {
			panic(`Unable to unmarshal response header: ` + err.Error())
		}
		watchResp.Header = respHeader
		watchResp.WatchId = int64(id)
		if data, err = watchResp.MarshalVT(); err != nil {
			panic(`Unable to marshal watch response: ` + err.Error())
		}
		grpc_server.Send(data)
	case WatchMessageType_CANCELED:
		watchResp.Reset()
		watchResp.WatchId = int64(id)
		watchResp.Canceled = true
		if data, err = watchResp.MarshalVT(); err != nil {
			panic(`Unable to marshal watch response: ` + err.Error())
		}
		grpc_server.Send(data)
		// TODO: reset watch buffer pool
	case WatchMessageType_ERR_EXISTS:
		watchResp.Reset()
		watchResp.WatchId = -1
		watchResp.Created = true
		watchResp.Canceled = true
		watchResp.CancelReason = ErrWatcherDuplicateID.Error()
		if data, err = watchResp.MarshalVT(); err != nil {
			panic(`Unable to marshal watch response: ` + err.Error())
		}
		grpc_server.Send(data)
	case WatchMessageType_ERR_COMPACTED:
		respHeader.Reset()
		if err = respHeader.UnmarshalVT(data[1:]); err != nil {
			panic(`Unable to unmarshal response header: ` + err.Error())
		}
		watchResp.Reset()
		watchResp.Header = respHeader
		watchResp.WatchId = int64(id)
		watchResp.Created = true
		if data, err = watchResp.MarshalVT(); err != nil {
			panic(`Unable to marshal watch response: ` + err.Error())
		}
		grpc_server.Send(data)
		watchResp.Reset()
		watchResp.Header = respHeader
		watchResp.WatchId = int64(id)
		watchResp.Canceled = true
		watchResp.CompactRevision = respHeader.Revision
		if data, err = watchResp.MarshalVT(); err != nil {
			panic(`Unable to marshal watch response: ` + err.Error())
		}
		grpc_server.Send(data)
	default:
		panic(`Unrecognized`)
	}
}

func sendEvent(id int64, rev uint64, b []byte) {
	events := bufferPoolWatchEvent.Find(uint64(id))
	b2 := binary.BigEndian.AppendUint64(b, rev)
	if events.Append(b2) {
		return
	}
	clearEvents(events, id, rev, false)
	if !events.Append(b2) {
		panic(`Failed to append watch event after reset`)
	}
}

func clearEvents(events buffer_pool.MultiValue, id int64, rev uint64, sync bool) {
	var lastRev uint64
	respHeader.Reset()
	watchResp.Reset()
	watchResp.Header = respHeader
	watchResp.WatchId = int64(id)
	for b := range events.Iter() {
		evt := evtPool.Get().(*internal.Event)
		if err := evt.UnmarshalVT(b[:len(b)-8]); err != nil {
			panic(`Unable to unmarshal event B: ` + err.Error())
		}
		lastRev = binary.BigEndian.Uint64(b[len(b)-8:])
		watchResp.Events = append(watchResp.Events, evt)
	}
	if len(watchResp.Events) == 0 {
		return
	}
	watchResp.Fragment = !sync && lastRev == rev
	watchResp.Header.Revision = int64(rev)
	res, err := watchResp.MarshalVT()
	if err != nil {
		panic(`Unable to marshal watch response: ` + err.Error())
	}
	grpc_server.Send(res)
	events.Reset()
	for _, evt := range watchResp.Events {
		evt.Reset()
		evtPool.Put(evt)
	}
}

var watchResp = &internal.WatchResponse{
	Header: &internal.ResponseHeader{},
}
var respHeader = &internal.ResponseHeader{}

func watchOpen() (err error) {
	return kvShard().StreamOpen([]byte(`watch`))
}

func watchRecv(data []byte) (err error) {
	return kvShard().StreamSend([]byte(`watch`), data)
}

func watchClose() (err error) {
	return
}
