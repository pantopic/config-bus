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
				sendEvent(id, watchEventBatch.Revision, b, watchEventBatch.Event)
			}
		}
		if len(watchEventBatch.WatchIds) > 0 {
			watchEventBatch.Event.PrevKv = nil
			b, err := watchEventBatch.Event.MarshalVT()
			if err != nil {
				panic(`Unable to marshal watch event: ` + err.Error())
			}
			for _, id := range watchEventBatch.WatchIds {
				sendEvent(id, watchEventBatch.Revision, b, watchEventBatch.Event)
			}
		}
	case WatchMessageType_EVENT_SYNC:
		watchEventSync.Reset()
		if err = watchEventSync.UnmarshalVT(data[1:]); err != nil {
			panic(`Unable to unmarshal watch event batch: ` + err.Error())
		}
		for _, id := range watchEventSync.IDs {
			events := bufferPoolWatchEvent.Find(uint64(id))
			clearEvents(events, id, watchEventSync.Revision, nil, nil)
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

func sendEvent(id int64, rev uint64, b []byte, evt *internal.Event) {
	events := bufferPoolWatchEvent.Find(uint64(id))
	b2 := binary.BigEndian.AppendUint64(b, rev)
	if events.Append(b2) {
		return
	}
	clearEvents(events, id, rev, b, evt)
	if !events.Append(b2) {
		panic(`Failed to append watch event after reset`)
	}
}

func clearEvents(events buffer_pool.MultiValue, id int64, rev uint64, b []byte, evt *internal.Event) {
	var lastRev uint64
	resp := &internal.WatchResponse{
		Header:  &internal.ResponseHeader{},
		WatchId: int64(id),
	}
	for b := range events.Iter() {
		evt := &internal.Event{}
		if err := evt.UnmarshalVT(b[:len(b)-8]); err != nil {
			panic(`Unable to unmarshal event B: ` + err.Error())
		}
		lastRev = binary.BigEndian.Uint64(b[len(b)-8:])
		resp.Events = append(resp.Events, evt)
	}
	if len(resp.Events) == 0 {
		return
	}
	if len(b) > 0 {
		var recycle bool
		if evt == nil {
			recycle = true
			evt := evtPool.Get().(*internal.Event)
			evt.Reset()
			if err := evt.UnmarshalVT(b); err != nil {
				panic(`Unable to unmarshal event C: ` + err.Error())
			}
		}
		if lastRev == rev {
			resp.Fragment = true
		}
		if recycle {
			defer evtPool.Put(evt)
		}
		resp.Header.Revision = int64(lastRev)
	} else {
		resp.Header.Revision = int64(rev)
	}
	res, err := resp.MarshalVT()
	if err != nil {
		panic(`Unable to marshal watch response: ` + err.Error())
	}
	grpc_server.Send(res)
	events.Reset()
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
