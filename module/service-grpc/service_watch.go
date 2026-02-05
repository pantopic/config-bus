package main

import (
	"encoding/binary"
	"errors"
	"iter"

	"github.com/pantopic/wazero-grpc-server/sdk-go"
	"github.com/pantopic/wazero-grpc-server/sdk-go/codes"
	"github.com/pantopic/wazero-grpc-server/sdk-go/status"
	"github.com/pantopic/wazero-shard-client/sdk-go"

	internal "github.com/pantopic/config-bus/module/service-grpc/internal"
)

func serviceWatchInit() {
	grpc_server.NewService(`etcdserverpb.Watch`).
		BidirectionalStream(`Watch`, watchRecv, watchSend)
	shard_client.RegisterStreamRecv(shardRecv)
}

func shardRecv(_, _ []byte, data []byte, val uint64) {
	pipeWatchSend(val, data)
}

func watchSendErr(code codes.Code, err error) {
	pipeWatchSend(0, []byte(status.New(code, err.Error()).Err().Error()))
}

func pipeWatchSend(val uint64, data []byte) {
	b := binary.LittleEndian.AppendUint64(data, val)
	pipeWatch.Send(b)
}

func pipeWatchDecode(msg []byte) (val uint64, data []byte) {
	val = binary.LittleEndian.Uint64(msg[len(msg)-8:])
	data = msg[:len(msg)-8]
	return
}

var watchResp = &internal.WatchResponse{
	Header: &internal.ResponseHeader{},
}
var respHeader = &internal.ResponseHeader{}

func watchRecv(in iter.Seq[[]byte]) (err error) {
	if err = kvShard().StreamOpen([]byte(`watch`)); err != nil {
		return
	}
	for data := range in {
		if err = kvShard().StreamSend([]byte(`watch`), data); err != nil {
			return
		}
	}
	return
}

func watchSend() (out iter.Seq[[]byte], err error) {
	size := make(map[uint64]int)
	response := make(map[uint64]*internal.WatchResponse)
	out = func(yield func([]byte) bool) {
		for {
			msg, err := pipeWatch.Recv()
			if err != nil {
				break
			}
			id, data := pipeWatchDecode(msg)
			if id == WATCH_ID_ERROR {
				err = errors.New(string(data))
				return
			}
			switch data[0] {
			case WatchMessageType_INIT:
				watchResp.Reset()
				if err = respHeader.UnmarshalVT(data[1:]); err != nil {
					panic(err)
				}
				watchResp.Header = respHeader
				watchResp.WatchId = int64(id)
				watchResp.Created = true
				if data, err = watchResp.MarshalVT(); err != nil {
					panic(err)
				}
				if !yield(data) {
					return
				}
			case WatchMessageType_EVENT:
				evt := &internal.Event{}
				if err = evt.UnmarshalVT(data[9:]); err != nil {
					panic(err)
				}
				var sz = len(evt.Kv.Key) + len(evt.Kv.Value) + sizeMetaKeyValue + sizeMetaEvent
				if evt.PrevKv != nil {
					sz += len(evt.PrevKv.Key) + len(evt.PrevKv.Value) + sizeMetaKeyValue
				}
				if _, ok := response[id]; !ok {
					response[id] = &internal.WatchResponse{
						Header:  &internal.ResponseHeader{},
						WatchId: int64(id),
					}
					size[id] = 0
				}
				if size[id]+sz < int(PCB_RESPONSE_SIZE_MAX()) {
					response[id].Header.Revision = evt.Kv.ModRevision
					response[id].Events = append(response[id].Events, evt)
					size[id] += sz
					continue
				}
				if response[id].Header.Revision == evt.Kv.ModRevision {
					response[id].Fragment = true
				}
				response[id].Header.Revision = int64(binary.LittleEndian.Uint64(data[1:9]))
				if data, err = response[id].MarshalVT(); err != nil {
					panic(err)
				}
				if !yield(data) {
					return
				}
				h := response[id].Header
				h.Reset()
				response[id].Reset()
				response[id].Header = h
				response[id].WatchId = int64(id)
				response[id].Header.Revision = evt.Kv.ModRevision
				response[id].Events = append(response[id].Events, evt)
				size[id] = sz + sizeMetaWatchResponse + sizeMetaHeader
			case WatchMessageType_SYNC:
				if err = response[id].Header.UnmarshalVT(data[1:]); err != nil {
					panic(err)
				}
				if len(response[id].Events) > 0 {
					if data, err = response[id].MarshalVT(); err != nil {
						panic(err)
					}
					if !yield(data) {
						return
					}
					h := response[id].Header
					h.Reset()
					response[id].Reset()
					response[id].Header = h
					response[id].WatchId = int64(id)
				}
			case WatchMessageType_NOTIFY:
				watchResp.Reset()
				if err = respHeader.UnmarshalVT(data[1:]); err != nil {
					panic(err)
				}
				watchResp.Header = respHeader
				watchResp.WatchId = int64(id)
				if data, err = watchResp.MarshalVT(); err != nil {
					panic(err)
				}
				if !yield(data) {
					return
				}
			case WatchMessageType_CANCELED:
				watchResp.Reset()
				watchResp.WatchId = int64(id)
				watchResp.Canceled = true
				if data, err = watchResp.MarshalVT(); err != nil {
					panic(err)
				}
				if !yield(data) {
					return
				}
			case WatchMessageType_ERR_EXISTS:
				watchResp.Reset()
				watchResp.WatchId = -1
				watchResp.Created = true
				watchResp.Canceled = true
				watchResp.CancelReason = ErrWatcherDuplicateID.Error()
				if data, err = watchResp.MarshalVT(); err != nil {
					panic(err)
				}
				if !yield(data) {
					return
				}
			case WatchMessageType_ERR_COMPACTED:
				if err = respHeader.UnmarshalVT(data[1:]); err != nil {
					panic(err)
				}
				watchResp.Reset()
				watchResp.Header = respHeader
				watchResp.WatchId = int64(id)
				watchResp.Created = true
				if data, err = watchResp.MarshalVT(); err != nil {
					panic(err)
				}
				if !yield(data) {
					return
				}
				watchResp.Reset()
				watchResp.Header = respHeader
				watchResp.WatchId = int64(id)
				watchResp.Canceled = true
				watchResp.CompactRevision = respHeader.Revision
				if data, err = watchResp.MarshalVT(); err != nil {
					panic(err)
				}
				if !yield(data) {
					return
				}
			default:
				panic(`Unrecognized`)
			}
		}
	}
	return
}
