package pcb

import (
	"context"
	"encoding/binary"
	"io"
	"log/slog"
	"sync"

	"github.com/logbn/zongzi"
	"github.com/tidwall/btree"
	"google.golang.org/protobuf/proto"

	"github.com/pantopic/config-bus/internal"
)

type serviceWatch struct {
	internal.UnimplementedWatchServer

	client zongzi.ShardClient
}

func NewServiceWatch(client zongzi.ShardClient) *serviceWatch {
	return &serviceWatch{client: client}
}

func (s *serviceWatch) addTerm(header *internal.ResponseHeader) *internal.ResponseHeader {
	_, term := s.client.Leader()
	header.RaftTerm = term
	return header
}

// Watch runs a watch
func (s *serviceWatch) Watch(
	server internal.Watch_WatchServer,
) (err error) {
	var watchId int64
	if !PCB_WATCH_ID_ZERO_INDEX {
		watchId++
	}
	var mu sync.RWMutex
	var watches btree.Map[int64, *watch]
	for {
		req, err := server.Recv()
		if err == io.EOF {
			break
		}
		if err != nil {
			break
		}
		switch ut := req.RequestUnion.(type) {
		case *internal.WatchRequest_CreateRequest:
			slog.Debug(`WatchRequest_CreateRequest`, `req`, req)
			req := ut.CreateRequest
			if req.WatchId > 0 {
				mu.RLock()
				_, ok := watches.Get(req.WatchId)
				mu.RUnlock()
				if ok {
					slog.Info("Ignoring request to create existing watch", "id", req.WatchId)
					if err = s.watchResp(server, &internal.WatchResponse{
						WatchId:      -1,
						Created:      true,
						Canceled:     true,
						CancelReason: internal.ErrWatcherDuplicateID.Error(),
					}); err != nil {
						slog.Error("Unable to send watch create failure response", "err", err.Error())
						return err
					}
					continue
				}
			}
			var w *watch
			id := req.WatchId
			w = s.watch(server.Context(), req, server, func() int64 {
				if id == 0 {
					id = watchId
					watchId++
				}
				mu.Lock()
				watches.Set(id, w)
				mu.Unlock()
				return id
			}, func() {
				// TODO - retry?
				mu.Lock()
				watches.Delete(id)
				mu.Unlock()
			})
		case *internal.WatchRequest_CancelRequest:
			slog.Debug(`WatchRequest_CancelRequest`, `req`, req)
			req := ut.CancelRequest
			mu.RLock()
			w, ok := watches.Get(req.WatchId)
			mu.RUnlock()
			if !ok {
				slog.Info("Ignoring request to cancel non-existent watch", "id", req.WatchId)
				if err = s.watchResp(server, &internal.WatchResponse{
					WatchId: req.WatchId,
				}); err != nil {
					slog.Error("Unable to send watch cancel failure response", "req", req, "err", err.Error())
				}
				break
			}
			w.Close()
			if err = s.watchResp(server, &internal.WatchResponse{
				WatchId:  req.WatchId,
				Canceled: true,
				Events:   []*internal.Event{},
			}); err != nil {
				slog.Error("Unable to send watch cancel response", "req", req, "err", err.Error())
			}
		case *internal.WatchRequest_ProgressRequest:
			slog.Debug(`WatchRequest_ProgressRequest`, `req`, req)
			_, data, err := s.client.Read(server.Context(), []byte{QUERY_WATCH_PROGRESS}, true)
			if err != nil {
				slog.Error("Unable to request progress", "req", req, "err", err.Error())
				break
			}
			var h internal.ResponseHeader
			if err = proto.Unmarshal(data, &h); err != nil {
				slog.Error("Error unmarshaling init", "err", err)
				break
			}
			mu.RLock()
			watchId, _, _ := watches.Min()
			mu.RUnlock()
			if err = s.watchResp(server, &internal.WatchResponse{
				Header:  &h,
				WatchId: watchId,
			}); err != nil {
				slog.Error("Unable to send watch progress response", "req", req, "err", err.Error())
			}
		}
	}
	return
}

func (s *serviceWatch) watch(
	ctx context.Context,
	req *internal.WatchCreateRequest,
	server internal.Watch_WatchServer,
	idFunc func() int64,
	done func(),
) (w *watch) {
	w = &watch{done: make(chan bool)}
	ctx, w.cancel = context.WithCancel(ctx)
	result := make(chan *Result)
	var id int64
	go func() {
		var clusterID uint64
		var memberID uint64
		var err error
		var size int
		var resp = &internal.WatchResponse{
			Header: &internal.ResponseHeader{},
		}
		for {
			res, ok := <-result
			if res == nil || !ok {
				break
			}
			switch res.Data[0] {
			case WatchMessageType_INIT:
				slog.Debug(`WatchMessageType_INIT`)
				if err = proto.Unmarshal(res.Data[1:], resp.Header); err != nil {
					slog.Error("Error unmarshaling init", "err", err)
					return
				}
				id = idFunc()
				req.WatchId = id
				resp.WatchId = id
				if err = s.watchResp(server, &internal.WatchResponse{
					Header:  resp.Header,
					WatchId: id,
					Created: true,
				}); err != nil {
					slog.Error("Error sending create response", "err", err)
					return
				}
				clusterID = resp.Header.ClusterId
				memberID = resp.Header.MemberId
			case WatchMessageType_EVENT:
				evt := &internal.Event{}
				if err = proto.Unmarshal(res.Data[9:], evt); err != nil {
					slog.Error("Error unmarshaling event", "err", err)
					return
				}
				slog.Debug(`WatchMessageType_EVENT`)
				var sz = len(evt.Kv.Key) + len(evt.Kv.Value) + sizeMetaKeyValue + sizeMetaEvent
				if evt.PrevKv != nil {
					sz += len(evt.PrevKv.Key) + len(evt.PrevKv.Value) + sizeMetaKeyValue
				}
				if size+sz < PCB_RESPONSE_SIZE_MAX {
					resp.Header.Revision = evt.Kv.ModRevision
					resp.Events = append(resp.Events, evt)
					size += sz
					continue
				}
				if resp.Header.Revision == evt.Kv.ModRevision {
					resp.Fragment = true
				}
				s.addTerm(resp.Header)
				resp.Header.Revision = int64(binary.LittleEndian.Uint64(res.Data[1:9]))
				if err = s.watchResp(server, resp); err != nil {
					slog.Error("Error sending response")
					return
				}
				resp = &internal.WatchResponse{
					Header: &internal.ResponseHeader{
						ClusterId: clusterID,
						MemberId:  memberID,
					},
					WatchId: id,
				}
				resp.Header.Revision = evt.Kv.ModRevision
				resp.Events = append(resp.Events, evt)
				size = sz + sizeMetaWatchResponse + sizeMetaHeader
			case WatchMessageType_SYNC:
				if err = proto.Unmarshal(res.Data[1:], resp.Header); err != nil {
					slog.Error("Error unmarshaling sync", "err", err)
					return
				}
				slog.Debug(`WatchMessageType_SYNC`)
				if len(resp.Events) > 0 {
					if err = s.watchResp(server, resp); err != nil {
						slog.Error("Error sending response")
						return
					}
					resp = &internal.WatchResponse{
						Header: &internal.ResponseHeader{
							ClusterId: clusterID,
							MemberId:  memberID,
						},
						WatchId: id,
					}
				}
			case WatchMessageType_NOTIFY:
				slog.Debug(`WatchMessageType_NOTIFY`, `req`, req)
				var header = &internal.ResponseHeader{}
				if err = proto.Unmarshal(res.Data[1:], header); err != nil {
					slog.Error("Error unmarshaling notify", "err", err)
					return
				}
				s.addTerm(header)
				if err = s.watchResp(server, &internal.WatchResponse{
					Header:  header,
					WatchId: id,
				}); err != nil {
					slog.Error("Error sending response")
					return
				}
			case WatchMessageType_ERR_COMPACTED:
				slog.Debug(`WatchMessageType_ERR_COMPACTED`, `req`, req)
				var header = &internal.ResponseHeader{}
				if err = proto.Unmarshal(res.Data[1:], header); err != nil {
					slog.Error("Error unmarshaling compaction error", "err", err)
					return
				}
				if PCB_WATCH_CREATE_COMPACTED {
					id = idFunc()
					if err = s.watchResp(server, &internal.WatchResponse{
						WatchId: id,
						Created: true,
					}); err != nil {
						slog.Error("Error sending watch created response", "err", err)
						return
					}
					if err = s.watchResp(server, &internal.WatchResponse{
						WatchId:         id,
						Canceled:        true,
						CompactRevision: header.Revision,
					}); err != nil {
						slog.Error("Error sending compacted error response", "err", err)
						return
					}
				} else {
					if err = s.watchResp(server, &internal.WatchResponse{
						WatchId:         req.WatchId,
						CompactRevision: header.Revision,
					}); err != nil {
						slog.Error("Error sending compacted error response", "err", err)
						return
					}
				}
				done()
			}
		}
	}()
	go func() {
		defer close(result)
		defer done()
		defer close(w.done)
		query, err := proto.Marshal(req)
		if err != nil {
			slog.Error("Error marshaling query", "err", err.Error())
			return
		}
		if err := s.client.Watch(ctx, query, result, true); err != nil {
			slog.Error("Error watching", "err", err.Error())
			return
		}
	}()
	return
}

func (s *serviceWatch) watchResp(
	server internal.Watch_WatchServer,
	resp *internal.WatchResponse,
) (err error) {
	if resp.Header == nil {
		resp.Header = &internal.ResponseHeader{}
	}
	s.addTerm(resp.Header)
	slog.Debug(`watchResp`, `resp`, resp)
	return server.Send(resp)
}

type watch struct {
	done   chan bool
	cancel context.CancelFunc
}

func (w *watch) Close() {
	w.cancel()
	<-w.done
}
