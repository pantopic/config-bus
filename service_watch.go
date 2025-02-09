package icarus

import (
	"context"
	"io"
	"log/slog"
	"sync"

	"github.com/logbn/zongzi"
	"google.golang.org/protobuf/proto"

	"github.com/logbn/icarus/internal"
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
	if !ICARUS_ZERO_INDEX_WATCH_ID {
		watchId++
	}
	var mu sync.RWMutex
	watches := make(map[int64]*watch)
	for {
		req, err := server.Recv()
		if err == io.EOF {
			break
		}
		if err != nil {
			break
		}
		switch req.RequestUnion.(type) {
		case *internal.WatchRequest_CreateRequest:
			req := req.RequestUnion.(*internal.WatchRequest_CreateRequest).CreateRequest
			if req.WatchId > 0 {
				mu.RLock()
				if _, ok := watches[req.WatchId]; ok {
					slog.Info("Ignoring request to create existing watch", "id", req.WatchId)
					if err = s.watchResp(server, &internal.WatchResponse{
						WatchId: req.WatchId,
					}); err != nil {
						slog.Error("Unable to send watch create failure response", "err", err.Error())
						return err
					}
					mu.RUnlock()
					break
				}
				mu.RUnlock()
			} else {
				req.WatchId = watchId
				watchId++
			}
			if err = s.watchResp(server, &internal.WatchResponse{
				WatchId: req.WatchId,
				Created: true,
			}); err != nil {
				return err
			}
			mu.Lock()
			watches[req.WatchId] = s.watch(server.Context(), req, server, req.WatchId, func() {
				// TODO - retry?
				mu.Lock()
				defer mu.Unlock()
				delete(watches, req.WatchId)
			})
			mu.Unlock()
		case *internal.WatchRequest_CancelRequest:
			req := req.RequestUnion.(*internal.WatchRequest_CancelRequest).CancelRequest
			mu.RLock()
			w, ok := watches[req.WatchId]
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
			}); err != nil {
				slog.Error("Unable to send watch cancel response", "req", req, "err", err.Error())
			}
		case *internal.WatchRequest_ProgressRequest:
			req := req.RequestUnion.(*internal.WatchRequest_ProgressRequest).ProgressRequest
			// TODO - track watch progress and generate response
			if err = s.watchResp(server, &internal.WatchResponse{
				WatchId: -1,
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
	id int64,
	done func(),
) (w *watch) {
	w = &watch{done: make(chan bool)}
	ctx, w.cancel = context.WithCancel(ctx)
	result := make(chan *Result)
	go func() {
		var clusterID uint64
		var memberID uint64
		var err error
		var size int
		var resp = &internal.WatchResponse{
			Header:  &internal.ResponseHeader{},
			WatchId: id,
		}
		for {
			res, ok := <-result
			if res == nil || !ok {
				slog.Debug("Closing watch", "id", id)
				break
			}
			switch res.Data[0] {
			case WatchMessageType_INIT:
				if err = proto.Unmarshal(res.Data[1:], resp.Header); err != nil {
					slog.Error("Error unmarshaling init", "err", err)
					return
				}
				clusterID = resp.Header.ClusterId
				memberID = resp.Header.MemberId
			case WatchMessageType_EVENT:
				evt := &internal.Event{}
				if err = proto.Unmarshal(res.Data[1:], evt); err != nil {
					slog.Error("Error unmarshaling event", "err", err)
					return
				}
				var sz = len(evt.Kv.Key) + len(evt.Kv.Value) + sizeMetaKeyValue + sizeMetaEvent
				if evt.PrevKv != nil {
					sz += len(evt.PrevKv.Key) + len(evt.PrevKv.Value) + sizeMetaKeyValue
				}
				if size+sz < ICARUS_RESPONSE_SIZE_MAX {
					resp.Header.Revision = evt.Kv.ModRevision
					resp.Events = append(resp.Events, evt)
					size += sz
					continue
				}
				if resp.Header.Revision == evt.Kv.ModRevision {
					resp.Fragment = true
				}
				s.addTerm(resp.Header)
				if err = server.Send(resp); err != nil {
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
				if len(resp.Events) > 0 {
					s.addTerm(resp.Header)
					if err = server.Send(resp); err != nil {
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
				var header = &internal.ResponseHeader{}
				if err = proto.Unmarshal(res.Data[1:], header); err != nil {
					slog.Error("Error unmarshaling notify", "err", err)
					return
				}
				s.addTerm(header)
				if err = server.Send(&internal.WatchResponse{
					Header:  header,
					WatchId: id,
				}); err != nil {
					slog.Error("Error sending response")
					return
				}
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
