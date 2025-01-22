package icarus

import (
	"context"
	"io"
	"log/slog"

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
	watches := make(map[int64]func())
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
				if _, ok := watches[req.WatchId]; ok {
					slog.Info("Watch already exists", "id", req.WatchId)
					if err = s.watchResp(server, &internal.WatchResponse{
						WatchId: req.WatchId,
					}); err != nil {
						slog.Error("Unable to send watch response", "err", err.Error())
						return err
					}
					break
				}
			} else {
				req.WatchId = watchId
				_, ok := watches[req.WatchId]
				for ok {
					req.WatchId++
					_, ok = watches[req.WatchId]
				}
				watchId = req.WatchId + 1
			}
			if err = s.watchResp(server, &internal.WatchResponse{
				WatchId: req.WatchId,
				Created: true,
			}); err != nil {
				return err
			}
			watches[req.WatchId] = s.watch(server.Context(), req, server, req.WatchId, func() {
				// TODO - retry?
				delete(watches, req.WatchId)
			})
		case *internal.WatchRequest_CancelRequest:
			req := req.RequestUnion.(*internal.WatchRequest_CancelRequest).CancelRequest
			if _, ok := watches[req.WatchId]; !ok {
				if err = s.watchResp(server, &internal.WatchResponse{
					WatchId: req.WatchId,
				}); err != nil {
					slog.Warn("Watch not found in cancel", "req", req, "err", err.Error())
				}
				break
			}
			watches[req.WatchId]()
			if err = s.watchResp(server, &internal.WatchResponse{
				WatchId:  req.WatchId,
				Canceled: true,
			}); err != nil {
				slog.Warn("Unable to send watch cancel response", "req", req, "err", err.Error())
			}
		case *internal.WatchRequest_ProgressRequest:
			req := req.RequestUnion.(*internal.WatchRequest_ProgressRequest).ProgressRequest
			// TODO - track watch progress and generate response
			if err = s.watchResp(server, &internal.WatchResponse{
				WatchId: watchId,
			}); err != nil {
				slog.Warn("Watch not found in cancel", "req", req, "err", err.Error())
			}
		}
	}
	for _, cancel := range watches {
		cancel()
	}
	return
}

func (s *serviceWatch) watch(
	ctx context.Context,
	req *internal.WatchCreateRequest,
	server internal.Watch_WatchServer,
	id int64,
	done func(),
) (cancel func()) {
	ctx, cancel = context.WithCancel(ctx)
	result := make(chan *Result)
	go func() {
		var memberID uint64
		var clusterID uint64
		var prev *internal.Event
		var events []*internal.Event
		var err error
		for {
			res, ok := <-result
			if res == nil || len(res.Data) == 0 || !ok {
				slog.Debug("Closing watch", "id", id)
				break
			}
			switch res.Data[0] {
			case WatchMessageType_INIT:
				var header = &internal.ResponseHeader{}
				slog.Info("INIT", "id", id, "header", res.Data[1:])
				if err = proto.Unmarshal(res.Data[1:], header); err != nil {
					slog.Error("Error unmarshaling init", "err", err)
					return
				}
				memberID = header.MemberId
				clusterID = header.ClusterId
			case WatchMessageType_EVENT:
				evt := &internal.Event{}
				slog.Info("EVENT", "id", id, "data", res.Data[1:])
				if err = proto.Unmarshal(res.Data[1:], evt); err != nil {
					slog.Error("Error unmarshaling event", "err", err)
					return
				}
				if prev != nil && prev.Kv != nil && prev.Kv.ModRevision != evt.Kv.ModRevision {
					if err = server.Send(&internal.WatchResponse{
						Header: s.addTerm(&internal.ResponseHeader{
							MemberId:  memberID,
							ClusterId: clusterID,
							Revision:  prev.Kv.ModRevision,
						}),
						WatchId: id,
						Events:  events,
					}); err != nil {
						slog.Error("Error sending response")
						return
					}
					events = append(events[:0], evt)
					prev = evt
				} else {
					events = append(events, evt)
					prev = evt
				}
			case WatchMessageType_SYNC:
				var header = &internal.ResponseHeader{}
				slog.Info("SYNC", "id", id, "header", res.Data[1:])
				if err = proto.Unmarshal(res.Data[1:], header); err != nil {
					slog.Error("Error unmarshaling sync", "err", err, "Adata", res.Data[1:])
					return
				}
				if err = server.Send(&internal.WatchResponse{
					Header:  s.addTerm(header),
					WatchId: id,
					Events:  events,
				}); err != nil {
					slog.Error("Error sending response")
					return
				}
				events = events[:0]
				prev = nil
			case WatchMessageType_NOTIFY:
				var header = &internal.ResponseHeader{}
				if err = proto.Unmarshal(res.Data[1:], header); err != nil {
					slog.Error("Error unmarshaling notify", "err", err)
					return
				}
				if err = server.Send(&internal.WatchResponse{
					Header:  s.addTerm(header),
					WatchId: id,
				}); err != nil {
					slog.Error("Error sending response")
					return
				}
			default:
				slog.Error("Unrecognized response")
				return
			}
		}
	}()
	go func() {
		defer close(result)
		defer done()
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

type watch struct{}
