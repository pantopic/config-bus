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

func (s *serviceWatch) addTerm(header *internal.ResponseHeader) {
	_, term := s.client.Leader()
	header.RaftTerm = term
}

// Watch runs a watch
func (s *serviceWatch) Watch(
	server internal.Watch_WatchServer,
) (err error) {
	var watchId int64
	result := make(chan *Result)
	watches := make(map[int64]func())
	go func() {
		var header = &internal.ResponseHeader{}
		var prev *internal.Event
		var events []*internal.Event
		for {
			res, ok := <-result
			if res == nil || !ok {
				slog.Debug("Closing watch", "id", watchId)
				break
			}
			switch res.Data[0] {
			case WatchMessageType_INIT:
				if err = proto.Unmarshal(res.Data[1:], header); err != nil {
					slog.Error("Error unmarshaling init", "err", err)
					return
				}
			case WatchMessageType_EVENT:
				evt := &internal.Event{}
				if err = proto.Unmarshal(res.Data[1:], evt); err != nil {
					slog.Error("Error unmarshaling event", "err", err)
					return
				}
				if prev != nil && prev.Kv != nil && prev.Kv.ModRevision != evt.Kv.ModRevision {
					s.addTerm(header)
					header.Revision = prev.Kv.ModRevision
					if err = server.Send(&internal.WatchResponse{
						Header:  header,
						WatchId: watchId,
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
				if err = proto.Unmarshal(res.Data[1:], header); err != nil {
					slog.Error("Error unmarshaling sync", "err", err)
					return
				}
				s.addTerm(header)
				if err = server.Send(&internal.WatchResponse{
					Header:  header,
					WatchId: watchId,
					Events:  events,
				}); err != nil {
					slog.Error("Error sending response")
					return
				}
				events = events[:0]
				prev = nil
			case WatchMessageType_NOTIFY:
				if err = proto.Unmarshal(res.Data[1:], header); err != nil {
					slog.Error("Error unmarshaling notify", "err", err)
					return
				}
				s.addTerm(header)
				if err = server.Send(&internal.WatchResponse{
					Header:  header,
					WatchId: watchId,
				}); err != nil {
					slog.Error("Error sending response")
					return
				}
			}
		}
	}()
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
			watchId++
			req.WatchId = watchId
			if err = s.watchResp(server, &internal.WatchResponse{
				WatchId: watchId,
				Created: true,
			}); err != nil {
				return err
			}
			watches[watchId] = s.watch(server.Context(), req, result, func() {
				// TODO - retry?
				delete(watches, watchId)
			})
		case *internal.WatchRequest_CancelRequest:
			req := req.RequestUnion.(*internal.WatchRequest_CancelRequest).CancelRequest
			if _, ok := watches[req.WatchId]; !ok {
				if err = s.watchResp(server, &internal.WatchResponse{
					WatchId: watchId,
				}); err != nil {
					slog.Warn("Watch not found in cancel", "req", req, "err", err.Error())
				}
				break
			}
			watches[req.WatchId]()
			if err = s.watchResp(server, &internal.WatchResponse{
				WatchId:  watchId,
				Canceled: true,
			}); err != nil {
				slog.Warn("Unable to send watch cancel response", "req", req, "err", err.Error())
			}
		case *internal.WatchRequest_ProgressRequest:
			req := req.RequestUnion.(*internal.WatchRequest_ProgressRequest).ProgressRequest
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
	close(result)
	return
}

func (s *serviceWatch) watch(ctx context.Context, req *internal.WatchCreateRequest, result chan *Result, done func()) (cancel func()) {
	ctx, cancel = context.WithCancel(ctx)
	go func() {
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
