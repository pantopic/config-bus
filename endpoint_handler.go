package pcb

import (
	// "log/slog"
	"net/http"
	"strings"

	"google.golang.org/grpc"
)

type endpointHandler struct {
	grpcServer *grpc.Server
}

func NewEndpointHandler(grpcServer *grpc.Server) *endpointHandler {
	return &endpointHandler{
		grpcServer: grpcServer,
	}
}

func (h *endpointHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	// slog.Info(`endpoint: ` + r.URL.Path)
	if r.ProtoMajor == 2 && strings.HasPrefix(
		r.Header.Get("Content-Type"), "application/grpc") {
		h.grpcServer.ServeHTTP(w, r)
		return
	}
	switch r.URL.Path {
	case "/metrics":
		w.Write([]byte(`pantopic_power_level 9001`))
	case "/health":
		w.Write([]byte(`{"health":"true","reason":""}`))
	case "/version":
		w.Write([]byte(`{"etcdserver":"3.5.25","etcdcluster":"3.5.0"}`))
	default:
		panic(r.URL.Path)
	}
}
