dev:
	@go build -ldflags="-s -w" -o _dist/standalone ./cmd/standalone && cd cmd/standalone && docker compose up --build

test:
	@go test

integration:
	@go test ./...

parity:
	@KRV_PARITY_CHECK=true go test -v

bench:
	@go test -bench=. -run=_ -v

unit:
	@go test ./... -tags unit -v

cover:
	@mkdir -p _dist
	@go test -coverprofile=_dist/coverage.out -v
	@go tool cover -html=_dist/coverage.out -o _dist/coverage.html

gen:
	@protoc internal/*.proto --go_out=internal \
		--go-grpc_out=internal -I internal
	@protoc \
		--go_out=module/state_machine/internal --plugin protoc-gen-go="${GOBIN}/protoc-gen-go" \
		--go-grpc_out=module/state_machine/internal --plugin protoc-gen-go-grpc="${GOBIN}/protoc-gen-go-grpc" \
		--go-vtproto_out=module/state_machine/internal --plugin protoc-gen-go-vtproto="${GOBIN}/protoc-gen-go-vtproto" \
		--go-vtproto_opt=features=marshal+unmarshal+size \
		internal/*.proto;
	@mv module/state_machine/proto/github.com/pantopic/krv/internal module/state_machine
	@rm -rf module/state_machine/internal/github.com

gen-install:
	go install google.golang.org/protobuf/cmd/protoc-gen-go@latest
	go install google.golang.org/grpc/cmd/protoc-gen-go-grpc@latest
	go install github.com/planetscale/vtprotobuf/cmd/protoc-gen-go-vtproto@latest

cloc:
	@cloc . --exclude-dir=_example,_dist,internal --exclude-ext=pb.go
