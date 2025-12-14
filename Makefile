dev:
	@go build -ldflags="-s -w" -o _dist/standalone ./cmd/standalone && cd cmd/standalone && docker compose up --build

build:
	@go build -ldflags="-s -w" -o _dist/pcb ./cmd/standalone

test:
	@go test -v

integration:
	@go test ./...

parity:
	@PCB_PARITY_CHECK=true go test -v

bench:
	@go test -bench=. -run=_ -v

unit:
	@go test ./... -tags unit -v

scp:
	@scp -o "StrictHostKeyChecking=accept-new" ./_dist/pcb root@etcd-0:/usr/bin/pcb
	@scp -o "StrictHostKeyChecking=accept-new" ./_dist/pcb root@etcd-1:/usr/bin/pcb
	@scp -o "StrictHostKeyChecking=accept-new" ./_dist/pcb root@etcd-2:/usr/bin/pcb
	@scp -o "StrictHostKeyChecking=accept-new" ./_dist/pcb root@apiserver-0:/usr/bin/pcb
	@scp -o "StrictHostKeyChecking=accept-new" ./_dist/pcb root@apiserver-1:/usr/bin/pcb
	@scp -o "StrictHostKeyChecking=accept-new" ./_dist/pcb root@apiserver-2:/usr/bin/pcb

cover:
	@mkdir -p _dist
	@go test -coverprofile=_dist/coverage.out -v
	@go tool cover -html=_dist/coverage.out -o _dist/coverage.html

cloc:
	@cloc . --exclude-dir=_example,_dist,internal --exclude-ext=pb.go

gen:
	@protoc internal/*.proto --go_out=internal --go_opt=paths=source_relative \
		--go-grpc_out=internal --go-grpc_opt=paths=source_relative -I internal

gen-install:
	go install google.golang.org/protobuf/cmd/protoc-gen-go@latest
	go install google.golang.org/grpc/cmd/protoc-gen-go-grpc@latest

gen-lite:
	@mkdir -p module/service-grpc/internal
	@protoc \
		--plugin protoc-gen-go-lite="${GOBIN}/protoc-gen-go-lite" \
		--go-lite_out=module/service-grpc  \
		--go-lite_opt=features=marshal+unmarshal+size \
		--go-lite_opt=paths=source_relative \
		internal/*.proto;
	@cp module/service-grpc/internal/* module/storage-kv/internal

gen-lite-install:
	go install github.com/aperturerobotics/protobuf-go-lite/cmd/protoc-gen-go-lite@latest

wasm-storage-kv:
	@cd module/storage-kv && tinygo build -buildmode=wasi-legacy -target=wasi -opt=s -gc=conservative -scheduler=none -o ../../cmd/standalone/storage-kv.wasm -no-debug
wasm-storage-kv-dev:
	@cd module/storage-kv && tinygo build -buildmode=wasi-legacy -target=wasi -opt=2 -gc=conservative -scheduler=none -o ../../cmd/standalone/storage-kv.dev.wasm
wasm-service-grpc:
	@cd module/service-grpc && tinygo build -buildmode=wasi-legacy -target=wasi -opt=s -gc=conservative -scheduler=none -o ../../cmd/standalone/service-grpc.wasm -no-debug
wasm-service-grpc-dev:
	@cd module/service-grpc && tinygo build -buildmode=wasi-legacy -target=wasi -opt=2 -gc=conservative -scheduler=none -o ../../cmd/standalone/service-grpc.dev.wasm
wasm: wasm-storage-kv wasm-service-grpc
wasm-dev: wasm-storage-kv-dev wasm-service-grpc-dev
wasm-all: wasm wasm-dev
