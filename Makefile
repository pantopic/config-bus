dev:
	@go build -ldflags="-s -w" -o _dist/standalone ./cmd/standalone && cd cmd/standalone && docker compose up --build

cluster:
	@go build -ldflags="-s -w" -o _dist/cluster ./cmd/cluster && cd cmd/cluster && docker compose up --build

build:
	@go build -ldflags="-s -w" -o _dist/pcb ./cmd/standalone

build-cluster:
	@go build -ldflags="-s -w" -o _dist/pcb ./cmd/cluster

test:
	@go test -v

integration:
	@go test ./...

parity:
	@PCB_PARITY_CHECK=true go test -v

test-cluster:
	@PCB_CLUSTER_CHECK=true go test -v

bench:
	@go test -bench=. -run=_ -v

unit:
	@go test ./... -tags unit -v

scp:
	@scp -o "StrictHostKeyChecking=accept-new" ./_dist/pcb root@etcd-0:/usr/bin/pcb
	@scp -o "StrictHostKeyChecking=accept-new" ./_dist/pcb root@etcd-1:/usr/bin/pcb
	@scp -o "StrictHostKeyChecking=accept-new" ./_dist/pcb root@etcd-2:/usr/bin/pcb
# 	@scp -o "StrictHostKeyChecking=accept-new" ./_dist/pcb root@apiserver-0:/usr/bin/pcb
# 	@scp -o "StrictHostKeyChecking=accept-new" ./_dist/pcb root@apiserver-1:/usr/bin/pcb
# 	@scp -o "StrictHostKeyChecking=accept-new" ./_dist/pcb root@apiserver-2:/usr/bin/pcb

scp-0:
	@scp -o "StrictHostKeyChecking=accept-new" ./_dist/pcb root@etcd-0:/usr/bin/pcb

get-logs:
	@scp -o "StrictHostKeyChecking=accept-new" root@etcd-0:~/nohup.out ./_logs/pcb.etcd-0.log
	@scp -o "StrictHostKeyChecking=accept-new" root@etcd-1:~/nohup.out ./_logs/pcb.etcd-1.log
	@scp -o "StrictHostKeyChecking=accept-new" root@etcd-2:~/nohup.out ./_logs/pcb.etcd-2.log

cover:
	@mkdir -p _dist
	@go test -coverprofile=_dist/coverage.out -v
	@go tool cover -html=_dist/coverage.out -o _dist/coverage.html

cloc:
	@cloc . --exclude-dir=_example,_dist,internal --exclude-ext=pb.go

cloc-native:
	@cloc . --exclude-dir=_example,_dist,internal,module --exclude-ext=pb.go

cloc-wasm:
	@cloc ./module --exclude-dir=_example,_dist,internal,patch --exclude-ext=pb.go

gen:
	@protoc internal/*.proto \
		--go_out=internal \
		--go_opt=paths=source_relative \
		--go-grpc_out=internal \
		--go-grpc_opt=paths=source_relative \
		-I internal
	@protoc internal/*.proto \
		--plugin protoc-gen-go-lite="${GOBIN}/protoc-gen-go-lite" \
		--go-lite_out=module/service-grpc/internal  \
		--go-lite_opt=paths=source_relative \
		--go-lite_opt=features=marshal+unmarshal+size \
		-I internal
	@cp module/service-grpc/internal/* module/storage-kv/internal
	@cp module/service-grpc/internal/* module/storage-lease/internal

gen-install:
	go install google.golang.org/protobuf/cmd/protoc-gen-go@latest
	go install google.golang.org/grpc/cmd/protoc-gen-go-grpc@latest
	go install github.com/aperturerobotics/protobuf-go-lite/cmd/protoc-gen-go-lite@v0.15.0

wasm-storage-kv:
	@cd module/storage-kv && tinygo build -buildmode=wasi-legacy -target=wasi -opt=s -gc=leaking -scheduler=none -o ../../embed/storage-kv.wasm -no-debug
wasm-storage-kv-dev:
	@cd module/storage-kv && tinygo build -buildmode=wasi-legacy -target=wasi -opt=2 -gc=leaking -scheduler=none -o ../../embed/storage-kv.dev.wasm
wasm-service-grpc:
	@cd module/service-grpc && tinygo build -buildmode=wasi-legacy -target=wasi -opt=s -gc=leaking -scheduler=none -o ../../embed/service-grpc.wasm -no-debug
wasm-service-grpc-dev:
	@cd module/service-grpc && tinygo build -buildmode=wasi-legacy -target=wasi -opt=2 -gc=leaking -scheduler=none -o ../../embed/service-grpc.dev.wasm
wasm-dev: wasm-storage-kv-dev wasm-service-grpc-dev
wasm-prod: wasm-storage-kv wasm-service-grpc
wasm-storage: wasm-storage-kv wasm-storage-kv-dev
wasm-service: wasm-service-grpc wasm-service-grpc-dev
wasm: wasm-dev wasm-prod

.PHONY: test
