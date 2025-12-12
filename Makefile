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

gen:
	@protoc internal/*.proto --go_out=internal --go_opt=paths=source_relative \
		--go-grpc_out=internal --go-grpc_opt=paths=source_relative -I internal

cloc:
	@cloc . --exclude-dir=_example,_dist,internal --exclude-ext=pb.go
