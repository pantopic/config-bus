dev:
	@go build -ldflags="-s -w" -o _dist/standalone ./cmd/standalone && cd cmd/standalone && docker compose up --build

build:
	@go build -ldflags="-s -w" -o _dist/krv ./cmd/standalone

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

scp:
	@scp -o "StrictHostKeyChecking=accept-new" ./_dist/krv root@etcd-0:~/krv
	@scp -o "StrictHostKeyChecking=accept-new" ./_dist/krv root@etcd-1:~/krv
	@scp -o "StrictHostKeyChecking=accept-new" ./_dist/krv root@etcd-2:~/krv

cover:
	@mkdir -p _dist
	@go test -coverprofile=_dist/coverage.out -v
	@go tool cover -html=_dist/coverage.out -o _dist/coverage.html

gen:
	@protoc internal/*.proto --go_out=internal --go_opt=paths=source_relative \
		--go-grpc_out=internal --go-grpc_opt=paths=source_relative -I internal

cloc:
	@cloc . --exclude-dir=_example,_dist,internal --exclude-ext=pb.go
