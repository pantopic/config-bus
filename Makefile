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
		--go_opt=paths=source_relative \
		--go-grpc_opt=paths=source_relative \
		--go-grpc_out=internal -I internal
	@mkdir -p module/state_machine/proto/gen
	@protoc \
		--go_out=module/state_machine/proto/gen --plugin protoc-gen-go="${GOBIN}/protoc-gen-go" \
		--go_opt=paths=source_relative \
		--go-vtproto_out=module/state_machine/proto/gen --plugin protoc-gen-go-vtproto="${GOBIN}/protoc-gen-go-vtproto" \
		--go-vtproto_opt=features=marshal+unmarshal+size \
		--go-vtproto_opt=paths=source_relative \
		internal/*.proto;

gen-install:
	go install google.golang.org/protobuf/cmd/protoc-gen-go@latest
	go install google.golang.org/grpc/cmd/protoc-gen-go-grpc@latest
	go install github.com/planetscale/vtprotobuf/cmd/protoc-gen-go-vtproto@latest

gen-lite:
	@mkdir -p module/state_machine/proto/gen-lite
	@ protoc \
		--plugin protoc-gen-go-lite="${GOBIN}/protoc-gen-go-lite" \
		--go-lite_out=module/state_machine/proto/gen-lite  \
		--go-lite_opt=features=marshal+unmarshal+size+equal+clone \
		--go-lite_opt=paths=source_relative \
		internal/*.proto;

gen-lite-install:
	go install github.com/aperturerobotics/protobuf-go-lite/cmd/protoc-gen-go-lite@latest

cloc:
	@cloc . --exclude-dir=_example,_dist,internal --exclude-ext=pb.go

wasm:
	@cd module/state_machine && tinygo build -buildmode=wasi-legacy -target=wasi -opt=2 -gc=conservative -scheduler=none -o ../state_machine.wasm

wasm-prod:
	@cd module/state_machine && tinygo build -buildmode=wasi-legacy -target=wasi -opt=2 -gc=conservative -scheduler=none -o ../state_machine.prod.wasm -no-debug
