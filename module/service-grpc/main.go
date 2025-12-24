package main

import (
	"github.com/pantopic/wazero-grpc-server/sdk-go"
)

func main() {
	grpc_server.NewService(`etcdserverpb.KV`).
		Unary(`Range`, kvRange).
		Unary(`Put`, kvPut).
		Unary(`DeleteRange`, kvDeleteRange).
		Unary(`Txn`, kvTxn).
		Unary(`Compact`, kvCompact)
}
