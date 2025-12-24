# Pantopic Config Bus

An [etcd](https://pkg.go.dev/github.com/tetratelabs/wazero) compatible Pantopic distributed database model powered by WebAssembly.

## Status

This repository has two side-by-side implementations, one native and one in WebAssembly. The same test suite can be run
against native, WASM, or an etcd instance to evaluate parity.

Many uncommonly used etcd features are not implemented (such as locks and leader election).

More docs to come.
