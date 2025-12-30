# Pantopic Config Bus

An [etcd](https://pkg.go.dev/github.com/tetratelabs/wazero) compatible distributed database.

## Status

This repository has two side-by-side implementations, one native and one in WebAssembly. The same test suite can be run
against native, WebAssembly, or an etcd instance to evaluate parity.

Many uncommonly used etcd features are not implemented (such as locks and leader election).

(wip)

## Roadmap

This project is in alpha. Breaking changes should be expected until Beta.

- `v0.0.x` - Alpha
  - [ ] Migrate working implementation to Pantopic model (Wasm)
- `v0.1.x` - Beta
  - [ ] Add metrics
  - [ ] Test in production
- `v1.x.x` - General Availability
  - [ ] Proven long term stability in production
