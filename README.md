raft-badger
===========

This repository provides the `raftbadger` package. The package exports the
`BadgerStore` which is an implementation of both a `LogStore` and `StableStore`.

It is meant to be used as a backend for the `raft` [package here](https://github.com/hashicorp/raft).

This implementation uses [BadgerDB](https://github.com/dgraph-io/badger).
BadgerDB is an embeddable, persistent, simple and fast key-value (KV) database written in pure Go.
It's meant to be a performant alternative to non-Go-based key-value stores
like [RocksDB](https://github.com/facebook/rocksdb).
