# Orleans.SearchableStorage

Orleans-native persistent storage with searchable secondary indexes.

This repository is under active development and is not ready for production use.

## Goal

The project provides an `IGrainStorage` implementation whose durable records and local secondary indexes are owned by Orleans storage-partition grains. Partition state is persisted through a separately configured physical Orleans storage provider, allowing the same storage semantics to be exercised against PostgreSQL, Redis, and object-storage backends.

## Prior art

This project is informed by the archived [OrleansContrib/Orleans.Indexing-1.5](https://github.com/OrleansContrib/Orleans.Indexing-1.5) implementation and the paper [Indexing in an Actor-Oriented Database](https://www.cidrdb.org/cidr2017/papers/p29-bernstein-cidr17.pdf).

The prior implementation is treated as research and design input. This project is a new implementation for current .NET and Microsoft Orleans.

## Status

The first vertical slice is being implemented on .NET 10 and Orleans 10.
