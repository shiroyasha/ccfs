# CCFS

Chop-Chop File System: A distrubuted, highly available file system.

## Metadata server

The metadata server contains the information about the files and file chunks.
It is used by the client app to get info about which server should it upload/download the file chunks to/from,
also it takes care of replicating chunks on different servers.

- [x] create simple http server with API needed by the client app and the chunk servers

- [ ] use DB to store metadata info and setup replication

- [ ] add tests

## Chunk server

The chunk server stores the chunks received by the user to its storage.
It is used by the client app to get info about which server should it upload/download the file chunks to/from,
also it takes care of replicating chunks on different servers.

- [x] create http server with upload/download API

- [ ] ping metadata server periodically to notify that server is available for storing chunks

- [ ] add tests

## CLI

A CLI for managing files on the CCFS.

- [x] create initial commands for basic usage

- [ ] add tests
