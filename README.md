# CCFS

Chop-Chop File System: A distrubuted, highly available file system.

## Metadata server

The metadata server contains the information about the files and file chunks.
It is used by the client app to get info about which server should it upload/download the file chunks to/from,
also it takes care of replicating chunks on different servers.

[ ] create simple http server with API needed by the client app and the chunk servers

[ ] use DB to store metadata info and setup replication