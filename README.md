# geyser-grpc-plugin
A lightweight gRPC service for streaming account and slot updates to subscribers.

## server
This where the server side logic lives i.e implements the geyser-plugin-interface, wires up the gRPC service, and manages connections.
Note, what accounts are streamed is decided upon on the initial load of the plugin via a config json file.

### get_heartbeat_interval
Returns the server's configured heartbeat interval. Clients should consider the server unhealthy if some N consecutive heartbeats are missed.

### subscribe_account_updates
Subscribe to this endpoint if you're interested in receiving the entire account payload including the account's data bytes.

### subscribe_partial_account_updates
This endpoint streams account updates similar to `subscribe_account_updates` except consuming much less bandwidth. You may want to 
use this when you only care to be notified when an account is updated and don't neccessarily care to what the actual state transition was.

### subscribe_slot_updates
Notifies subscribers of slot state transitions i.e. processed, confirmed, rooted.

### Necessary Assumptions
The following assumptions __must__ be made:
* Clients may receive data for slots out of order.
* Clients may receive account updates for a given slot out of order.

## proto
Contains the protobuf API definitions.

## client
A simple gRPC client wrapper that Takes an opinionated approach to how account updates shall be consumed.
Clients using this consumer can expect the following:
1. Account updates will be streamed in monotonically; i.e. updates for older slots are discarded in the event that they were streamed by the server late.
2. Account updates received out of order will trigger an error.

## Helper Scripts
For your convenience:
* Run `./s` script to rsync to a server.
* Run `./f` to build the binary within a Docker container and spit out to a `docker-output` folder.
