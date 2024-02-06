# geyser-grpc-plugin

A lightweight gRPC service for streaming account and slot updates to subscribers.

## Building

When building for your validator, ensure the solana version library 
matches the imported packages here. Also, ensure that the cargo 
version (installed in rust-toolchain.toml) matches the rust-toolchain.toml 
in the solana repository.

There are two options for building:

For building using cargo:
```bash
$ cargo b --release
```

For building in docker:
```bash
$ ./f
```

## Releasing
When releasing, ensure the version being released matches the solana version. 
This keeps things simple :)

```bash
$ ./release
```

## Releases
Releases built by CI can be found [here](https://github.com/jito-foundation/geyser-grpc-plugin/releases).
The release version should match the version of validator client you're running

## Running
1. Copy and edit the [config json file](./server/example-config.json) to suit your validator
1. Add startup arg to solana validator
    - Example: `--geyser-plugin-config geyser.json`
1. Restart validator
1. Check logs for `Starting GeyserPluginService from config files` or `geyser_grpc_plugin_server::server`

### Helper Scripts

For your convenience:

* Run `./s` script to rsync to a server.
* Run `./f` to build the binary within a container and spit out to a `container-output` folder.
