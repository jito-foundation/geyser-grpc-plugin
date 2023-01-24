# About
- Most of these protobufs are copied from Solana Lab's repository.

## Pulling in updates
- confirmed_block.proto and transaction_by_addr.proto are untouched.
- Added the following to lib.rs (previously under generated  in convert.rs)
```rust
pub mod solana {
    pub mod geyser {
        tonic::include_proto!("solana.geyser");
    }
    pub mod storage {
        pub mod confirmed_block {
            tonic::include_proto!("solana.storage.confirmed_block");
        }
    }
}
```
- Moved the tx_by_addr to canonical include_proto! in lib.rs
```rust
pub mod tx_by_addr {
    tonic::include_proto!("solana.storage.transaction_by_addr");
}
```
- Rename convert.rs modules where necessary
