use tonic_build::configure;

fn main() {
    configure()
        .compile(
            &[
                "proto/geyser.proto",
                "proto/confirmed_block.proto",
                "proto/transaction_by_addr.proto",
            ],
            &["proto"],
        )
        .unwrap();
}
