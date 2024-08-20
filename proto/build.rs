use tonic_build::configure;

fn main() {
    const PROTOC_ENVAR: &str = "PROTOC";
    if std::env::var(PROTOC_ENVAR).is_err() {
        #[cfg(not(windows))]
        std::env::set_var(PROTOC_ENVAR, protobuf_src::protoc());
    }

    configure()
        .type_attribute(
            "TransactionErrorType",
            "#[cfg_attr(test, derive(enum_iterator::Sequence))]",
        )
        .type_attribute(
            "InstructionErrorType",
            "#[cfg_attr(test, derive(enum_iterator::Sequence))]",
        )
        .compile(
            &[
                "proto/confirmed_block.proto",
                "proto/entries.proto",
                "proto/geyser.proto",
                "proto/transaction_by_addr.proto",
            ],
            &["proto"],
        )
        .unwrap();
}
