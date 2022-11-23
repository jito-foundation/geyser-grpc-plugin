pub mod geyser_consumer;
pub mod types;

use std::{
    str::FromStr,
    sync::{atomic::AtomicBool, Arc},
};

use solana_sdk::{clock::Slot, pubkey::Pubkey};
use tonic::transport::{ClientTlsConfig, Endpoint};

use crate::{geyser_consumer::GeyserConsumer, geyser_proto::geyser_client::GeyserClient};

pub mod geyser_proto {
    tonic::include_proto!("geyser");
}

pub async fn connect(
    geyser_addr: String,
    tls_config: Option<ClientTlsConfig>,
    exit: Arc<AtomicBool>,
) -> GeyserConsumer {
    let endpoint = Endpoint::from_str(&geyser_addr).unwrap();
    let ch = if let Some(tls) = tls_config {
        endpoint.tls_config(tls).expect("tls_config")
    } else {
        endpoint
    }
    .connect()
    .await
    .expect("failed to connect");

    let c = GeyserClient::new(ch);

    GeyserConsumer::new(c, exit)
}
