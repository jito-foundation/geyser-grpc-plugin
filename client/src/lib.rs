pub mod geyser_consumer;
pub mod types;

use std::{
    str::FromStr,
    sync::{atomic::AtomicBool, Arc},
};

use solana_sdk::{clock::Slot, pubkey::Pubkey};
use tokio::runtime::Runtime;
use tonic::transport::{ClientTlsConfig, Endpoint};

use crate::{geyser_consumer::GeyserConsumer, geyser_proto::geyser_client::GeyserClient};

pub(crate) mod geyser_proto {
    tonic::include_proto!("geyser");
}

pub fn connect(
    geyser_addr: String,
    tls_config: Option<ClientTlsConfig>,
    max_rooted_slot_distance: u64,
    max_allowable_missed_heartbeats: usize,
    exit: Arc<AtomicBool>,
) -> GeyserConsumer {
    let rt = Runtime::new().unwrap();

    let endpoint = Endpoint::from_str(&geyser_addr).unwrap();
    let ch = rt.block_on(async {
        if let Some(tls) = tls_config {
            endpoint.tls_config(tls).expect("tls_config")
        } else {
            endpoint
        }
        .connect()
        .await
        .expect("failed to connect")
    });

    let c = GeyserClient::new(ch);

    GeyserConsumer::new(
        c,
        rt,
        max_rooted_slot_distance,
        max_allowable_missed_heartbeats,
        exit,
    )
}
