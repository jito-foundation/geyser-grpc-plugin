//! Takes an opinionated approach to how account updates shall be consumed. Exposes APIs to consume
//! partial and full updates.
//!
//! Clients using this consumer can expect the following:
//!     1. Account updates will be streamed in monotonically; i.e. updates for older slots
//!        are discarded in the event that they were streamed by the server late.
//!     2. Account updates received out of order will trigger an error.

use std::{
    collections::HashMap,
    num::ParseIntError,
    sync::{
        atomic::{AtomicBool, AtomicU64, Ordering},
        Arc,
    },
    time::Duration,
};

use crossbeam::channel::Sender;
use geyser_proto::geyser_client::GeyserClient;
use log::*;
use thiserror::Error;
use tokio::{
    runtime::Runtime,
    time::{interval, Instant},
};
use tonic::{transport::Channel, Response, Streaming};

use crate::{
    geyser_consumer::GeyserConsumerError::{HeartbeatError, StreamClosed},
    geyser_proto,
    geyser_proto::{
        maybe_account_update, maybe_partial_account_update, EmptyRequest, MaybeAccountUpdate,
        MaybePartialAccountUpdate, SlotUpdate as PbSlotUpdate, SubscribeAccountUpdatesRequest,
        SubscribePartialAccountUpdatesRequest,
    },
    types::{AccountUpdate, AccountUpdateNotification, PartialAccountUpdate, SlotUpdate},
    Pubkey, Slot,
};

#[derive(Error, Debug)]
pub enum GeyserConsumerError {
    #[error("GrpcError {0}")]
    GrpcError(#[from] tonic::Status),

    #[error("HeartbeatError")]
    HeartbeatError,

    #[error("MalformedResponse {0}")]
    MalformedResponse(String),

    #[error("StaleAccountUpdate update_slot={update_slot:?}, rooted_slot={rooted_slot:?}")]
    StaleAccountUpdate { update_slot: u64, rooted_slot: u64 },

    #[error("OutOfOrderSeqAccountUpdate update_slot={update_slot:?}, rooted_slot={rooted_slot:?}, actual_global_seq={actual_global_seq:?}, expected_global_seq={expected_global_seq:?}")]
    OutOfOrderSeqAccountUpdate {
        update_slot: u64,
        rooted_slot: u64,
        actual_global_seq: u64,
        expected_global_seq: u64,
    },

    #[error("StreamClosed")]
    StreamClosed,
}

pub type Result<T> = std::result::Result<T, GeyserConsumerError>;

pub const HIGHEST_WRITE_SLOT_HEADER: &str = "highest-write-slot";

pub type AccountWriteSeqs = HashMap<Slot, HashMap<Pubkey, AccountWriteSeq>>;

#[derive(Clone)]
pub struct AccountWriteSeq {
    /// This account's write sequence since the first write.
    global_seq: u64,

    /// This account's write sequence within a slot.
    slot_seq: u64,
}

#[derive(Clone)]
pub struct GeyserConsumer {
    /// Geyser client.
    client: GeyserClient<Channel>,

    /// Tokio runtime used to wrap async calls.
    runtime: Arc<Runtime>,

    /// Used to discard account writes received out of order.
    account_write_sequences: AccountWriteSeqs,

    /// Exit signal.
    exit: Arc<AtomicBool>,
}

impl GeyserConsumer {
    pub fn new(
        client: GeyserClient<Channel>,
        runtime: Arc<Runtime>,
        exit: Arc<AtomicBool>,
    ) -> Self {
        Self {
            client,
            runtime,
            account_write_sequences: HashMap::default(),
            exit,
        }
    }

    pub fn subscribe_account_updates(
        mut self,
        tx: Sender<Result<AccountUpdate>>,
        highest_rooted_slot: Arc<AtomicU64>,
        // Oldest slot from root consumer willing to tolerate.
        // e.g.
        //    current slot = 12, max_rooted_slot_distance = 6
        //    new slot = 13
        //    new slot = 6 -> Error
        max_rooted_slot_distance: u64,
        // Maximum number of heartbeats we're willing to consecutively miss before assuming something's wrong.
        max_allowable_missed_heartbeats: usize,
        skip_vote_accounts: bool,
    ) -> Result<()> {
        let expected_heartbeat_interval_ms = self
            .runtime
            .block_on(async { self.client.get_heartbeat_interval(EmptyRequest {}).await })?
            .into_inner()
            .heartbeat_interval_ms;
        let resp = self.runtime.block_on(async {
            self.client
                .subscribe_account_updates(SubscribeAccountUpdatesRequest { skip_vote_accounts })
                .await
        })?;

        let oldest_write_slot = extract_highest_write_slot_header(&resp)?;

        let stream = resp.into_inner();
        let _ = self.runtime.spawn(Self::process_account_updates_stream(
            stream,
            tx,
            self.account_write_sequences,
            highest_rooted_slot,
            oldest_write_slot,
            max_rooted_slot_distance,
            Duration::from_millis(expected_heartbeat_interval_ms),
            max_allowable_missed_heartbeats,
            self.exit.clone(),
        ));

        Ok(())
    }

    pub fn subscribe_partial_account_updates(
        mut self,
        tx: Sender<Result<PartialAccountUpdate>>,
        highest_rooted_slot: Arc<AtomicU64>,
        max_rooted_slot_distance: u64,
        max_allowable_missed_heartbeats: usize,
        skip_vote_accounts: bool,
    ) -> Result<()> {
        let expected_heartbeat_interval_ms = self
            .runtime
            .block_on(async { self.client.get_heartbeat_interval(EmptyRequest {}).await })?
            .into_inner()
            .heartbeat_interval_ms;
        let resp = self.runtime.block_on(async {
            self.client
                .subscribe_partial_account_updates(SubscribePartialAccountUpdatesRequest {
                    skip_vote_accounts,
                })
                .await
        })?;

        let oldest_write_slot = extract_highest_write_slot_header(&resp)?;

        let stream = resp.into_inner();
        self.runtime
            .spawn(Self::process_partial_account_updates_stream(
                stream,
                tx,
                self.account_write_sequences,
                highest_rooted_slot,
                oldest_write_slot,
                max_rooted_slot_distance,
                Duration::from_millis(expected_heartbeat_interval_ms),
                max_allowable_missed_heartbeats,
                self.exit.clone(),
            ));

        Ok(())
    }

    pub fn subscribe_slot_updates(mut self, tx: Sender<Result<SlotUpdate>>) -> Result<()> {
        let resp = self
            .runtime
            .block_on(async { self.client.subscribe_slot_updates(EmptyRequest {}).await })?;

        let stream = resp.into_inner();
        self.runtime.spawn(Self::process_slot_updates_stream(
            stream,
            tx,
            self.exit.clone(),
        ));

        Ok(())
    }

    #[allow(clippy::too_many_arguments)]
    async fn process_account_updates_stream(
        mut stream: Streaming<MaybeAccountUpdate>,
        tx: Sender<Result<AccountUpdate>>,
        mut account_write_sequences: AccountWriteSeqs,
        highest_rooted_slot: Arc<AtomicU64>,
        oldest_write_slot: Slot,
        max_rooted_slot_distance: u64,
        expected_heartbeat_interval: Duration,
        max_allowable_missed_heartbeats: usize,
        exit: Arc<AtomicBool>,
    ) {
        let mut latest_write_slot = 0;
        let mut last_heartbeat = Instant::now();

        let heartbeat_expiration =
            expected_heartbeat_interval.as_millis() * max_allowable_missed_heartbeats as u128;
        let heartbeat_expiration = Duration::from_millis(heartbeat_expiration as u64);
        let mut expected_heartbeat_interval = interval(expected_heartbeat_interval);

        while !exit.load(Ordering::Relaxed) {
            tokio::select! {
                now = expected_heartbeat_interval.tick() => {
                    let heartbeat_expired = now.duration_since(last_heartbeat).gt(&heartbeat_expiration);
                    if heartbeat_expired {
                        let _ = tx.try_send(Err(HeartbeatError));
                        exit.store(true, Ordering::Relaxed);
                    }
                }
                maybe_message = stream.message() => {
                    match maybe_message {
                        Ok(Some(maybe_update)) => {
                            match maybe_update.msg {
                                Some(maybe_account_update::Msg::AccountUpdate(update)) => {
                                    let mut update: AccountUpdate = update.into();
                                    if let Err(e) = Self::process_update(
                                        &mut update,
                                        &mut account_write_sequences,
                                        &highest_rooted_slot,
                                        &mut latest_write_slot,
                                        max_rooted_slot_distance,
                                        oldest_write_slot,
                                    ) {
                                        error!("error processing update: {:?}", e);
                                        let _ = tx.try_send(Err(e));
                                        exit.store(true, Ordering::Relaxed);
                                    } else {
                                        let _ = tx.try_send(Ok(update));
                                    }
                                },
                                Some(maybe_account_update::Msg::Hb(_)) => {
                                    last_heartbeat = Instant::now();
                                }
                                _ => {}
                            }
                        }
                        Ok(None) => {
                            let _ = tx.try_send(Err(StreamClosed));
                            exit.store(true, Ordering::Relaxed);
                        }
                        Err(e) => {
                            let _ = tx.try_send(Err(e.into()));
                            exit.store(true, Ordering::Relaxed);
                        }
                    }
                }
            }
        }
    }

    #[allow(clippy::too_many_arguments)]
    async fn process_partial_account_updates_stream(
        mut stream: Streaming<MaybePartialAccountUpdate>,
        tx: Sender<Result<PartialAccountUpdate>>,
        mut account_write_sequences: AccountWriteSeqs,
        highest_rooted_slot: Arc<AtomicU64>,
        oldest_write_slot: Slot,
        max_rooted_slot_distance: u64,
        expected_heartbeat_interval: Duration,
        max_allowable_missed_heartbeats: usize,
        exit: Arc<AtomicBool>,
    ) {
        let mut latest_write_slot = 0;
        let mut last_heartbeat = Instant::now();

        let heartbeat_expiration =
            expected_heartbeat_interval.as_millis() * max_allowable_missed_heartbeats as u128;
        let heartbeat_expiration = Duration::from_millis(heartbeat_expiration as u64);
        let mut expected_heartbeat_interval = interval(expected_heartbeat_interval);

        while !exit.load(Ordering::Relaxed) {
            tokio::select! {
                now = expected_heartbeat_interval.tick() => {
                    let heartbeat_expired = now.duration_since(last_heartbeat).gt(&heartbeat_expiration);
                    if heartbeat_expired {
                        let _ = tx.try_send(Err(HeartbeatError));
                        exit.store(true, Ordering::Relaxed);
                    }
                }
                maybe_message = stream.message() => {
                    match maybe_message {
                        Ok(Some(maybe_update)) => {
                            match maybe_update.msg {
                                Some(maybe_partial_account_update::Msg::PartialAccountUpdate(update)) => {
                                    let mut update: PartialAccountUpdate = update.into();
                                    if let Err(e) = Self::process_update(
                                        &mut update,
                                        &mut account_write_sequences,
                                        &highest_rooted_slot,
                                        &mut latest_write_slot,
                                        max_rooted_slot_distance,
                                        oldest_write_slot,
                                    ) {
                                        error!("error processing update: {:?}", e);
                                        let _ = tx.try_send(Err(e));
                                        exit.store(true, Ordering::Relaxed);
                                    } else {
                                        let _ = tx.try_send(Ok(update));
                                    }
                                }
                                Some(maybe_partial_account_update::Msg::Hb(_)) => {
                                    last_heartbeat = Instant::now();
                                }
                                None => {},
                            }
                        }
                        Ok(None) => {
                            let _ = tx.try_send(Err(StreamClosed));
                            exit.store(true, Ordering::Relaxed);
                        }
                        Err(e) => {
                            let _ = tx.try_send(Err(e.into()));
                            exit.store(true, Ordering::Relaxed);
                        }
                    }
                }
            }
        }
    }

    async fn process_slot_updates_stream(
        mut stream: Streaming<PbSlotUpdate>,
        tx: Sender<Result<SlotUpdate>>,
        exit: Arc<AtomicBool>,
    ) {
        while !exit.load(Ordering::Relaxed) {
            match stream.message().await {
                Ok(Some(slot_update)) => {
                    let _ = tx.try_send(Ok(slot_update.into()));
                }
                Ok(None) => {
                    let _ = tx.try_send(Err(StreamClosed));
                    exit.store(true, Ordering::Relaxed);
                }
                Err(e) => {
                    let _ = tx.try_send(Err(e.into()));
                    exit.store(true, Ordering::Relaxed);
                }
            }
        }
    }

    fn process_update<A: AccountUpdateNotification>(
        update: &mut A,
        account_write_sequences: &mut AccountWriteSeqs,
        highest_rooted_slot: &Arc<AtomicU64>,
        latest_write_slot: &mut Slot,
        max_rooted_slot_distance: u64,
        oldest_write_slot: u64,
    ) -> Result<()> {
        let update_slot = update.slot();
        if update_slot < oldest_write_slot {
            return Err(GeyserConsumerError::StaleAccountUpdate {
                update_slot,
                rooted_slot: highest_rooted_slot.load(Ordering::Relaxed),
            });
        }

        if update_slot > *latest_write_slot {
            *latest_write_slot = update.slot();
        } else if update_slot
            < highest_rooted_slot.load(Ordering::Relaxed) - max_rooted_slot_distance
        {
            return Err(GeyserConsumerError::StaleAccountUpdate {
                update_slot,
                rooted_slot: highest_rooted_slot.load(Ordering::Relaxed),
            });
        }

        let update_seqs = account_write_sequences.entry(update_slot).or_default();
        let account_write_seq = update_seqs
            .entry(update.pubkey())
            .or_insert(AccountWriteSeq {
                global_seq: update.seq(),
                slot_seq: 0,
            });

        if update.seq() < account_write_seq.global_seq {
            return Err(GeyserConsumerError::OutOfOrderSeqAccountUpdate {
                update_slot,
                rooted_slot: highest_rooted_slot.load(Ordering::Relaxed),
                actual_global_seq: update.seq(),
                expected_global_seq: account_write_seq.global_seq,
            });
        }

        update.set_seq(account_write_seq.slot_seq);
        account_write_seq.slot_seq += 1;
        Ok(())
    }
}

fn extract_highest_write_slot_header<T>(resp: &Response<T>) -> Result<Slot> {
    if let Some(highest_write_slot) = resp.metadata().get(HIGHEST_WRITE_SLOT_HEADER) {
        let highest_write_slot = highest_write_slot.to_str().map_err(|e| {
            GeyserConsumerError::MalformedResponse(format!(
                "error deserializing {} header: {}",
                HIGHEST_WRITE_SLOT_HEADER, e
            ))
        })?;
        let highest_write_slot: Slot = highest_write_slot.parse().map_err(|e: ParseIntError| {
            GeyserConsumerError::MalformedResponse(format!(
                "error parsing {} header: {}",
                HIGHEST_WRITE_SLOT_HEADER, e
            ))
        })?;

        Ok(highest_write_slot)
    } else {
        Err(GeyserConsumerError::MalformedResponse(format!(
            "missing {} header",
            HIGHEST_WRITE_SLOT_HEADER
        )))
    }
}
