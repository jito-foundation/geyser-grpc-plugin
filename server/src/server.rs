use std::{
    collections::HashMap,
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc,
    },
    thread::{Builder, JoinHandle},
    time::{Duration, Instant},
};

use crossbeam::channel::{unbounded, Receiver, Sender};
use crossbeam_channel::tick;
use log::*;
use once_cell::sync::OnceCell;
use serde_derive::Deserialize;
use thiserror::Error;
use tokio::sync::mpsc::{channel, Sender as TokioSender};
use tonic::{metadata::MetadataValue, Request, Response, Status};
use uuid::Uuid;

use crate::{
    geyser_proto::{
        geyser_server::Geyser, maybe_account_update, maybe_partial_account_update, AccountUpdate,
        EmptyRequest, GetHeartbeatIntervalResponse, Heartbeat, MaybeAccountUpdate,
        MaybePartialAccountUpdate, PartialAccountUpdate, SlotUpdate,
        SubscribeAccountUpdatesRequest, SubscribePartialAccountUpdatesRequest,
    },
    subscription_stream::{StreamClosedSender, SubscriptionStream},
};

static VOTE_PROGRAM_ID: OnceCell<Vec<u8>> = OnceCell::new();

pub const HIGHEST_WRITE_SLOT_HEADER: &str = "highest-write-slot";

#[derive(Clone)]
struct SubscriptionClosedSender {
    inner: Sender<SubscriptionClosedEvent>,
}
impl StreamClosedSender<SubscriptionClosedEvent> for SubscriptionClosedSender {
    type Error = crossbeam_channel::TrySendError<SubscriptionClosedEvent>;

    fn send(&self, event: SubscriptionClosedEvent) -> Result<(), Self::Error> {
        self.inner.try_send(event)
    }
}

type AccountUpdateTx = TokioSender<Result<MaybeAccountUpdate, Status>>;
type PartialAccountUpdateTx = TokioSender<Result<MaybePartialAccountUpdate, Status>>;
type SlotUpdateTx = TokioSender<Result<SlotUpdate, Status>>;

trait AccountUpdateStreamer<T> {
    fn stream_update(&self, update: T) -> GeyserServiceResult<()>;
}
trait HeartbeatStreamer {
    fn send_heartbeat(&self) -> GeyserServiceResult<()>;
}
trait ErrorStatusStreamer {
    fn stream_error(&self, status: Status) -> GeyserServiceResult<()>;
}

struct AccountUpdateSubscription {
    subscription_tx: AccountUpdateTx,
    skip_votes: bool,
}
impl AccountUpdateStreamer<AccountUpdate> for AccountUpdateSubscription {
    fn stream_update(&self, update: AccountUpdate) -> GeyserServiceResult<()> {
        if self.skip_votes
            && &update.owner
                == VOTE_PROGRAM_ID
                    .get_or_init(|| solana_program::vote::program::id().to_bytes().to_vec())
        {
            return Ok(());
        }

        let update = MaybeAccountUpdate {
            msg: Some(maybe_account_update::Msg::AccountUpdate(update)),
        };
        self.subscription_tx
            .try_send(Ok(update))
            .map_err(|_| GeyserServiceError::StreamMessageError)?;

        Ok(())
    }
}
impl HeartbeatStreamer for AccountUpdateSubscription {
    fn send_heartbeat(&self) -> GeyserServiceResult<()> {
        self.subscription_tx
            .try_send(Ok(MaybeAccountUpdate {
                msg: Some(maybe_account_update::Msg::Hb(Heartbeat {})),
            }))
            .map_err(|_| GeyserServiceError::StreamMessageError)?;
        Ok(())
    }
}
impl ErrorStatusStreamer for AccountUpdateSubscription {
    fn stream_error(&self, status: Status) -> GeyserServiceResult<()> {
        self.subscription_tx
            .try_send(Err(status))
            .map_err(|_| GeyserServiceError::StreamMessageError)?;
        Ok(())
    }
}

struct PartialAccountUpdateSubscription {
    subscription_tx: PartialAccountUpdateTx,
    skip_votes: bool,
}
impl AccountUpdateStreamer<PartialAccountUpdate> for PartialAccountUpdateSubscription {
    fn stream_update(&self, update: PartialAccountUpdate) -> GeyserServiceResult<()> {
        if self.skip_votes
            && &update.owner
                == VOTE_PROGRAM_ID
                    .get_or_init(|| solana_program::vote::program::id().to_bytes().to_vec())
        {
            return Ok(());
        }

        let update = MaybePartialAccountUpdate {
            msg: Some(maybe_partial_account_update::Msg::PartialAccountUpdate(
                update,
            )),
        };
        self.subscription_tx
            .try_send(Ok(update))
            .map_err(|_| GeyserServiceError::StreamMessageError)?;

        Ok(())
    }
}
impl HeartbeatStreamer for PartialAccountUpdateSubscription {
    fn send_heartbeat(&self) -> GeyserServiceResult<()> {
        self.subscription_tx
            .try_send(Ok(MaybePartialAccountUpdate {
                msg: Some(maybe_partial_account_update::Msg::Hb(Heartbeat {})),
            }))
            .map_err(|_| GeyserServiceError::StreamMessageError)?;
        Ok(())
    }
}
impl ErrorStatusStreamer for PartialAccountUpdateSubscription {
    fn stream_error(&self, status: Status) -> GeyserServiceResult<()> {
        self.subscription_tx
            .try_send(Err(status))
            .map_err(|_| GeyserServiceError::StreamMessageError)?;
        Ok(())
    }
}

struct SlotUpdateSubscription {
    subscription_tx: SlotUpdateTx,
}
impl ErrorStatusStreamer for SlotUpdateSubscription {
    fn stream_error(&self, status: Status) -> GeyserServiceResult<()> {
        self.subscription_tx
            .try_send(Err(status))
            .map_err(|_| GeyserServiceError::StreamMessageError)?;
        Ok(())
    }
}

#[allow(clippy::enum_variant_names)]
enum SubscriptionAddedEvent {
    AccountUpdate {
        uuid: Uuid,
        subscription_tx: AccountUpdateTx,
        skip_votes: bool,
    },
    PartialAccountUpdate {
        uuid: Uuid,
        subscription_tx: PartialAccountUpdateTx,
        skip_votes: bool,
    },
    SlotUpdate {
        uuid: Uuid,
        subscription_tx: SlotUpdateTx,
    },
}

#[allow(clippy::enum_variant_names)]
enum SubscriptionClosedEvent {
    AccountUpdateSubscription(Uuid),
    PartialAccountUpdateSubscription(Uuid),
    SlotUpdateSubscription(Uuid),
}

#[derive(Error, Debug)]
pub enum GeyserServiceError {
    #[error("CrossbeamError {0}")]
    CrossbeamRecvError(#[from] crossbeam_channel::RecvError),

    #[error("MalformedAccountUpdate {0}")]
    MalformedAccountUpdate(String),

    #[error("StreamMessageError")]
    StreamMessageError,
}
type GeyserServiceResult<T> = Result<T, GeyserServiceError>;

#[derive(Clone, Debug, Deserialize)]
pub struct GeyserServiceConfig {
    /// Cadence of heartbeats.
    heartbeat_interval_ms: u64,

    /// Individual subscriber buffer size.
    subscriber_buffer_size: usize,
}

pub struct GeyserService {
    /// Highest slot observed for a write, thus far.
    /// This value is returned in the http headers to clients on initial connection.
    highest_write_slot: Arc<AtomicU64>,

    /// General service configurations.
    service_config: GeyserServiceConfig,

    /// Used to add new subscriptions.
    subscription_added_tx: Sender<SubscriptionAddedEvent>,

    /// Used to close existing subscriptions.
    subscription_closed_sender: SubscriptionClosedSender,

    /// Internal event loop thread.
    t_hdl: JoinHandle<()>,
}

impl GeyserService {
    pub fn new(
        service_config: GeyserServiceConfig,
        // Account updates streamed from the validator.
        account_update_rx: Receiver<AccountUpdate>,
        // Slot updates streamed from the validator.
        slot_update_rx: Receiver<SlotUpdate>,
        // This value is maintained in the upstream context.
        highest_write_slot: Arc<AtomicU64>,
    ) -> Self {
        let (subscription_added_tx, subscription_added_rx) = unbounded();
        let (subscription_closed_tx, subscription_closed_rx) = unbounded();
        let heartbeat_tick = tick(Duration::from_millis(service_config.heartbeat_interval_ms));

        let t_hdl = Self::event_loop(
            account_update_rx,
            slot_update_rx,
            subscription_added_rx,
            subscription_closed_rx,
            heartbeat_tick,
        );

        Self {
            highest_write_slot,
            service_config,
            subscription_added_tx,
            subscription_closed_sender: SubscriptionClosedSender {
                inner: subscription_closed_tx,
            },
            t_hdl,
        }
    }

    pub fn join(self) {
        self.t_hdl.join().unwrap();
    }

    /// Main event loop that handles the following:
    ///     1. Add new subscriptions.
    ///     2. Cleanup closed subscriptions.
    ///     3. Receive geyser events and stream them to subscribers.
    fn event_loop(
        account_update_rx: Receiver<AccountUpdate>,
        slot_update_rx: Receiver<SlotUpdate>,
        subscription_added_rx: Receiver<SubscriptionAddedEvent>,
        subscription_closed_rx: Receiver<SubscriptionClosedEvent>,
        heartbeat_tick: Receiver<Instant>,
    ) -> JoinHandle<()> {
        Builder::new()
            .name("geyser-service-event-loop".to_string())
            .spawn(move || {
                info!("Starting event loop");
                let mut account_update_subscriptions: HashMap<Uuid, AccountUpdateSubscription> =
                    HashMap::new();
                let mut partial_account_update_subscriptions: HashMap<
                    Uuid,
                    PartialAccountUpdateSubscription,
                > = HashMap::new();
                let mut slot_update_subscriptions: HashMap<Uuid, SlotUpdateSubscription> = HashMap::new();

                loop {
                    crossbeam_channel::select! {
                        recv(heartbeat_tick) -> _ => {
                            let failed_subscription_ids = Self::send_heartbeats(&account_update_subscriptions);
                            Self::drop_subscriptions(&failed_subscription_ids[..], &mut account_update_subscriptions);

                            let failed_subscription_ids = Self::send_heartbeats(&partial_account_update_subscriptions);
                            Self::drop_subscriptions(&failed_subscription_ids[..], &mut partial_account_update_subscriptions);
                        }
                        recv(subscription_added_rx) -> maybe_subscription_added => {
                            if let Err(e) = Self::handle_subscription_added(maybe_subscription_added, &mut account_update_subscriptions, &mut partial_account_update_subscriptions, &mut slot_update_subscriptions) {
                                error!("error adding new subscription: {}", e);
                                return;
                            }
                        },
                        recv(subscription_closed_rx) -> maybe_subscription_closed => {
                            if let Err(e) = Self::handle_subscription_closed(maybe_subscription_closed, &mut account_update_subscriptions, &mut partial_account_update_subscriptions, &mut slot_update_subscriptions) {
                                error!("error closing existing subscription: {}", e);
                                return;
                            }
                        },
                        recv(account_update_rx) -> maybe_account_update => {
                            match Self::handle_account_update_event(maybe_account_update, &account_update_subscriptions, &partial_account_update_subscriptions) {
                                Err(e) => {
                                    error!("error handling an account update event: {}", e);
                                    return;
                                },
                                Ok(failed_subscription_ids) => {
                                    Self::drop_subscriptions(&failed_subscription_ids[..], &mut account_update_subscriptions);
                                    Self::drop_subscriptions(&failed_subscription_ids[..], &mut partial_account_update_subscriptions);
                                },
                            }
                        },
                        recv(slot_update_rx) -> maybe_slot_update => {
                            match Self::handle_slot_update_event(maybe_slot_update, &slot_update_subscriptions) {
                                Err(e) => {
                                    error!("error handling a slot update event: {}", e);
                                    return;
                                },
                                Ok(failed_subscription_ids) => {
                                    Self::drop_subscriptions(&failed_subscription_ids[..], &mut slot_update_subscriptions);
                                },
                            }
                        },
                    }
                }
            })
            .unwrap()
    }

    /// Handles adding new subscriptions.
    fn handle_subscription_added(
        maybe_subscription_added: Result<SubscriptionAddedEvent, crossbeam_channel::RecvError>,
        account_update_subscriptions: &mut HashMap<Uuid, AccountUpdateSubscription>,
        partial_account_update_subscriptions: &mut HashMap<Uuid, PartialAccountUpdateSubscription>,
        slot_update_subscriptions: &mut HashMap<Uuid, SlotUpdateSubscription>,
    ) -> GeyserServiceResult<()> {
        let subscription_added = maybe_subscription_added?;
        match subscription_added {
            SubscriptionAddedEvent::AccountUpdate {
                uuid,
                subscription_tx,
                skip_votes,
            } => {
                account_update_subscriptions.insert(
                    uuid,
                    AccountUpdateSubscription {
                        subscription_tx,
                        skip_votes,
                    },
                );
            }
            SubscriptionAddedEvent::PartialAccountUpdate {
                uuid,
                subscription_tx,
                skip_votes,
            } => {
                partial_account_update_subscriptions.insert(
                    uuid,
                    PartialAccountUpdateSubscription {
                        subscription_tx,
                        skip_votes,
                    },
                );
            }
            SubscriptionAddedEvent::SlotUpdate {
                uuid,
                subscription_tx,
            } => {
                slot_update_subscriptions.insert(uuid, SlotUpdateSubscription { subscription_tx });
            }
        }

        Ok(())
    }

    /// Handles closing existing subscriptions.
    fn handle_subscription_closed(
        maybe_subscription_closed: Result<SubscriptionClosedEvent, crossbeam_channel::RecvError>,
        account_update_subscriptions: &mut HashMap<Uuid, AccountUpdateSubscription>,
        partial_account_update_subscriptions: &mut HashMap<Uuid, PartialAccountUpdateSubscription>,
        slot_update_subscriptions: &mut HashMap<Uuid, SlotUpdateSubscription>,
    ) -> GeyserServiceResult<()> {
        let subscription_closed = maybe_subscription_closed?;
        match subscription_closed {
            SubscriptionClosedEvent::AccountUpdateSubscription(subscription_id) => {
                let _ = account_update_subscriptions.remove(&subscription_id);
            }
            SubscriptionClosedEvent::PartialAccountUpdateSubscription(subscription_id) => {
                let _ = partial_account_update_subscriptions.remove(&subscription_id);
            }
            SubscriptionClosedEvent::SlotUpdateSubscription(subscription_id) => {
                let _ = slot_update_subscriptions.remove(&subscription_id);
            }
        }

        Ok(())
    }

    /// Streams account updates to subscribers.
    fn handle_account_update_event(
        maybe_account_update: Result<AccountUpdate, crossbeam_channel::RecvError>,
        account_update_subscriptions: &HashMap<Uuid, AccountUpdateSubscription>,
        partial_account_update_subscriptions: &HashMap<Uuid, PartialAccountUpdateSubscription>,
    ) -> GeyserServiceResult<Vec<Uuid>> {
        let account_update = maybe_account_update?;

        let mut failed_subscription_ids = vec![];

        let messages = (0..account_update_subscriptions.len())
            .map(|_| account_update.clone())
            .collect::<Vec<_>>();
        failed_subscription_ids.extend(Self::send_account_update_messages(
            account_update_subscriptions,
            messages,
        ));

        let messages = (0..partial_account_update_subscriptions.len())
            .map(|_| PartialAccountUpdate {
                slot: account_update.slot,
                pubkey: account_update.pubkey.clone(),
                owner: account_update.owner.clone(),
                is_startup: account_update.is_startup,
                seq: account_update.seq,
                tx_signature: account_update.tx_signature.clone(),
                replica_version: account_update.replica_version,
            })
            .collect::<Vec<_>>();
        failed_subscription_ids.extend(Self::send_account_update_messages(
            partial_account_update_subscriptions,
            messages,
        ));

        Ok(failed_subscription_ids)
    }

    fn send_account_update_messages<S: AccountUpdateStreamer<T>, T>(
        subscriptions: &HashMap<Uuid, S>,
        updates: Vec<T>,
    ) -> Vec<Uuid> {
        assert_eq!(subscriptions.len(), updates.len());

        let mut failed_subscription_ids = vec![];
        for (sub, update) in subscriptions.iter().zip(updates) {
            if let Err(e) = sub.1.stream_update(update) {
                warn!("error sending message to client: {}", e);
                failed_subscription_ids.push(*sub.0);
            }
        }

        failed_subscription_ids
    }

    fn send_heartbeats<S: HeartbeatStreamer>(subscriptions: &HashMap<Uuid, S>) -> Vec<Uuid> {
        let mut failed_subscription_ids = vec![];
        for (sub_id, sub) in subscriptions {
            if let Err(e) = sub.send_heartbeat() {
                warn!("error sending heartbeat to client: {}", e);
                failed_subscription_ids.push(*sub_id);
            }
        }

        failed_subscription_ids
    }

    /// Streams slot updates to subscribers.
    fn handle_slot_update_event(
        maybe_slot_update: Result<SlotUpdate, crossbeam_channel::RecvError>,
        slot_update_subscriptions: &HashMap<Uuid, SlotUpdateSubscription>,
    ) -> GeyserServiceResult<Vec<Uuid>> {
        let slot_update = maybe_slot_update?;
        let mut failed_subscription_ids = vec![];

        for (sub_id, sub) in slot_update_subscriptions {
            if let Err(e) = sub.subscription_tx.try_send(Ok(slot_update.clone())) {
                warn!("error sending slot_update to client: {}", e);
                failed_subscription_ids.push(*sub_id)
            }
        }

        Ok(failed_subscription_ids)
    }

    /// Drop broken connections.
    fn drop_subscriptions<S: ErrorStatusStreamer>(
        subscription_ids: &[Uuid],
        subscriptions: &mut HashMap<Uuid, S>,
    ) {
        for sub_id in subscription_ids {
            if let Some(sub) = subscriptions.remove(sub_id) {
                let _ = sub.stream_error(Status::failed_precondition("broken connection"));
            }
        }
    }
}

#[tonic::async_trait]
impl Geyser for GeyserService {
    async fn get_heartbeat_interval(
        &self,
        _request: Request<EmptyRequest>,
    ) -> Result<Response<GetHeartbeatIntervalResponse>, Status> {
        Ok(Response::new(GetHeartbeatIntervalResponse {
            heartbeat_interval_ms: self.service_config.heartbeat_interval_ms,
        }))
    }

    type SubscribeAccountUpdatesStream = SubscriptionStream<Uuid, MaybeAccountUpdate>;
    async fn subscribe_account_updates(
        &self,
        request: Request<SubscribeAccountUpdatesRequest>,
    ) -> Result<Response<Self::SubscribeAccountUpdatesStream>, Status> {
        let (subscription_tx, subscription_rx) =
            channel(self.service_config.subscriber_buffer_size);

        let uuid = Uuid::new_v4();
        self.subscription_added_tx
            .try_send(SubscriptionAddedEvent::AccountUpdate {
                uuid,
                subscription_tx,
                skip_votes: request.into_inner().skip_vote_accounts,
            })
            .map_err(|e| {
                error!(
                    "failed to add subscribe_account_updates subscription: {}",
                    e
                );
                Status::internal("error adding subscription")
            })?;

        let stream = SubscriptionStream::new(
            subscription_rx,
            uuid,
            (
                self.subscription_closed_sender.clone(),
                SubscriptionClosedEvent::AccountUpdateSubscription(uuid),
            ),
            "subscribe_account_updates",
        );
        let mut resp = Response::new(stream);
        resp.metadata_mut().insert(
            HIGHEST_WRITE_SLOT_HEADER,
            MetadataValue::from(self.highest_write_slot.load(Ordering::Relaxed)),
        );

        Ok(resp)
    }

    type SubscribePartialAccountUpdatesStream = SubscriptionStream<Uuid, MaybePartialAccountUpdate>;
    async fn subscribe_partial_account_updates(
        &self,
        request: Request<SubscribePartialAccountUpdatesRequest>,
    ) -> Result<Response<Self::SubscribePartialAccountUpdatesStream>, Status> {
        let (subscription_tx, subscription_rx) =
            channel(self.service_config.subscriber_buffer_size);

        let uuid = Uuid::new_v4();
        self.subscription_added_tx
            .try_send(SubscriptionAddedEvent::PartialAccountUpdate {
                uuid,
                subscription_tx,
                skip_votes: request.into_inner().skip_vote_accounts,
            })
            .map_err(|e| {
                error!(
                    "failed to add subscribe_partial_account_updates subscription: {}",
                    e
                );
                Status::internal("error adding subscription")
            })?;

        let stream = SubscriptionStream::new(
            subscription_rx,
            uuid,
            (
                self.subscription_closed_sender.clone(),
                SubscriptionClosedEvent::PartialAccountUpdateSubscription(uuid),
            ),
            "subscribe_partial_account_updates",
        );
        let mut resp = Response::new(stream);
        resp.metadata_mut().insert(
            HIGHEST_WRITE_SLOT_HEADER,
            MetadataValue::from(self.highest_write_slot.load(Ordering::Relaxed)),
        );

        Ok(resp)
    }

    type SubscribeSlotUpdatesStream = SubscriptionStream<Uuid, SlotUpdate>;
    async fn subscribe_slot_updates(
        &self,
        _request: Request<EmptyRequest>,
    ) -> Result<Response<Self::SubscribeSlotUpdatesStream>, Status> {
        let (subscription_tx, subscription_rx) =
            channel(self.service_config.subscriber_buffer_size);

        let uuid = Uuid::new_v4();
        self.subscription_added_tx
            .try_send(SubscriptionAddedEvent::SlotUpdate {
                uuid,
                subscription_tx,
            })
            .map_err(|e| {
                error!("failed to add subscribe_slot_updates subscription: {}", e);
                Status::internal("error adding subscription")
            })?;

        let stream = SubscriptionStream::new(
            subscription_rx,
            uuid,
            (
                self.subscription_closed_sender.clone(),
                SubscriptionClosedEvent::SlotUpdateSubscription(uuid),
            ),
            "subscribe_slot_updates",
        );
        let mut resp = Response::new(stream);
        resp.metadata_mut().insert(
            HIGHEST_WRITE_SLOT_HEADER,
            MetadataValue::from(self.highest_write_slot.load(Ordering::Relaxed)),
        );

        Ok(resp)
    }
}
