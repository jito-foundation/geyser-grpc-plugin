//! Implements the geyser plugin interface.

use std::{
    fs::File,
    io::Read,
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc,
    },
};

use bs58;
use crossbeam_channel::{bounded, Sender, TrySendError};
use log::*;
use serde_derive::Deserialize;
use serde_json;
use solana_geyser_plugin_interface::geyser_plugin_interface::{
    GeyserPlugin, GeyserPluginError, ReplicaAccountInfoVersions, Result as PluginResult, SlotStatus,
};
use tokio::{runtime::Runtime, sync::oneshot};
use tonic::transport::Server;

use crate::{
    accounts_selector::AccountsSelector,
    geyser_proto::{geyser_server::GeyserServer, AccountUpdate, SlotUpdate, SlotUpdateStatus},
    server::{GeyserService, GeyserServiceConfig},
};

pub struct PluginData {
    runtime: Runtime,
    server_exit_tx: oneshot::Sender<()>,
    accounts_selector: AccountsSelector,

    /// Where updates are piped thru to the grpc service.
    account_update_tx: Sender<AccountUpdate>,
    slot_update_tx: Sender<SlotUpdate>,

    /// Highest slot that an account write has been processed for thus far.
    highest_write_slot: Arc<AtomicU64>,
}

#[derive(Default)]
pub struct GeyserGrpcPlugin {
    /// Initialized on initial plugin load.
    data: Option<PluginData>,
}

impl std::fmt::Debug for GeyserGrpcPlugin {
    fn fmt(&self, _: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        Ok(())
    }
}

#[derive(Clone, Debug, Deserialize)]
pub struct PluginConfig {
    pub geyser_service_config: GeyserServiceConfig,
    pub bind_address: String,
    pub account_update_buffer_size: usize,
    pub slot_update_buffer_size: usize,
}

impl GeyserPlugin for GeyserGrpcPlugin {
    fn name(&self) -> &'static str {
        "geyser-grpc-plugin"
    }

    fn on_load(&mut self, config_path: &str) -> PluginResult<()> {
        solana_logger::setup_with_default("info");
        info!(
            "Loading plugin {:?} from config_path {:?}",
            self.name(),
            config_path
        );

        let mut file = File::open(config_path)?;
        let mut buf = String::new();
        file.read_to_string(&mut buf)?;

        let result: serde_json::Value = serde_json::from_str(&buf).unwrap();
        let accounts_selector = AccountsSelector::from(&result["accounts_selector"]);

        let config: PluginConfig =
            serde_json::from_str(&buf).map_err(|err| GeyserPluginError::ConfigFileReadError {
                msg: format!("Error deserializing PluginConfig: {:?}", err),
            })?;

        let addr =
            config
                .bind_address
                .parse()
                .map_err(|err| GeyserPluginError::ConfigFileReadError {
                    msg: format!("Error parsing the bind_address {:?}", err),
                })?;

        let highest_write_slot = Arc::new(AtomicU64::new(0));
        let (account_update_tx, account_update_rx) = bounded(config.account_update_buffer_size);
        let (slot_update_tx, slot_update_rx) = bounded(config.slot_update_buffer_size);
        let svc = GeyserService::new(
            config.geyser_service_config,
            account_update_rx,
            slot_update_rx,
            highest_write_slot.clone(),
        );
        let svc = GeyserServer::new(svc);

        let runtime = Runtime::new().unwrap();
        let (server_exit_tx, server_exit_rx) = oneshot::channel();
        runtime.spawn(
            Server::builder()
                .add_service(svc)
                .serve_with_shutdown(addr, async move {
                    let _ = server_exit_rx.await;
                }),
        );

        self.data = Some(PluginData {
            runtime,
            server_exit_tx,
            accounts_selector,
            account_update_tx,
            slot_update_tx,
            highest_write_slot,
        });
        info!("plugin data initialized");

        Ok(())
    }

    fn on_unload(&mut self) {
        info!("Unloading plugin: {:?}", self.name());

        let data = self.data.take().expect("plugin not initialized");
        data.server_exit_tx
            .send(())
            .expect("sending grpc server termination should succeed");
        data.runtime.shutdown_background();
    }

    fn update_account(
        &mut self,
        account: ReplicaAccountInfoVersions,
        slot: u64,
        is_startup: bool,
    ) -> PluginResult<()> {
        let data = self.data.as_ref().expect("plugin must be initialized");
        let account_update = match account {
            ReplicaAccountInfoVersions::V0_0_1(account) => AccountUpdate {
                slot,
                pubkey: account.pubkey.to_vec(),
                lamports: account.lamports,
                owner: account.owner.to_vec(),
                is_executable: account.executable,
                rent_epoch: account.rent_epoch,
                data: account.data.to_vec(),
                seq: account.write_version,
                is_startup,
                tx_signature: None,
                replica_version: 1,
            },
            ReplicaAccountInfoVersions::V0_0_2(account) => {
                let tx_signature = account.txn_signature.map(|sig| sig.to_string());
                AccountUpdate {
                    slot,
                    pubkey: account.pubkey.to_vec(),
                    lamports: account.lamports,
                    owner: account.owner.to_vec(),
                    is_executable: account.executable,
                    rent_epoch: account.rent_epoch,
                    data: account.data.to_vec(),
                    seq: account.write_version,
                    is_startup,
                    tx_signature,
                    replica_version: 2,
                }
            }
        };

        if account_update.pubkey.len() != 32 {
            error!(
                "bad account pubkey length: {}",
                bs58::encode(account_update.pubkey).into_string()
            );
            return Ok(());
        }
        if account_update.owner.len() != 32 {
            error!(
                "bad account owner pubkey length: {}",
                bs58::encode(account_update.owner).into_string()
            );
            return Ok(());
        }

        // Select only accounts configured to look at.
        let is_selected = data
            .accounts_selector
            .is_account_selected(&account_update.pubkey[..], &account_update.owner[..]);
        if !is_selected {
            return Ok(());
        }

        data.highest_write_slot.fetch_max(slot, Ordering::SeqCst);

        debug!(
            "Streaming AccountUpdate {:?} with owner {:?} at slot {:?}",
            bs58::encode(&account_update.pubkey[..]).into_string(),
            bs58::encode(&account_update.owner[..]).into_string(),
            slot,
        );

        match data.account_update_tx.try_send(account_update) {
            Ok(_) => Ok(()),
            Err(TrySendError::Full(_)) => {
                warn!("account_update channel full, skipping");
                Ok(())
            }
            Err(TrySendError::Disconnected(_)) => {
                error!("account send error");
                Err(GeyserPluginError::AccountsUpdateError {
                    msg: "account_update channel disconnected, exiting".to_string(),
                })
            }
        }
    }

    fn notify_end_of_startup(&mut self) -> PluginResult<()> {
        Ok(())
    }

    fn update_slot_status(
        &mut self,
        slot: u64,
        parent_slot: Option<u64>,
        status: SlotStatus,
    ) -> PluginResult<()> {
        let data = self.data.as_ref().expect("plugin must be initialized");
        debug!("Updating slot {:?} at with status {:?}", slot, status);

        let status = match status {
            SlotStatus::Processed => SlotUpdateStatus::Processed,
            SlotStatus::Confirmed => SlotUpdateStatus::Confirmed,
            SlotStatus::Rooted => SlotUpdateStatus::Rooted,
        };

        match data.slot_update_tx.try_send(SlotUpdate {
            slot,
            parent_slot,
            status: status as i32,
        }) {
            Ok(_) => Ok(()),
            Err(TrySendError::Full(_)) => {
                warn!("slot_update channel full, skipping");
                Ok(())
            }
            Err(TrySendError::Disconnected(_)) => {
                error!("slot send error");
                Err(GeyserPluginError::SlotStatusUpdateError {
                    msg: "slot_update channel disconnected, exiting".to_string(),
                })
            }
        }
    }
}

#[no_mangle]
#[allow(improper_ctypes_definitions)]
/// # Safety
///
/// This function returns the Plugin pointer as trait GeyserPlugin.
pub unsafe extern "C" fn _create_plugin() -> *mut dyn GeyserPlugin {
    let plugin = GeyserGrpcPlugin::default();
    let plugin: Box<dyn GeyserPlugin> = Box::new(plugin);
    Box::into_raw(plugin)
}

#[cfg(test)]
pub(crate) mod tests {
    use serde_json;
    use solana_program::pubkey::Pubkey;

    use super::*;

    #[test]
    fn test_accounts_selector_from_config() {
        let config = "{\"accounts_selector\" : { \
           \"owners\" : [\"9xQeWvG816bUx9EPjHmaT23yvVM2ZWbrrpZb9PusVFin\"] \
        }}";

        let config: serde_json::Value = serde_json::from_str(config).unwrap();
        let accounts_selector = AccountsSelector::from(&config["accounts_selector"]);

        assert_eq!(accounts_selector.owners.len(), 1);

        let owners = accounts_selector
            .owners
            .into_iter()
            .collect::<Vec<Vec<u8>>>();
        let owner = &owners[0];
        let owner = Pubkey::from(<[u8; 32]>::try_from(owner.as_slice()).unwrap());
        assert_eq!(
            owner.to_string(),
            "9xQeWvG816bUx9EPjHmaT23yvVM2ZWbrrpZb9PusVFin"
        );
    }
}
