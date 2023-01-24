use std::{
    str::FromStr,
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use clap::{Parser, Subcommand};
use futures_util::StreamExt;
use jito_geyser_protos::solana::geyser::{
    geyser_client::GeyserClient, EmptyRequest, SlotUpdateStatus, SubscribeAccountUpdatesRequest,
    SubscribeBlockUpdatesRequest, SubscribePartialAccountUpdatesRequest,
    SubscribeProgramsUpdatesRequest, SubscribeSlotUpdateRequest,
    SubscribeTransactionUpdatesRequest, TimestampedAccountUpdate,
};
use prost_types::Timestamp;
use solana_sdk::pubkey::Pubkey;
use tonic::{
    service::Interceptor,
    transport::{ClientTlsConfig, Endpoint},
    Status, Streaming,
};
use uuid::Uuid;

#[derive(Parser, Debug)]
#[clap(author, version, about, long_about = None)]
struct Args {
    #[clap(long, env, default_value = "mainnet.rpc.jito.wtf")]
    url: String,

    /// access token uuid
    #[clap(long, env, required = false)]
    access_token: Uuid,

    #[command(subcommand)]
    command: Commands,
}

#[derive(Debug, Subcommand)]
enum Commands {
    /// Subscribe to slot updates from Geyser
    Slots,

    /// Similar to Solana's programSubscribe, it will send you updates when an account owned by any of
    /// the programs have a change in state
    Programs {
        /// A space-separated list of programs to subscribe to
        #[clap(required = true)]
        programs: Vec<String>,
    },

    /// Subscribe to a set of accounts
    Accounts {
        /// A space-separated list of accounts to subscribe to
        #[clap(required = true)]
        accounts: Vec<String>,
    },

    /// Get the heartbeat interval
    GetHeartbeatInterval,

    /// Get partial account updates
    PartialAccounts { skip_votes: bool },

    /// Subscribe to transactions
    Transactions,

    /// Subscribe to blocks
    Blocks,
}

struct GrpcInterceptor {
    access_token: Uuid,
}

impl Interceptor for GrpcInterceptor {
    fn call(&mut self, mut request: tonic::Request<()>) -> Result<tonic::Request<()>, Status> {
        request.metadata_mut().insert(
            "access-token",
            self.access_token.to_string().parse().unwrap(),
        );
        Ok(request)
    }
}

#[tokio::main]
async fn main() {
    let args: Args = Args::parse();
    println!("args: {args:?}");

    // The geyser client must use https and ensure their OS and client is configured for TLS
    let url = format!("https://{}", args.url);
    let channel = Endpoint::from_str(&url)
        .expect("valid url")
        .tls_config(ClientTlsConfig::new())
        .expect("create tls config")
        .connect()
        .await
        .expect("connects");

    // The access token is provided as "access-token": "{uuid_v4}" in the request header
    let interceptor = GrpcInterceptor {
        access_token: args.access_token,
    };
    let mut client = GeyserClient::with_interceptor(channel, interceptor);

    match args.command {
        Commands::Slots => {
            let mut stream = client
                .subscribe_slot_updates(SubscribeSlotUpdateRequest {})
                .await
                .expect("subscribes to slot stream")
                .into_inner();
            while let Some(msg) = stream.next().await {
                match msg {
                    Ok(update) => {
                        let slot_update = update.slot_update.unwrap();
                        println!(
                            "slot: {} parent: {:?} status: {:?}",
                            slot_update.slot,
                            slot_update.parent_slot,
                            SlotUpdateStatus::from_i32(slot_update.status)
                        );
                    }
                    Err(e) => {
                        println!("subscribe_slot_updates error: {e:?}");
                    }
                }
            }
        }
        Commands::Programs { programs: accounts } => {
            println!("subscribing to programs: {accounts:?}");
            let response = client
                .subscribe_program_updates(SubscribeProgramsUpdatesRequest {
                    programs: accounts
                        .iter()
                        .map(|a| Pubkey::from_str(a).unwrap().to_bytes().to_vec())
                        .collect(),
                })
                .await
                .expect("subscribe to geyser")
                .into_inner();
            print_account_updates(response).await;
        }
        Commands::Accounts { accounts } => {
            println!("subscribing to accounts: {accounts:?}");
            let response = client
                .subscribe_account_updates(SubscribeAccountUpdatesRequest {
                    accounts: accounts
                        .iter()
                        .map(|a| Pubkey::from_str(a).unwrap().to_bytes().to_vec())
                        .collect(),
                })
                .await
                .expect("subscribe to geyser")
                .into_inner();
            print_account_updates(response).await;
        }
        Commands::GetHeartbeatInterval => {
            let response = client
                .get_heartbeat_interval(EmptyRequest {})
                .await
                .expect("gets heartbeat interval")
                .into_inner();
            println!("heartbeat interval: {response:?}");
        }
        Commands::PartialAccounts { skip_votes } => {
            let mut response = client
                .subscribe_partial_account_updates(SubscribePartialAccountUpdatesRequest {
                    skip_vote_accounts: skip_votes,
                })
                .await
                .expect("subscribes to partial updates")
                .into_inner();
            loop {
                let account_update = response
                    .message()
                    .await
                    .expect("get partial account update");
                match account_update {
                    None => {
                        println!("error receiving account update, exiting");
                    }
                    Some(partial_update) => {
                        println!("partial update: {partial_update:?}");
                    }
                }
            }
        }
        Commands::Transactions => {
            let mut response = client
                .subscribe_transaction_updates(SubscribeTransactionUpdatesRequest {})
                .await
                .expect("subscribes to transaction updates")
                .into_inner();
            loop {
                let transaction_update = response.message().await.expect("get transaction update");
                match transaction_update {
                    None => {
                        println!("error receiving account update, exiting");
                    }
                    Some(transaction_update) => {
                        println!("transaction update: {transaction_update:?}");
                    }
                }
            }
        }
        Commands::Blocks => {
            let mut response = client
                .subscribe_block_updates(SubscribeBlockUpdatesRequest {})
                .await
                .expect("subscribes to block updates")
                .into_inner();
            loop {
                let block_update = response.message().await.expect("get block update");
                match block_update {
                    None => {
                        println!("error receiving block update, exiting");
                    }
                    Some(block_update) => {
                        println!("block update: {block_update:?}");
                    }
                }
            }
        }
    }
}

// calculates a pseudo latency. assumes clocks are synced
fn calc_skew(ts: &Timestamp) -> f64 {
    let now = SystemTime::now().duration_since(UNIX_EPOCH).unwrap();
    let packet_ts = Duration::new(ts.seconds as u64, ts.nanos as u32);
    now.checked_sub(packet_ts)
        .map(|d| d.as_secs_f64())
        .unwrap_or_else(|| packet_ts.checked_sub(now).unwrap().as_secs_f64() * -1.0)
}

async fn print_account_updates(mut response: Streaming<TimestampedAccountUpdate>) {
    loop {
        let account_update = response.message().await.expect("get account update");
        match account_update {
            None => {
                println!("error, exiting...");
                break;
            }
            Some(update) => {
                let ts = update.ts.unwrap();
                let account_update = update.account_update.unwrap();
                let skew = calc_skew(&ts);
                println!(
                    "# {:?} slot: {:?} pubkey: {:?} clock skew: {:.3}s",
                    account_update.seq,
                    account_update.slot,
                    Pubkey::new(account_update.pubkey.as_slice()),
                    skew
                );
            }
        }
    }
}
