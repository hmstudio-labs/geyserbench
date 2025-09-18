use std::{ collections::HashMap, error::Error, str::FromStr, sync::{ Arc, Mutex }, time::{SystemTime, UNIX_EPOCH} };
use futures::channel::mpsc::unbounded;
use futures_util::{ stream::StreamExt, sink::SinkExt };
use rand::{Rng, RngCore};
use solana_sdk::{pubkey::Pubkey, signature::Keypair, signer::Signer, transaction::VersionedTransaction};
use tokio::{ sync::broadcast, task };
use tokio_stream::Stream;

use crate::{
    config::{ Config, Endpoint }, providers::nextstream::Nextstream::{next_stream_service_client::NextStreamServiceClient, next_stream_service_server::NextStreamService, NextStreamSubscription}, utils::{ get_current_timestamp, open_log_file, write_log_entry, Comparator, TransactionData }
};

use super::GeyserProvider;

pub mod Nextstream {
    #![allow(clippy::clone_on_ref_ptr)]
    #![allow(clippy::missing_const_for_fn)]

    include!(concat!(env!("OUT_DIR"), "/stream.rs"));

    pub const FILE_DESCRIPTOR_SET: &[u8] = tonic::include_file_descriptor_set!("proto_descriptors");
}

pub struct NextstreamProvider;

impl GeyserProvider for NextstreamProvider {
    fn process(
        &self,
        endpoint: Endpoint,
        config: Config,
        shutdown_tx: broadcast::Sender<()>,
        shutdown_rx: broadcast::Receiver<()>,
        start_time: f64,
        comparator: Arc<Mutex<Comparator>>
    ) -> task::JoinHandle<Result<(), Box<dyn Error + Send + Sync>>> {
        task::spawn(async move {
            process_shredstream_endpoint(
                endpoint,
                config,
                shutdown_tx,
                shutdown_rx,
                start_time,
                comparator
            ).await
        })
    }
}

async fn process_shredstream_endpoint(
    endpoint: Endpoint,
    config: Config,
    shutdown_tx: broadcast::Sender<()>,
    mut shutdown_rx: broadcast::Receiver<()>,
    start_time: f64,
    comparator: Arc<Mutex<Comparator>>
) -> Result<(), Box<dyn Error + Send + Sync>> {
    let mut transaction_count = 0;

    let mut log_file = open_log_file(&endpoint.name)?;

    log::info!("[{}] Connecting to endpoint: {}", endpoint.name, endpoint.url);
    let endpoint_url_clone=  endpoint.url.clone();
    let domain = endpoint_url_clone.trim_start_matches("http://").trim_start_matches("https://");
    let mut client = NextStreamServiceClient::connect(endpoint.url).await?;
    log::info!("[{}] Connected successfully", endpoint.name);

    let authentication_keypair = Keypair::from_base58_string(&endpoint.x_token);
    let authentication_pubkey = authentication_keypair.pubkey();
    let authentication_message = build_auth_message(domain, &authentication_pubkey);
    let authentication_signature =
        authentication_keypair.sign_message(authentication_message.as_bytes());

    
    let mut stream = client
        .subscribe_next_stream(NextStreamSubscription {
            authentication_publickey: authentication_pubkey.to_string(),
            authentication_message: authentication_message,
            authentication_signature: authentication_signature.to_string(),
            accounts: vec![config.account.clone()],
        })
        .await
        .unwrap()
        .into_inner();

    'ploop: loop {
        tokio::select! {
            _ = shutdown_rx.recv() => {
                log::info!("[{}] Received stop signal...", endpoint.name);
                break;
            }

            message = stream.next() => {
                if let Some(Ok(msg)) = message {
                    if let Some(packet) = msg.packet {
                        let tx: VersionedTransaction = bincode::deserialize(&packet.transaction)?;
                        let accounts = tx.message.static_account_keys()
                            .iter()
                            .map(|e| e.to_string())
                            .collect::<Vec<String>>();
                        if accounts.contains(&config.account) {
                            let timestamp = get_current_timestamp();
                            let signature = tx.signatures[0].to_string();

                            write_log_entry(&mut log_file, timestamp, &endpoint.name, &signature)?;

                            let mut comp = comparator.lock().unwrap();

                            comp.add(
                                endpoint.name.clone(),
                                TransactionData {
                                    timestamp,
                                    signature: signature.clone(),
                                    start_time,
                                },
                            );

                            if comp.get_valid_count() == config.transactions as usize {
                                log::info!("Endpoint {} shutting down after {} transactions seen and {} by all workers",
                                    endpoint.name, transaction_count, config.transactions);
                                shutdown_tx.send(()).unwrap();
                                break 'ploop;
                            }

                            log::info!("[{:.3}] [{}] {}", timestamp, endpoint.name, signature);
                            transaction_count += 1;
                        }
                    }
                }
            }
        }
    }

    log::info!("[{}] Stream closed", endpoint.name);
    Ok(())
}

fn build_auth_message(domain: &str, pubkey: &Pubkey) -> String {
    let nonce: u64 = rand::rng().random();
    let ts = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_secs();
    format!("{}|{}|{}|{}", domain, pubkey.to_string(), nonce, ts)
}