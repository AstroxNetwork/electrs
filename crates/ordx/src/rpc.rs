use std::time::Duration;

use anyhow::{bail, Context};
use bitcoincore_rpc::{Auth, Client, RpcApi};
use log::{error, info};
use tokio::time::sleep;

use crate::chain::Chain;
use crate::settings::Settings;

pub fn create_bitcoincore_rpc_client(settings: &Settings) -> anyhow::Result<(Client, Chain)> {
    let bitcoin_rpc_url = settings.bitcoin_rpc_url.as_ref().expect("BITCOIN_RPC_URL is required");
    let bitcoin_rpc_username = settings.bitcoin_rpc_username.as_ref().expect("BITCOIN_RPC_USERNAME is required");
    let bitcoin_rpc_password = settings.bitcoin_rpc_password.as_ref().expect("BITCOIN_RPC_PASSWORD is required");

    info!("Connecting to Bitcoin Core RPC at {}", bitcoin_rpc_url);

    let client = Client::new(bitcoin_rpc_url, Auth::UserPass(bitcoin_rpc_username.clone(), bitcoin_rpc_password.clone()))
        .with_context(|| format!("Failed to connect to Bitcoin Core RPC at {}", bitcoin_rpc_url)).unwrap();


    let result: bitcoincore_rpc::Result<serde_json::Value> = client.call("getblockchaininfo", &[]);

    info!("Got blockchain info: {:?}", &result);

    let binding = result.unwrap();
    let chain_str = binding.as_object().unwrap().get("chain").unwrap().as_str().unwrap();
    let rpc_chain = match chain_str {
        "main" | "mainnet" => Chain::Mainnet,
        "test" | "testnet" => Chain::Testnet,
        "test4" | "testnet4" => Chain::Testnet4,
        "regtest" => Chain::Regtest,
        "signet" => Chain::Signet,
        other => bail!("Bitcoin RPC server on unknown chain: {other}"),
    };

    let ord_chain = settings.network.as_ref().expect("network is required").parse::<Chain>().unwrap();

    if rpc_chain != ord_chain {
        bail!("Bitcoin RPC server is on {rpc_chain} but ord is on {ord_chain}");
    }

    Ok((client, ord_chain))
}

pub async fn with_retry<F, T>(mut call: F, attempts: u8, delay: Duration) -> anyhow::Result<T>
where
    F: FnMut() -> anyhow::Result<T>,
{
    let mut attempt: u8 = 0;
    loop {
        let ret = call();
        match ret {
            Ok(result) => return Ok(result),
            Err(e) if attempt < attempts - 1 => {
                attempt += 1;
                let duration = delay * 2u32.pow(attempt as _);
                sleep(duration).await;
                error!("{}, retrying operation, attempt: {}, duration: {:?}", e, attempt,duration);
            }
            Err(e) => return Err(e),
        }
    }
}