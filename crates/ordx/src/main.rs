use std::cmp::max;
use std::collections::HashMap;
use std::sync::atomic::{AtomicU32, Ordering};
use std::thread;
use std::time::{Duration, Instant};

use bitcoin::constants::SUBSIDY_HALVING_INTERVAL;
use bitcoin::hashes::Hash;
use bitcoin::Txid;
use bitcoincore_rpc::RpcApi;
use log::{info, warn};

use ordinals::{Height, Rune, RuneId, SpacedRune, Terms};
use ordx::chain::Chain;
use ordx::db::RunesDB;
use ordx::entry::{RuneEntry, Statistic};
use ordx::rpc::{create_bitcoincore_rpc_client, with_retry};
use ordx::settings::Settings;
use ordx::updater::RuneUpdater;

#[tokio::main]
async fn main() {
    let settings = Settings::load();
    env_logger::init();
    info!("{}", &settings);
    let (rpc_client, chain) = create_bitcoincore_rpc_client(&settings).unwrap();
    let db_path = chain.join_with_data_dir(settings.data_dir.unwrap_or("./index".to_string()).as_str());
    info!("Using rocksdb at {:?}", db_path);
    let runes_db = RunesDB::new(db_path);

    // Create the first rune if it doesn't exist
    if chain == Chain::Mainnet {
        let id = RuneId { block: 1, tx: 0 };
        if runes_db.rune_id_to_rune_entry_get(&id).is_none() {
            let rune = Rune(2055900680524219742);
            let etching = Txid::all_zeros();
            runes_db.rune_to_rune_id_put(&rune, &id);
            runes_db.height_to_statistic_count_inc(&Statistic::Runes, 1);
            runes_db.rune_id_to_rune_entry_put(&id, &RuneEntry {
                block: id.block,
                burned: 0,
                divisibility: 0,
                etching,
                terms: Some(Terms {
                    amount: Some(1),
                    cap: Some(u128::MAX),
                    height: (
                        Some((SUBSIDY_HALVING_INTERVAL * 4).into()),
                        Some((SUBSIDY_HALVING_INTERVAL * 5).into()),
                    ),
                    offset: (None, None),
                }),
                mints: 0,
                number: 0,
                premine: 0,
                spaced_rune: SpacedRune { rune, spacers: 128 },
                symbol: Some('\u{29C9}'),
                timestamp: 0,
                turbo: true,
            });
            runes_db.transaction_id_to_rune_put(&etching, &rune);
        }
    }

    let first_rune_height = {
        if chain == Chain::Testnet {
            // testnet first rune height
            2583205
        } else {
            Rune::first_rune_height(chain.network())
        }
    };
    let started_height = runes_db.latest_indexed_height().unwrap_or(first_rune_height);

    let start_timestamp = Instant::now();

    let reorg_height = AtomicU32::new(0);
    let index_height = AtomicU32::new(started_height);
    info!("Starting from height: {}", index_height.load(Ordering::Relaxed));
    loop {
        let index_timestamp = Instant::now();
        let block = with_retry(|| {
            let latest_height: u32 = {
                match rpc_client.get_block_count() {
                    Ok(v) => v as _,
                    Err(e) => return Err(e.into())
                }
            };
            let h = index_height.load(Ordering::Relaxed);
            if latest_height < h {
                thread::sleep(Duration::from_secs(1));
                return Ok(None);
            }

            let block_hash = {
                match rpc_client.get_block_hash(h.into()) {
                    Ok(v) => v,
                    Err(e) => return Err(e.into())
                }
            };

            let block = {
                match rpc_client.get_block(&block_hash) {
                    Ok(v) => v,
                    Err(e) => return Err(e.into())
                }
            };
            let bitcoind_prev_blockhash = block.header.prev_blockhash;
            let mut prev_height = h - 1;
            let mut first_check = true;
            loop {
                if prev_height > first_rune_height {
                    let header = {
                        runes_db.height_to_block_header_get(prev_height)
                    };
                    match header {
                        None => {
                            let sh = {
                                runes_db.latest_indexed_height().unwrap_or(first_rune_height)
                            };
                            let to_height = sh.max(first_rune_height);
                            index_height.store(to_height, Ordering::Relaxed);
                            reorg_height.store(to_height, Ordering::Relaxed);
                            warn!("No header found for height: {}, resetting to: {}", prev_height, to_height);
                            return Ok(None);
                        }
                        Some(v) => {
                            if first_check {
                                first_check = false;
                                if v.block_hash() == bitcoind_prev_blockhash {
                                    break;
                                } else {
                                    prev_height = max(first_rune_height, prev_height - 1);
                                }
                            } else {
                                let block_hash = {
                                    match rpc_client.get_block_hash(prev_height.into()) {
                                        Ok(v) => v,
                                        Err(e) => return Err(e.into())
                                    }
                                };
                                if block_hash == v.block_hash() {
                                    let to_height = prev_height + 1;
                                    index_height.store(max(first_rune_height, to_height), Ordering::Relaxed);
                                    reorg_height.store(max(first_rune_height, to_height), Ordering::Relaxed);
                                    warn!("Block hash mismatch, resetting to: {}", to_height);
                                    return Ok(None);
                                }
                                prev_height = max(first_rune_height, prev_height - 1);
                            }
                        }
                    }
                } else {
                    break;
                }
            }
            Ok(Some((block, h, latest_height)))
        }, 10, Duration::from_millis(100)).await;
        match block {
            Ok(Some((block, block_height, latest_height))) => {
                let curr_reorg_height = reorg_height.load(Ordering::Relaxed);
                if curr_reorg_height != 0 {
                    if block_height > curr_reorg_height {
                        warn!("Skipping block: {}", block_height);
                        continue;
                    }
                    warn!("Reorg detected, resetting to height: {}", curr_reorg_height);
                    let start = Instant::now();
                    runes_db.reorg_to_height(curr_reorg_height - 1);
                    let elapsed = start.elapsed();
                    warn!("Reorg done, {:?}", elapsed);
                    reorg_height.store(0, Ordering::Relaxed);
                }
                let updater_timestamp = Instant::now();
                let runes_num_before = runes_db.height_to_statistic_count_sum_to_height(&Statistic::Runes, block_height);
                let mut rune_updater = RuneUpdater {
                    block_time: block.header.time,
                    burned: HashMap::new(),
                    client: &rpc_client,
                    height: block_height,
                    minimum: Rune::minimum_at_height(
                        chain.network(),
                        Height(block_height),
                    ),
                    runes: runes_num_before,
                    runes_db: &runes_db,
                };
                for (i, tx) in block.txdata.iter().enumerate() {
                    rune_updater.index_runes(u32::try_from(i).unwrap(), tx).await.unwrap();
                }
                rune_updater.update().unwrap();
                let runes_num_total = rune_updater.runes_num();

                let changed_count = runes_num_total - runes_num_before;
                if changed_count > 0 {
                    info!("Runes added: {}, total: {}", changed_count, rune_updater.runes_num());
                    runes_db.height_to_statistic_count_put(&Statistic::Runes, block_height, changed_count);
                }
                runes_db.height_to_block_header_put(block_height, &block.header);

                let remaining_height = latest_height - block_height;
                if remaining_height <= 3 {
                    info!("{}-{}({})={}({:.5}%), {:?}/{:?}", latest_height, block_height, block.txdata.len(), remaining_height, 100f64-(block_height as f64) * 100f64 / (latest_height as f64), updater_timestamp.elapsed(), index_timestamp.elapsed());
                } else {
                    let remaining = start_timestamp.elapsed() / (block_height - started_height + 1) * (remaining_height);
                    info!("{}-{}({})={}({:.5}%), {:?}/{:?}, {}", latest_height, block_height, block.txdata.len(), remaining_height, 100f64-(block_height as f64) * 100f64 / (latest_height as f64), updater_timestamp.elapsed(), index_timestamp.elapsed(), format_duration(remaining));
                }
                index_height.store(block_height + 1, Ordering::Relaxed);
            }
            _ => {
                warn!("No block found, retrying, {:?}", index_timestamp.elapsed());
            }
        }
    }
}

fn format_duration(duration: Duration) -> String {
    let total_seconds = duration.as_secs();
    let hours = total_seconds / 3600;
    let minutes = (total_seconds % 3600) / 60;
    let seconds = total_seconds % 60;
    let milliseconds = duration.subsec_millis();

    format!("{}h{}m{}s{}", hours, minutes, seconds, milliseconds)
}