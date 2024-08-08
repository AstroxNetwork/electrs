use std::cmp::Ordering;
use std::collections::{HashMap, HashSet};
use std::str::FromStr;
use std::sync::Arc;

use axum::{Extension, Json};
use axum::extract::{Path, Query};
use axum::response::IntoResponse;
use bitcoin::{Address, OutPoint, Transaction};
use bitcoin::psbt::Psbt;
use bitcoincore_rpc::json::Bip125Replaceable::No;
use itertools::Itertools;
use log::info;
use rusqlite::params;
use serde_json::{json, Value};

use ordinals::{Artifact, Edict, Rune, RuneId, Runestone, SpacedRune};

use crate::api::dto::{AddressRuneUTXOsDTO, AppError, ExpandRuneEntry, OutputsDTO, Paged, R, RuneEntryDTO, RunesPageParams, RunesPSBTParams, RunesTxDTO, RunesTxParams, RuneTx, UTXOWithRuneValueDTO};
use crate::api::util::hex_to_base64;
use crate::api::vo::RuneBalanceGroupKey;
use crate::cache::{CacheKey, CacheMethod, MokaCache};
use crate::db::model::RuneEntryForQueryInsert;
use crate::db::RunesDB;
use crate::into_usize::IntoUsize;
use crate::lot::Lot;
use crate::updater::RuneUpdater;

fn format_size(bytes: u64) -> String {
    let sizes = ["Bytes", "KB", "MB", "GB", "TB", "PB", "EB", "ZB", "YB"];
    let factor = 1024.0;

    if bytes < factor as u64 {
        return format!("{} Bytes", bytes);
    }

    let mut size = bytes as f64;
    let mut i = 0;
    while size >= factor && i < sizes.len() - 1 {
        size /= factor;
        i += 1;
    }

    format!("{:.2} {}", size, sizes[i])
}

pub async fn stats(
    Extension(db): Extension<Arc<RunesDB>>,
) -> anyhow::Result<Json<R<Value>>, AppError> {
    let indexed_height = db.latest_indexed_height();
    let latest_height = db.latest_height();
    let remaining_height = latest_height.unwrap_or_default() - indexed_height.unwrap_or_default();
    let db_size = fs_extra::dir::get_size(db.rocksdb.path().parent().unwrap())?;
    Ok(Json(R::with_data(json!({
        "indexer": {
            "indexed_height": indexed_height,
            "latest_height": latest_height,
            "remaining_height": remaining_height,
            "remaining_percentage": format!("{:.5}%", remaining_height as f64 / latest_height.unwrap_or_default() as f64 * 100.0)
        },
        "binary": {
            "version": env!("CARGO_PKG_VERSION"),
            "timestamp": env!("VERGEN_BUILD_TIMESTAMP"),
            "target": env!("VERGEN_CARGO_TARGET_TRIPLE"),
            "rustc": env!("VERGEN_RUSTC_SEMVER"),
        },
        "db": format_size(db_size),
    }))))
}

pub async fn block_height(
    Extension(db): Extension<Arc<RunesDB>>,
) -> anyhow::Result<Json<R<Option<u32>>>, AppError> {
    let latest_height = db.latest_height();
    Ok(Json(R::with_data(latest_height)))
}


pub async fn get_rune_by_id(
    Extension(cache): Extension<Arc<MokaCache>>,
    Extension(db): Extension<Arc<RunesDB>>,
    Path(id): Path<String>,
) -> anyhow::Result<Json<Option<Value>>, AppError> {
    let rune_id = {
        if let Ok(id) = RuneId::from_str(&id) {
            Some(id)
        } else if let Ok(v) = SpacedRune::from_str(&id) {
            db.rune_to_rune_id_get(&v.rune)
        } else if let Ok(v) = Rune::from_str(&id) {
            db.rune_to_rune_id_get(&v)
        } else {
            None
        }
    };

    if rune_id.is_none() {
        return Ok(Json(None));
    }

    let cache_key = CacheKey::new(CacheMethod::HandlerRuneById, Value::String(id.clone()));
    if let Some(value) = cache.get(&cache_key).await {
        return Ok(Json(Some(value)));
    }

    let entry: Option<RuneEntryDTO> = db.sqlite_rune_entry_get_by_id(rune_id.unwrap().to_string()).unwrap_or(None).map(|x| x.into());
    let r = R::with_data(entry);
    let value = serde_json::to_value(r)?;
    let mut cloned = value.clone();
    cloned["cache"] = Value::Bool(true);
    cache.insert(cache_key, cloned).await;
    Ok(Json(Some(value)))
}


pub async fn paged_runes(
    Extension(cache): Extension<Arc<MokaCache>>,
    Extension(db): Extension<Arc<RunesDB>>,
    Query(params): Query<RunesPageParams>,
) -> anyhow::Result<Json<Value>, AppError> {
    let cache_key = CacheKey::new(CacheMethod::HandlerPagedRunes, serde_json::to_value(&params)?);
    if let Some(value) = cache.get(&cache_key).await {
        return Ok(Json(value));
    }
    let (next, list) = db.rune_entry_paged(
        params.cursor.unwrap_or(0).max(0),
        params.size.unwrap_or(10).clamp(1, 1000),
        params.keywords,
        params.sort,
    );
    let latest_height = db.latest_height().unwrap_or_default();
    let runes = list.iter().map(|x| ExpandRuneEntry::load(x.0, x.1, latest_height)).collect::<Vec<_>>();
    let r = R::with_data(Paged::new(next, runes));
    let value = serde_json::to_value(r)?;
    let mut cloned = value.clone();
    cloned["cache"] = Value::Bool(true);
    cache.insert(cache_key, cloned).await;
    Ok(Json(value))
}


fn decode_runes_tx(db: &RunesDB, tx: Transaction) -> anyhow::Result<RunesTxDTO> {
    let mut runes_set = HashSet::new();
    let mut inputs = HashMap::new();
    let mut unallocated: HashMap<RuneId, Lot> = HashMap::new();
    let mut allocated: Vec<HashMap<RuneId, Lot>> = vec![HashMap::new(); tx.output.len()];
    for (index, vin) in tx.input.iter().enumerate() {
        let point = vin.previous_output;
        if let Some(v) = db.outpoint_to_rune_balances_get(&point) {
            let balances_buffer = v.2;
            let mut balance_map = HashMap::new();
            let mut i = 0;
            while i < balances_buffer.len() {
                let ((id, balance), length) = RuneUpdater::decode_rune_balance(&balances_buffer[i..]).unwrap();
                i += length;
                *unallocated.entry(id).or_default() += balance;
                balance_map.insert(id, balance);
                runes_set.insert(id);
            }
            inputs.insert(index, balance_map);
        }
    }

    let mut actions = HashSet::new();
    let artifact = Runestone::decipher(&tx);
    if let Some(artifact) = &artifact {
        let mint = |id: RuneId| -> anyhow::Result<Option<Lot>> {
            let Some(rune_entry) = db.rune_id_to_rune_entry_get(&id) else {
                return Ok(None);
            };
            Ok(rune_entry.terms.and_then(|terms| terms.amount.map(Lot)))
        };

        if let Some(id) = artifact.mint() {
            if let Some(amount) = mint(id)? {
                *unallocated.entry(id).or_default() += amount;
                actions.insert("mint".to_string());
            }
        }

        let etching = || -> Option<(RuneId, Rune)> {
            let rune = match artifact {
                Artifact::Runestone(runestone) => match runestone.etching {
                    Some(etching) => etching.rune,
                    None => return None,
                },
                Artifact::Cenotaph(cenotaph) => match cenotaph.etching {
                    Some(rune) => Some(rune),
                    None => return None,
                },
            };
            if let Some(rune) = rune {
                return db.rune_to_rune_id_get(&rune).map(|id| (id, rune));
            }
            None
        };

        let etched = etching();

        if let Artifact::Runestone(runestone) = artifact {
            if let Some((id, ..)) = etched {
                *unallocated.entry(id).or_default() +=
                    runestone.etching.unwrap().premine.unwrap_or_default();
                actions.insert("etching".to_string());
                runes_set.insert(id);
            }

            for Edict { id, amount, output } in runestone.edicts.iter().copied() {
                let amount = Lot(amount);

                // edicts with output values greater than the number of outputs
                // should never be produced by the edict parser
                let output = usize::try_from(output).unwrap();
                assert!(output <= tx.output.len());

                let id = if id == RuneId::default() {
                    let Some((id, ..)) = etched else {
                        continue;
                    };

                    id
                } else {
                    id
                };

                let Some(balance) = unallocated.get_mut(&id) else {
                    continue;
                };

                let mut allocate = |balance: &mut Lot, amount: Lot, output: usize| {
                    if amount > 0 {
                        *balance -= amount;
                        *allocated[output].entry(id).or_default() += amount;
                    }
                };

                if output == tx.output.len() {
                    // find non-OP_RETURN outputs
                    let destinations = tx
                        .output
                        .iter()
                        .enumerate()
                        .filter_map(|(output, tx_out)| {
                            (!tx_out.script_pubkey.is_op_return()).then_some(output)
                        })
                        .collect::<Vec<usize>>();

                    if !destinations.is_empty() {
                        if amount == 0 {
                            // if amount is zero, divide balance between eligible outputs
                            let amount = *balance / destinations.len() as u128;
                            let remainder = usize::try_from(*balance % destinations.len() as u128).unwrap();

                            for (i, output) in destinations.iter().enumerate() {
                                allocate(
                                    balance,
                                    if i < remainder { amount + 1 } else { amount },
                                    *output,
                                );
                            }
                        } else {
                            // if amount is non-zero, distribute amount to eligible outputs
                            for output in destinations {
                                allocate(balance, amount.min(*balance), output);
                            }
                        }
                    }
                } else {
                    // Get the allocatable amount
                    let amount = if amount == 0 {
                        *balance
                    } else {
                        amount.min(*balance)
                    };

                    allocate(balance, amount, output);
                }
            }
        }
    }

    let mut burned: HashMap<RuneId, Lot> = HashMap::new();
    if let Some(Artifact::Cenotaph(_)) = artifact {
        for (id, balance) in unallocated {
            *burned.entry(id).or_default() += balance;
        }
    } else {
        let pointer = artifact
            .map(|artifact| match artifact {
                Artifact::Runestone(runestone) => runestone.pointer,
                Artifact::Cenotaph(_) => unreachable!(),
            })
            .unwrap_or_default();

        // assign all un-allocated runes to the default output, or the first non
        // OP_RETURN output if there is no default
        if let Some(vout) = pointer
            .map(|pointer| pointer.into_usize())
            .inspect(|&pointer| assert!(pointer < allocated.len()))
            .or_else(|| {
                tx.output
                    .iter()
                    .enumerate()
                    .find(|(_vout, tx_out)| !tx_out.script_pubkey.is_op_return())
                    .map(|(vout, _tx_out)| vout)
            })
        {
            for (id, balance) in unallocated {
                if balance > 0 {
                    *allocated[vout].entry(id).or_default() += balance;
                }
            }
        } else {
            for (id, balance) in unallocated {
                if balance > 0 {
                    *burned.entry(id).or_default() += balance;
                }
            }
        }
    }

    let mut outputs = HashMap::new();
    // update outpoint balances
    for (vout, balances) in allocated.into_iter().enumerate() {
        if balances.is_empty() {
            continue;
        }
        // increment burned balances
        if tx.output[vout].script_pubkey.is_op_return() {
            for (id, balance) in &balances {
                *burned.entry(*id).or_default() += *balance;
            }
            continue;
        }
        outputs.insert(vout, balances);
    }


    let latest_height = db.latest_height().unwrap_or_default();
    let mut runes = vec![];
    for x in runes_set {
        let r = db.rune_id_to_rune_entry_get(&x).unwrap();
        runes.push(ExpandRuneEntry::load(x, r, latest_height));
    }

    if !burned.is_empty() {
        actions.insert("burn".to_string());
    }

    if !inputs.is_empty() {
        actions.insert("transfer".to_string());
    }
    Ok(RunesTxDTO {
        runes,
        inputs,
        outputs,
        burned,
        actions: actions.into_iter().collect(),
    })
}


pub async fn runes_decode_psbt(
    Extension(db): Extension<Arc<RunesDB>>,
    Json(params): Json<RunesPSBTParams>,
) -> anyhow::Result<Json<R<RunesTxDTO>>, AppError> {
    let base64 = hex_to_base64(params.get_psbt_hex().expect("`psbtHex` is required."))?;
    let psbt = Psbt::from_str(&base64)?;
    let x = decode_runes_tx(&db, psbt.unsigned_tx)?;
    Ok(Json(R::with_data(x)))
}


pub async fn runes_decode_tx(
    Extension(db): Extension<Arc<RunesDB>>,
    Json(params): Json<RunesTxParams>,
) -> anyhow::Result<Json<R<RunesTxDTO>>, AppError> {
    let bytes = hex::decode(params.get_raw_tx().unwrap())?;
    let tx = bitcoin::consensus::deserialize(&bytes)?;
    let x = decode_runes_tx(&db, tx)?;
    Ok(Json(R::with_data(x)))
}

pub async fn outputs_runes(
    Extension(db): Extension<Arc<RunesDB>>,
    Json(outpoints): Json<Vec<String>>,
) -> anyhow::Result<Json<R<OutputsDTO>>, AppError> {
    if outpoints.is_empty() {
        return Ok(Json(R::with_data(OutputsDTO::default())));
    }
    let mut runes_set = HashSet::new();
    let mut outputs = vec![];
    for outpoint in outpoints {
        let outpoint = OutPoint::from_str(&outpoint)?;
        let mut balance_map = HashMap::new();
        if let Some(v) = db.outpoint_to_rune_balances_get(&outpoint) {
            let balances_buffer = v.2;
            let mut i = 0;
            while i < balances_buffer.len() {
                let ((id, balance), length) = RuneUpdater::decode_rune_balance(&balances_buffer[i..])?;
                i += length;
                balance_map.insert(id, balance);
                runes_set.insert(id);
            }
        }
        outputs.push(balance_map);
    }
    let latest_height = db.latest_height().unwrap_or_default();
    let mut runes = vec![];
    for x in runes_set {
        let r = db.rune_id_to_rune_entry_get(&x).unwrap();
        runes.push(ExpandRuneEntry::load(x, r, latest_height));
    }
    Ok(Json(R::with_data(OutputsDTO { runes, outputs })))
}

pub async fn get_runes_by_rune_ids(
    Extension(db): Extension<Arc<RunesDB>>,
    Json(rune_ids): Json<Vec<String>>,
) -> anyhow::Result<Json<R<Vec<Option<ExpandRuneEntry>>>>, AppError> {
    let mut runes = vec![];
    if rune_ids.is_empty() {
        return Ok(Json(R::with_data(runes)));
    }
    let latest_height = db.latest_height().unwrap_or_default();
    for x in rune_ids {
        match RuneId::from_str(&x) {
            Ok(id) => match db.rune_id_to_rune_entry_get(&id) {
                None => runes.push(None),
                Some(v) => {
                    runes.push(Some(ExpandRuneEntry::load(id, v, latest_height)));
                }
            },
            Err(_) => runes.push(None),
        }
    }
    Ok(Json(R::with_data(runes)))
}

pub async fn get_tx(
    Extension(cache): Extension<Arc<MokaCache>>,
    Extension(db): Extension<Arc<RunesDB>>,
    Path(txid): Path<String>,
) -> anyhow::Result<Json<Option<Value>>, AppError> {
    bitcoin::Txid::from_str(&txid)?;
    let cache_key = CacheKey::new(CacheMethod::HandlerTx, Value::String(txid.clone()));
    if let Some(value) = cache.get(&cache_key).await {
        return Ok(Json(Some(value)));
    }

    let rows = db.sqlite_rune_balance_list_by_txid(&txid)?;
    let etching_rune_entry = db.sqlite_rune_entry_get_by_etching_txid(&txid)?;

    if rows.is_empty() && etching_rune_entry.is_none() {
        let r = R::with_data(RuneTx::default());
        let value = serde_json::to_value(r)?;
        let mut cloned = value.clone();
        cloned["cache"] = Value::Bool(true);
        cache.insert(cache_key, cloned).await;
        return Ok(Json(Some(value)));
    }

    if rows.is_empty() && etching_rune_entry.is_some() {
        let r = R::with_data(RuneTx {
            runes: vec![etching_rune_entry.unwrap().into()],
            actions: vec!["etching".into()],
            inputs: HashMap::new(),
            outputs: HashMap::new(),
            burned: HashMap::new(),
            minted: HashMap::new(),
            premine: HashMap::new(),
        });
        let value = serde_json::to_value(r)?;
        let mut cloned = value.clone();
        cloned["cache"] = Value::Bool(true);
        cache.insert(cache_key, cloned).await;
        return Ok(Json(Some(value)));
    }


    let mut rune_ids = HashSet::new();
    let mut inputs_balance_map = HashMap::new();
    let mut outputs_balance_map = HashMap::new();
    let mut inputs = HashMap::new();
    let mut outputs = HashMap::new();
    let mut actions = HashSet::new();
    let rows_map = rows.iter().into_group_map_by(|x| RuneBalanceGroupKey {
        txid: x.txid.clone(),
        vout: x.vout,
    });
    for (k, v) in rows_map {
        // outputs
        if k.txid == txid {
            let mut balance_map = HashMap::new();
            for e in v {
                rune_ids.insert(e.rune_id.clone());
                balance_map.insert(e.rune_id.clone(), e.rune_amount.clone());
                let x1 = outputs_balance_map.entry(e.rune_id.clone()).or_insert(0);
                *x1 += e.rune_amount.parse::<u128>().unwrap();
                e.with_actions(&mut actions);
            }
            outputs.insert(k.vout, balance_map);
        } else {
            let mut balance_map = HashMap::new();
            for e in v {
                rune_ids.insert(e.rune_id.clone());
                balance_map.insert(e.rune_id.clone(), e.rune_amount.clone());
                let x1 = inputs_balance_map.entry(e.rune_id.clone()).or_insert(0);
                *x1 += e.rune_amount.parse::<u128>().unwrap();
            }
            inputs.insert(k.vout, balance_map);
        }
    }

    let mut burned = HashMap::new();
    let mut minted = HashMap::new();
    let mut premine = HashMap::new();
    for rune_id in rune_ids.iter() {
        let input = inputs_balance_map.get(rune_id).unwrap_or(&0);
        let output = outputs_balance_map.get(rune_id).unwrap_or(&0);
        match input.cmp(output) {
            Ordering::Less => {
                match &etching_rune_entry {
                    None => {
                        actions.insert("mint".into());
                        minted.insert(rune_id.clone(), (output - input).to_string());
                    }
                    Some(v) => {
                        if v.rune_id == *rune_id {
                            actions.insert("premine".into());
                            premine.insert(rune_id.clone(), (output - input).to_string());
                        } else {
                            actions.insert("mint".into());
                            minted.insert(rune_id.clone(), (output - input).to_string());
                        }
                    }
                }
            }
            Ordering::Greater => {
                burned.insert(rune_id.clone(), (input - output).to_string());
                actions.insert("burn".into());
            }
            _ => {}
        }
    }

    if etching_rune_entry.is_some() {
        actions.insert("etching".into());
    }


    let runes = db.sqlite_rune_entry_list_by_ids(&rune_ids)?.into_iter().map(|x| x.into()).collect();

    let tx = RuneTx {
        runes,
        actions: actions.into_iter().collect(),
        inputs,
        outputs,
        burned,
        minted,
        premine,
    };

    let r = R::with_data(tx);
    let value = serde_json::to_value(r)?;
    let mut cloned = value.clone();
    cloned["cache"] = Value::Bool(true);
    cache.insert(cache_key, cloned).await;
    Ok(Json(Some(value)))
}

pub async fn address_runes_utxos(
    Extension(cache): Extension<Arc<MokaCache>>,
    Extension(db): Extension<Arc<RunesDB>>,
    Path(address_string): Path<String>,
) -> anyhow::Result<Json<Value>, AppError> {
    let cache_key = CacheKey::new(CacheMethod::HandlerAddressUtxos, Value::String(address_string.clone()));
    if let Some(value) = cache.get(&cache_key).await {
        info!("cache hit: {}", &address_string);
        return Ok(Json(value));
    }

    let unspent = db.sqlite_rune_balance_list_unspent_by_address(&address_string)?;
    let mut rune_ids = HashSet::new();
    let unspent_map = unspent.iter().into_group_map_by(|x| RuneBalanceGroupKey {
        txid: x.txid.clone(),
        vout: x.vout,
    });
    let mut utxos = vec![];
    for (k, v) in unspent_map.iter() {
        let mut balance_map = HashMap::new();
        for e in v {
            rune_ids.insert(e.rune_id.clone());
            balance_map.insert(e.rune_id.clone(), e.rune_amount.clone());
        }
        utxos.push(UTXOWithRuneValueDTO {
            txid: k.txid.clone(),
            vout: k.vout,
            value: v.first().unwrap().value,
            runes_value: balance_map,
        });
    }
    let runes = db.sqlite_rune_entry_list_by_ids(&rune_ids)?.into_iter().map(|x| x.into()).collect();
    let r = R::with_data(AddressRuneUTXOsDTO { utxos, runes });
    let value = serde_json::to_value(r)?;
    let mut cloned = value.clone();
    cloned["cache"] = Value::Bool(true);
    cache.insert(cache_key, cloned).await;
    info!("cache miss: {}", &address_string);
    Ok(Json(value))
}
