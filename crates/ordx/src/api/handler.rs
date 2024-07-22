use std::collections::{HashMap, HashSet};
use std::str::FromStr;
use std::sync::Arc;

use axum::{Extension, Json};
use axum::extract::{Path, Query};
use axum::response::IntoResponse;
use bitcoin::{Address, OutPoint, Transaction};
use bitcoin::psbt::Psbt;
use serde_json::{json, Value};

use ordinals::{Artifact, Edict, Rune, RuneId, Runestone, SpacedRune};

use crate::api::dto::{AddressRuneUTXOsDTO, AppError, ExpandRuneEntry, OutputsDTO, Paged, R, RunesPageParams, RunesPSBTParams, RunesTxDTO, RunesTxParams, UTXOWithRuneValueDTO};
use crate::api::util::hex_to_base64;
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

pub async fn block_height(
    Extension(db): Extension<Arc<RunesDB>>,
) -> anyhow::Result<Json<R<Value>>, AppError> {
    let indexed_height = db.latest_indexed_height();
    let latest_height = db.latest_height();
    let remaining_height = latest_height.unwrap_or_default() - indexed_height.unwrap_or_default();
    let db_size = fs_extra::dir::get_size(db.db.path())?;
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


pub async fn get_rune_by_id(
    Extension(db): Extension<Arc<RunesDB>>,
    Path(id): Path<String>,
) -> impl IntoResponse {
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
    match rune_id {
        None => Json(R::with_data(None)),
        Some(id) => {
            match db.rune_id_to_rune_entry_get(&id) {
                None => Json(R::with_data(None)),
                Some(v) => {
                    let latest_height = db.latest_height().unwrap_or_default();
                    Json(R::with_data(Some(ExpandRuneEntry::load(id, v, latest_height))))
                }
            }
        }
    }
}


pub async fn paged_runes(
    Extension(db): Extension<Arc<RunesDB>>,
    Query(params): Query<RunesPageParams>,
) -> impl IntoResponse {
    let (next, list) = db.rune_entry_paged(
        params.cursor.unwrap_or(0).max(0),
        params.size.unwrap_or(10).clamp(1, 1000),
        params.keywords,
        params.sort,
    );
    let latest_height = db.latest_height().unwrap_or_default();
    let runes = list.iter().map(|x| ExpandRuneEntry::load(x.0, x.1, latest_height)).collect::<Vec<_>>();
    Json(R::with_data(Paged::new(next, runes)))
}


fn decode_runes_tx(db: &RunesDB, tx: Transaction) -> anyhow::Result<RunesTxDTO> {
    let mut runes_set = HashSet::new();
    let mut inputs = HashMap::new();
    let mut unallocated: HashMap<RuneId, Lot> = HashMap::new();
    let mut allocated: Vec<HashMap<RuneId, Lot>> = vec![HashMap::new(); tx.output.len()];
    for (index, vin) in tx.input.iter().enumerate() {
        let point = vin.previous_output;
        if let Some(v) = db.outpoint_to_rune_balances_get(&point) {
            let balances_buffer = v.4;
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
        actions.insert("burned".to_string());
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
            let balances_buffer = v.4;
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

pub async fn address_runes(
    Extension(db): Extension<Arc<RunesDB>>,
    Path(address): Path<String>,
) -> anyhow::Result<Json<R<AddressRuneUTXOsDTO>>, AppError> {
    let address = Address::from_str(&address)?.assume_checked();
    let spk = address.script_pubkey();
    let entries = db.spk_to_rune_balance_entries(&spk);
    let mut runes_set = HashSet::new();
    let mut utxos = vec![];
    for (outpoint, entry) in entries {
        let balances_buffer = entry.4;
        let mut i = 0;
        let mut balance_map = HashMap::new();
        while i < balances_buffer.len() {
            let ((id, balance), length) = RuneUpdater::decode_rune_balance(&balances_buffer[i..])?;
            i += length;
            balance_map.insert(id, balance);
            runes_set.insert(id);
        }
        utxos.push(UTXOWithRuneValueDTO {
            txid: outpoint.txid,
            vout: outpoint.vout,
            value: entry.2,
            runes_value: balance_map,
        });
    }
    let latest_height = db.latest_height().unwrap_or_default();
    let mut runes = vec![];
    for x in runes_set {
        let r = db.rune_id_to_rune_entry_get(&x).unwrap();
        runes.push(ExpandRuneEntry::load(x, r, latest_height));
    }
    Ok(Json(R::with_data(AddressRuneUTXOsDTO { utxos, runes })))
}
