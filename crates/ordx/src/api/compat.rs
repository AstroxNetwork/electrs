use std::collections::hash_map::Entry::Vacant;
use std::collections::HashMap;
use std::str::FromStr;
use std::sync::Arc;

use axum::{Extension, Json};
use axum::extract::Path;
use bitcoin::{Address, Txid};
use log::info;
use serde::{Deserialize, Serialize};
use serde_json::Value;

use ordinals::{RuneId, SpacedRune};

use crate::api::dto::{AppError, serialize_as_string};
use crate::cache::{CacheKey, CacheMethod, MokaCache};
use crate::db::RunesDB;
use crate::entry::RuneEntry;
use crate::updater::RuneUpdater;

#[derive(Debug, Serialize)]
pub struct R<T> {
    pub status: bool,
    pub status_code: i64,
    pub message: String,
    pub data: T,
}

#[derive(Debug, Serialize)]
pub struct RuneValue {
    #[serde(
        serialize_with = "serialize_as_string"
    )]
    pub amount: u128,
    pub rune_id: RuneId,
    pub utxo: UTXO,
    pub rune: RuneItem,
}

#[derive(Debug, Serialize)]
pub struct UTXO {
    pub tx_hash: Txid,
    #[serde(
        serialize_with = "serialize_as_string"
    )]
    pub vout: u32,
    #[serde(
        serialize_with = "serialize_as_string"
    )]
    pub value: u64,
}

#[derive(Debug, Serialize)]
pub struct RuneItem {
    pub rune_id: RuneId,
    pub deploy_transaction: Txid,
    pub divisibility: u8,
    #[serde(
        serialize_with = "serialize_as_string"
    )]
    pub end_block: u32,
    pub rune: SpacedRune,
    pub symbol: char,
    pub timestamp: u64,
}

#[derive(Debug, Deserialize)]
pub struct PagedRunesParams {
    pub offset: u64,
    pub limit: u64,
    #[serde(rename = "type")]
    pub mint_type: Option<String>,
    pub search: Option<String>,
    pub sort: Option<String>,
}

pub async fn paged_runes(
    Extension(cache): Extension<Arc<MokaCache>>,
    Extension(db): Extension<Arc<RunesDB>>,
    Path(params): Path<PagedRunesParams>,
) -> anyhow::Result<Json<Value>, AppError> {
    Ok(Json(Value::Null))
}


pub async fn address_runes(
    Extension(cache): Extension<Arc<MokaCache>>,
    Extension(db): Extension<Arc<RunesDB>>,
    Path(address_string): Path<String>,
) -> anyhow::Result<Json<Value>, AppError> {
    let cache_key = CacheKey::new(CacheMethod::CompatAddressUtxos, Value::String(address_string.clone()));
    if let Some(cached) = cache.get(&cache_key).await {
        info!("cache hit: {}", &address_string);
        return Ok(Json(cached));
    }

    let address = Address::from_str(&address_string)?.assume_checked();
    let spk = address.script_pubkey();
    // let entries = db.spk_to_rune_balance_entries(&spk);
    let mut runes_map: HashMap<RuneId, RuneEntry> = HashMap::new();
    let mut items:Vec<String> = vec![];
    // for (outpoint, entry) in entries {
    //     let balances_buffer = entry.4;
    //     let mut i = 0;
    //     while i < balances_buffer.len() {
    //         let ((id, balance), length) = RuneUpdater::decode_rune_balance(&balances_buffer[i..])?;
    //         i += length;
    //         let rune_entry: RuneEntry = {
    //             if let Vacant(e) = runes_map.entry(id) {
    //                 let rune_entry = db.rune_id_to_rune_entry_get(&id).unwrap();
    //                 e.insert(rune_entry);
    //                 rune_entry
    //             } else {
    //                 *runes_map.get(&id).unwrap()
    //             }
    //         };
    //         items.push(RuneValue {
    //             amount: balance,
    //             rune_id: id,
    //             utxo: UTXO {
    //                 tx_hash: outpoint.txid,
    //                 vout: outpoint.vout,
    //                 value: entry.2,
    //             },
    //             rune: RuneItem {
    //                 rune_id: id,
    //                 deploy_transaction: rune_entry.etching,
    //                 divisibility: rune_entry.divisibility,
    //                 end_block: rune_entry.block as _,
    //                 rune: rune_entry.spaced_rune,
    //                 symbol: rune_entry.symbol.unwrap_or('¤'),
    //                 timestamp: rune_entry.timestamp,
    //             },
    //         });
    //     }
    // }
    let r = R {
        status: true,
        status_code: 200,
        message: "success".to_string(),
        data: items,
    };
    let value = serde_json::to_value(&r)?;
    let mut cloned = value.clone();
    cloned["cache"] = Value::Bool(true);
    cache.insert(cache_key, cloned).await;
    info!("cache miss: {}", &address_string);
    Ok(Json(value))
}
