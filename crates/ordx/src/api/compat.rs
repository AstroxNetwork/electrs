use std::str::FromStr;
use std::sync::Arc;

use axum::{Extension, Json};
use axum::extract::Path;
use bitcoin::Txid;
use serde::{Deserialize, Serialize};
use serde_json::Value;

use ordinals::{RuneId, SpacedRune};

use crate::api::dto::{AppError, serialize_as_string};
use crate::cache::{CacheKey, CacheMethod, MokaCache};
use crate::db::RunesDB;

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

#[derive(Debug, Serialize, Deserialize)]
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
    let cache_key = CacheKey::new(CacheMethod::CompatPagedRunes, serde_json::to_value(params).unwrap());
    if let Some(cached) = cache.get(&cache_key).await {
        return Ok(Json(cached));
    }
    
    // db.sqlite_rune_entry_list_for_compat(&params)?;

    Ok(Json(Value::Null))
}


pub async fn address_runes(
    Extension(cache): Extension<Arc<MokaCache>>,
    Extension(db): Extension<Arc<RunesDB>>,
    Path(address_string): Path<String>,
) -> anyhow::Result<Json<Value>, AppError> {
    let cache_key = CacheKey::new(CacheMethod::CompatAddressUtxos, Value::String(address_string.clone()));
    if let Some(cached) = cache.get(&cache_key).await {
        return Ok(Json(cached));
    }

    let unspent = db.sqlite_rune_balance_list_unspent_by_address(&address_string)?;
    let mut items: Vec<RuneValue> = vec![];
    for x in unspent.iter() {
        let rune_id = RuneId::from_str(&x.rune_id).unwrap();
        let rune_entry = db.rune_id_to_rune_entry_get(&rune_id).unwrap();
        items.push(RuneValue {
            amount: x.rune_amount.parse().unwrap(),
            rune_id,
            utxo: UTXO {
                tx_hash: Txid::from_str(&x.txid).unwrap(),
                vout: x.vout,
                value: x.value,
            },
            rune: RuneItem {
                rune_id,
                deploy_transaction: rune_entry.etching,
                divisibility: rune_entry.divisibility,
                end_block: rune_entry.block as _,
                rune: rune_entry.spaced_rune,
                symbol: rune_entry.symbol.unwrap_or('¤'),
                timestamp: rune_entry.timestamp,
            },
        });
    }
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
    Ok(Json(value))
}
