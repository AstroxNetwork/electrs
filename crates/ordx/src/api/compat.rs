use std::collections::hash_map::Entry::Vacant;
use std::collections::HashMap;
use std::str::FromStr;
use std::sync::Arc;

use axum::{Extension, Json};
use axum::extract::Path;
use axum::response::IntoResponse;
use bitcoin::{Address, Txid};
use serde_derive::Serialize;

use ordinals::{RuneId, SpacedRune};

use crate::api::dto::serialize_as_string;
use crate::db::RunesDB;
use crate::entry::RuneEntry;
use crate::updater::RuneUpdater;

#[derive(Debug, Serialize)]
struct R<T> {
    pub status: bool,
    pub status_code: i64,
    pub message: String,
    pub data: Vec<T>,
}

#[derive(Debug, Serialize)]
struct RuneValue {
    pub address: String,
    #[serde(
        serialize_with = "serialize_as_string"
    )]
    pub amount: u128,
    pub rune_id: RuneId,
    pub utxo: UTXO,
    pub rune: RuneItem,
}

#[derive(Debug, Serialize)]
struct UTXO {
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
struct RuneItem {
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


pub async fn address_runes(
    Extension(db): Extension<Arc<RunesDB>>,
    Path(address_string): Path<String>,
) -> impl IntoResponse {
    let address = Address::from_str(&address_string).unwrap().assume_checked();
    let spk = address.script_pubkey();
    let entries = db.spk_to_rune_balance_entries(&spk);
    let mut runes_map: HashMap<RuneId, RuneEntry> = HashMap::new();
    let mut items = vec![];
    for (outpoint, entry) in entries {
        let balances_buffer = entry.4;
        let mut i = 0;
        while i < balances_buffer.len() {
            let ((id, balance), length) = RuneUpdater::decode_rune_balance(&balances_buffer[i..]).unwrap();
            i += length;
            let rune_entry: RuneEntry = {
                if let Vacant(e) = runes_map.entry(id) {
                    let rune_entry = db.rune_id_to_rune_entry_get(&id).unwrap();
                    e.insert(rune_entry);
                    rune_entry
                } else {
                    *runes_map.get(&id).unwrap()
                }
            };
            items.push(RuneValue {
                address: address_string.clone(),
                amount: balance,
                rune_id: id,
                utxo: UTXO {
                    tx_hash: outpoint.txid,
                    vout: outpoint.vout,
                    value: entry.2,
                },
                rune: RuneItem {
                    rune_id: id,
                    deploy_transaction: rune_entry.etching,
                    divisibility: rune_entry.divisibility,
                    end_block: rune_entry.block as _,
                    rune: rune_entry.spaced_rune,
                    symbol: rune_entry.symbol.unwrap_or('¤'),
                    timestamp: rune_entry.timestamp,
                },
            });
        }
    }
    Json(R {
        status: true,
        status_code: 200,
        message: "success".to_string(),
        data: items,
    })
}
