use std::collections::HashMap;

use bitcoin::{OutPoint, Txid};
use serde::{Deserialize, Serialize, Serializer};
use serde::ser::{SerializeMap, SerializeSeq};

use ordinals::{RuneId, SpacedRune};

use crate::entry::RuneEntry;
use crate::lot::Lot;

pub fn serialize_as_string<S, T>(value: &T, serializer: S) -> Result<S::Ok, S::Error>
where
    S: Serializer,
    T: ToString,
{
    serializer.serialize_str(&value.to_string())
}

pub fn serialize_optional_number_as_string<T, S>(
    option_value: &Option<T>,
    serializer: S,
) -> Result<S::Ok, S::Error>
where
    T: ToString,
    S: Serializer,
{
    match option_value {
        Some(value) => serializer.serialize_str(&value.to_string()),
        None => unreachable!(),
    }
}

fn serialize_runes_outputs_map<S>(
    value: &HashMap<OutPoint, HashMap<RuneId, u128>>,
    serializer: S,
) -> Result<S::Ok, S::Error>
where
    S: Serializer,
{
    let mut map = serializer.serialize_map(Some(value.len()))?;
    for (outpoint, runeid_map) in value {
        let mut inner_map = HashMap::new();
        for (runeid, u128_val) in runeid_map {
            inner_map.insert(runeid, u128_val.to_string());
        }
        map.serialize_entry(outpoint, &inner_map)?;
    }
    map.end()
}

fn serialize_runes_outputs_with_lot_map<S>(
    value: &HashMap<usize, HashMap<RuneId, Lot>>,
    serializer: S,
) -> Result<S::Ok, S::Error>
where
    S: Serializer,
{
    let mut map = serializer.serialize_map(Some(value.len()))?;
    for (v, runeid_map) in value {
        let mut inner_map = HashMap::new();
        for (runeid, u128_val) in runeid_map {
            inner_map.insert(runeid, u128_val.0.to_string());
        }
        map.serialize_entry(v, &inner_map)?;
    }
    map.end()
}

fn serialize_runes_inputs_map<S>(
    value: &HashMap<usize, HashMap<RuneId, u128>>,
    serializer: S,
) -> Result<S::Ok, S::Error>
where
    S: Serializer,
{
    let mut map = serializer.serialize_map(Some(value.len()))?;
    for (v, runeid_map) in value {
        let mut inner_map = HashMap::new();
        for (runeid, u128_val) in runeid_map {
            inner_map.insert(runeid, u128_val.to_string());
        }
        map.serialize_entry(v, &inner_map)?;
    }
    map.end()
}

fn serialize_runes_burned_map<S>(
    value: &HashMap<RuneId, Lot>,
    serializer: S,
) -> Result<S::Ok, S::Error>
where
    S: Serializer,
{
    let mut map = serializer.serialize_map(Some(value.len()))?;
    for (rune_id, lot) in value {
        map.serialize_entry(rune_id, &lot.0.to_string())?;
    }
    map.end()
}

fn serialize_runes_map<S>(
    value: &HashMap<RuneId, u128>,
    serializer: S,
) -> Result<S::Ok, S::Error>
where
    S: Serializer,
{
    let mut map = serializer.serialize_map(Some(value.len()))?;
    for (rune_id, v) in value {
        map.serialize_entry(rune_id, &v.to_string())?;
    }
    map.end()
}

fn serialize_vec_runes_balance_map<S>(
    value: &Vec<HashMap<RuneId, u128>>,
    serializer: S,
) -> Result<S::Ok, S::Error>
where
    S: Serializer,
{
    let mut seq = serializer.serialize_seq(Some(value.len()))?;
    for hashmap in value {
        let mut map = HashMap::new();
        for (k, v) in hashmap {
            map.insert(k, v.to_string());
        }
        seq.serialize_element(&map)?;
    }
    seq.end()
}

#[derive(Debug, Serialize)]
pub struct ExpandRuneEntry {
    #[serde(serialize_with = "serialize_as_string")]
    pub burned: u128,
    pub divisibility: u8,
    pub etching: Txid,
    #[serde(serialize_with = "serialize_as_string")]
    pub mints: u128,
    #[serde(serialize_with = "serialize_as_string")]
    pub number: u64,
    #[serde(serialize_with = "serialize_as_string")]
    pub premine: u128,
    pub rune_id: RuneId,
    pub spaced_rune: SpacedRune,
    pub symbol: char,
    #[serde(
        skip_serializing_if = "Option::is_none",
        serialize_with = "serialize_optional_number_as_string"
    )]
    pub mint_amount: Option<u128>,
    #[serde(
        skip_serializing_if = "Option::is_none",
        serialize_with = "serialize_optional_number_as_string"
    )]
    pub cap: Option<u128>,
    #[serde(
        skip_serializing_if = "Option::is_none",
        serialize_with = "serialize_optional_number_as_string"
    )]
    pub start_height: Option<u64>,
    #[serde(
        skip_serializing_if = "Option::is_none",
        serialize_with = "serialize_optional_number_as_string"
    )]
    pub end_height: Option<u64>,
    #[serde(
        skip_serializing_if = "Option::is_none",
        serialize_with = "serialize_optional_number_as_string"
    )]
    pub start_offset: Option<u64>,
    #[serde(
        skip_serializing_if = "Option::is_none",
        serialize_with = "serialize_optional_number_as_string"
    )]
    pub end_offset: Option<u64>,
    #[serde(serialize_with = "serialize_as_string")]
    pub timestamp: u64,
    pub turbo: bool,
    pub mintable: bool,
}

impl ExpandRuneEntry {
    pub fn load(rune_id: RuneId, entry: RuneEntry, block_height: u32) -> Self {
        let mintable = entry.mintable((block_height + 1).into()).is_ok();
        let terms = entry.terms.unwrap_or_default();
        ExpandRuneEntry {
            burned: entry.burned,
            divisibility: entry.divisibility,
            etching: entry.etching,
            mints: entry.mints,
            number: entry.number,
            premine: entry.premine,
            rune_id,
            spaced_rune: entry.spaced_rune,
            symbol: entry.symbol.unwrap_or('¤'),
            mint_amount: terms.amount,
            cap: terms.cap,
            start_height: terms.height.0,
            end_height: terms.height.1,
            start_offset: terms.offset.0,
            end_offset: terms.offset.1,
            timestamp: entry.timestamp,
            turbo: entry.turbo,
            mintable,
        }
    }
}

#[derive(Debug, Serialize)]
pub struct Paged<T> {
    pub next: bool,
    pub list: Vec<T>,
}

impl<T> Paged<T> {
    pub fn new(next: bool, list: Vec<T>) -> Self {
        Paged { next, list }
    }
}

#[derive(Debug, Serialize)]
pub struct R<T> {
    pub success: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub code: Option<i32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub message: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub response: Option<T>,
}

impl<T> R<T> {
    pub fn error(code: i32, msg: String) -> Self {
        R {
            success: false,
            code: Some(code),
            message: Some(msg),
            response: None,
        }
    }

    pub fn with_data(data: T) -> Self {
        R {
            success: true,
            code: None,
            message: None,
            response: Some(data),
        }
    }
}

#[derive(Debug, Deserialize)]
pub struct RunesPSBTParams {
    #[serde(rename = "psbtHex")]
    pub psbt_hex: Option<String>,
    #[serde(rename = "psbt_hex")]
    pub psbt_hex_1: Option<String>,
}

impl RunesPSBTParams {
    pub fn get_psbt_hex(&self) -> Option<&String> {
        self.psbt_hex.as_ref().or(self.psbt_hex_1.as_ref())
    }
}

#[derive(Debug, Deserialize)]
pub struct RunesTxParams {
    pub raw_tx: Option<String>,
    #[serde(rename = "rawTx")]
    pub raw_tx_1: Option<String>,
    #[serde(rename = "tx_hex")]
    pub raw_tx_2: Option<String>,
    #[serde(rename = "txHex")]
    pub raw_tx_3: Option<String>,
}

impl RunesTxParams {
    pub fn get_raw_tx(&self) -> Option<&String> {
        self.raw_tx
            .as_ref()
            .or(self.raw_tx_1.as_ref())
            .or(self.raw_tx_2.as_ref())
            .or(self.raw_tx_3.as_ref())
    }
}

#[derive(Debug, Serialize, Default)]
pub struct RunesTxDTO {
    pub runes: Vec<ExpandRuneEntry>,
    #[serde(serialize_with = "serialize_runes_inputs_map")]
    pub inputs: HashMap<usize, HashMap<RuneId, u128>>,
    #[serde(serialize_with = "serialize_runes_outputs_with_lot_map")]
    pub outputs: HashMap<usize, HashMap<RuneId, Lot>>,
    #[serde(serialize_with = "serialize_runes_burned_map")]
    pub burned: HashMap<RuneId, Lot>,
    pub actions: Vec<String>,
}

#[derive(Debug, Deserialize)]
pub struct RunesPageParams {
    pub cursor: Option<usize>,
    pub size: Option<usize>,
    pub keywords: Option<String>,
    pub sort: Option<String>,
}

#[derive(Debug, Serialize, Default)]
pub struct OutputsDTO {
    pub runes: Vec<ExpandRuneEntry>,
    #[serde(serialize_with = "serialize_vec_runes_balance_map")]
    pub outputs: Vec<HashMap<RuneId, u128>>,
}

#[derive(Debug, Serialize, Default)]
pub struct RunesOutputsDTO {
    pub runes: Vec<ExpandRuneEntry>,
    #[serde(serialize_with = "serialize_runes_outputs_map")]
    pub outputs: HashMap<OutPoint, HashMap<RuneId, u128>>,
}

#[derive(Debug, Serialize)]
pub struct UTXOWithRuneValueDTO {
    pub txid: Txid,
    pub vout: u32,
    pub value: u64,
    #[serde(serialize_with = "serialize_runes_map")]
    pub runes_value: HashMap<RuneId, u128>,
}

#[derive(Debug, Serialize)]
pub struct AddressRuneUTXOsDTO {
    pub utxos: Vec<UTXOWithRuneValueDTO>,
    pub runes: Vec<ExpandRuneEntry>,
}



