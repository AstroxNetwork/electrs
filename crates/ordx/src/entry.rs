use std::io::Cursor;

use anyhow::Error;
use bitcoin::{consensus, OutPoint, Txid};
use bitcoin::block::Header;
use bitcoin::consensus::{Decodable, Encodable};
use bitcoin::hashes::Hash;
use serde_derive::{Deserialize, Serialize};

use ordinals::*;

use crate::bincode;

#[derive(Clone, Debug)]
pub struct RuneTxFlow {
    pub txid: String,
    pub vin: Option<u32>,
    pub vout: Option<u32>,
    pub rune_id: String,
    pub rune_amount: u128,
    pub operation: u8,
}

#[derive(Debug, PartialEq)]
pub enum OperationType {
    Premine = 1,
    Mint = 2,
    Burn = 3,
    Cenotaph = 4,
    Send = 5,
    Receive = 6,
}

impl OperationType {
    pub fn value(&self) -> u8 {
        match self {
            OperationType::Premine => 1,
            OperationType::Mint => 2,
            OperationType::Burn => 3,
            OperationType::Cenotaph => 4,
            OperationType::Send => 5,
            OperationType::Receive => 6,
        }
    }

    pub fn to_string(&self) -> &str {
        match self {
            OperationType::Premine => "premine",
            OperationType::Mint => "mint",
            OperationType::Burn => "burn",
            OperationType::Cenotaph => "cenotaph",
            OperationType::Send => "send",
            OperationType::Receive => "receive",
        }
    }
}
#[derive(Debug, PartialEq, Eq, Hash)]
pub struct TxFlowOutputKey {
    pub txid: String,
    pub vin: Option<u32>,
    pub vout: Option<u32>,
    pub operation: u8,
    pub rune_id: String,
}

impl From<&RuneTxFlow> for TxFlowOutputKey {
    fn from(value: &RuneTxFlow) -> Self {
        TxFlowOutputKey {
            txid: value.txid.clone(),
            vout: value.vout,
            vin: value.vin,
            operation: value.operation,
            rune_id: value.rune_id.clone(),
        }
    }
}


pub trait Entry: Sized {
    type Value;

    fn load(value: Self::Value) -> Self;

    fn store(self) -> Self::Value;
}

pub trait EntryBytes: Entry {
    fn load_bytes(bytes: &[u8]) -> Self;
    fn store_bytes(self) -> Vec<u8>;
}

pub type HeaderValue = [u8; 80];

impl Entry for Header {
    type Value = HeaderValue;

    fn load(value: Self::Value) -> Self {
        consensus::encode::deserialize(&value).unwrap()
    }

    fn store(self) -> Self::Value {
        let mut buffer = Cursor::new([0; 80]);
        let len = self
            .consensus_encode(&mut buffer)
            .expect("in-memory writers don't error");
        let buffer = buffer.into_inner();
        debug_assert_eq!(len, buffer.len());
        buffer
    }
}

impl EntryBytes for Header {
    fn load_bytes(bytes: &[u8]) -> Self {
        Self::load(bytes.try_into().unwrap())
    }

    fn store_bytes(self) -> Vec<u8> {
        self.store().to_vec()
    }
}


impl Entry for Rune {
    type Value = u128;

    fn load(value: Self::Value) -> Self {
        Self(value)
    }

    fn store(self) -> Self::Value {
        self.0
    }
}

impl EntryBytes for Rune {
    fn load_bytes(bytes: &[u8]) -> Self {
        Self::load(u128::from_be_bytes(bytes.try_into().unwrap()))
    }

    fn store_bytes(self) -> Vec<u8> {
        self.0.to_be_bytes().to_vec()
    }
}


#[derive(Debug, PartialEq)]
pub enum MintError {
    Cap(u128),
    End(u64),
    Start(u64),
    Unmintable,
}

#[derive(Debug, PartialEq, Copy, Clone, Serialize, Deserialize)]
pub struct RuneEntry {
    pub block: u64,
    pub burned: u128,
    pub divisibility: u8,
    pub etching: Txid,
    pub mints: u128,
    pub number: u64,
    pub premine: u128,
    pub spaced_rune: SpacedRune,
    pub symbol: Option<char>,
    pub terms: Option<Terms>,
    pub timestamp: u64,
    pub turbo: bool,
}


impl RuneEntry {
    pub fn mintable(&self, height: u64) -> Result<u128, MintError> {
        let Some(terms) = self.terms else {
            return Err(MintError::Unmintable);
        };

        if let Some(start) = self.start() {
            if height < start {
                return Err(MintError::Start(start));
            }
        }

        if let Some(end) = self.end() {
            if height >= end {
                return Err(MintError::End(end));
            }
        }

        let cap = terms.cap.unwrap_or_default();

        if self.mints >= cap {
            return Err(MintError::Cap(cap));
        }

        Ok(terms.amount.unwrap_or_default())
    }

    pub fn supply(&self) -> u128 {
        self.premine
            + self.mints
            * self
            .terms
            .and_then(|terms| terms.amount)
            .unwrap_or_default()
    }

    pub fn pile(&self, amount: u128) -> Pile {
        Pile {
            amount,
            divisibility: self.divisibility,
            symbol: self.symbol,
        }
    }

    pub fn start(&self) -> Option<u64> {
        let terms = self.terms?;

        let relative = terms
            .offset
            .0
            .map(|offset| self.block.saturating_add(offset));

        let absolute = terms.height.0;

        relative
            .zip(absolute)
            .map(|(relative, absolute)| relative.max(absolute))
            .or(relative)
            .or(absolute)
    }

    pub fn end(&self) -> Option<u64> {
        let terms = self.terms?;

        let relative = terms
            .offset
            .1
            .map(|offset| self.block.saturating_add(offset));

        let absolute = terms.height.1;

        relative
            .zip(absolute)
            .map(|(relative, absolute)| relative.min(absolute))
            .or(relative)
            .or(absolute)
    }
}

pub type TermsEntryValue = (
    Option<u128>,               // cap
    (Option<u64>, Option<u64>), // height
    Option<u128>,               // amount
    (Option<u64>, Option<u64>), // offset
);

pub type RuneEntryValue = (
    u64,                     // block
    u128,                    // burned
    u8,                      // divisibility
    (u128, u128),            // etching
    u128,                    // mints
    u64,                     // number
    u128,                    // premine
    (u128, u32),             // spaced rune
    Option<char>,            // symbol
    Option<TermsEntryValue>, // terms
    u64,                     // timestamp
    bool,                    // turbo
);

impl Default for RuneEntry {
    fn default() -> Self {
        Self {
            block: 0,
            burned: 0,
            divisibility: 0,
            etching: Txid::all_zeros(),
            mints: 0,
            number: 0,
            premine: 0,
            spaced_rune: SpacedRune::default(),
            symbol: None,
            terms: None,
            timestamp: 0,
            turbo: false,
        }
    }
}

impl Entry for RuneEntry {
    type Value = RuneEntryValue;

    fn load(
        (
            block,
            burned,
            divisibility,
            etching,
            mints,
            number,
            premine,
            (rune, spacers),
            symbol,
            terms,
            timestamp,
            turbo,
        ): RuneEntryValue,
    ) -> Self {
        Self {
            block,
            burned,
            divisibility,
            etching: {
                let low = etching.0.to_le_bytes();
                let high = etching.1.to_le_bytes();
                Txid::from_byte_array([
                    low[0], low[1], low[2], low[3], low[4], low[5], low[6], low[7], low[8], low[9], low[10],
                    low[11], low[12], low[13], low[14], low[15], high[0], high[1], high[2], high[3], high[4],
                    high[5], high[6], high[7], high[8], high[9], high[10], high[11], high[12], high[13],
                    high[14], high[15],
                ])
            },
            mints,
            number,
            premine,
            spaced_rune: SpacedRune {
                rune: Rune(rune),
                spacers,
            },
            symbol,
            terms: terms.map(|(cap, height, amount, offset)| Terms {
                cap,
                height,
                amount,
                offset,
            }),
            timestamp,
            turbo,
        }
    }

    fn store(self) -> Self::Value {
        (
            self.block,
            self.burned,
            self.divisibility,
            {
                let bytes = self.etching.to_byte_array();
                (
                    u128::from_le_bytes([
                        bytes[0], bytes[1], bytes[2], bytes[3], bytes[4], bytes[5], bytes[6], bytes[7],
                        bytes[8], bytes[9], bytes[10], bytes[11], bytes[12], bytes[13], bytes[14], bytes[15],
                    ]),
                    u128::from_le_bytes([
                        bytes[16], bytes[17], bytes[18], bytes[19], bytes[20], bytes[21], bytes[22], bytes[23],
                        bytes[24], bytes[25], bytes[26], bytes[27], bytes[28], bytes[29], bytes[30], bytes[31],
                    ]),
                )
            },
            self.mints,
            self.number,
            self.premine,
            (self.spaced_rune.rune.0, self.spaced_rune.spacers),
            self.symbol,
            self.terms.map(
                |Terms {
                     cap,
                     height,
                     amount,
                     offset,
                 }| (cap, height, amount, offset),
            ),
            self.timestamp,
            self.turbo,
        )
    }
}

impl EntryBytes for RuneEntry {
    fn load_bytes(bytes: &[u8]) -> Self {
        Self::load(bincode::deserialize_little(bytes).unwrap())
    }

    fn store_bytes(self) -> Vec<u8> {
        bincode::serialize_little(&self.store()).unwrap()
    }
}

pub type RuneIdValue = (u64, u32);

impl Entry for RuneId {
    type Value = RuneIdValue;

    fn load((block, tx): Self::Value) -> Self {
        Self { block, tx }
    }

    fn store(self) -> Self::Value {
        (self.block, self.tx)
    }
}

impl EntryBytes for RuneId {
    fn load_bytes(bytes: &[u8]) -> Self {
        let block = u64::from_be_bytes(bytes[0..8].try_into().unwrap());
        let tx = u32::from_be_bytes(bytes[8..12].try_into().unwrap());
        Self::load((block, tx))
    }

    fn store_bytes(self) -> Vec<u8> {
        let mut combined: [u8; 12] = [0; 12];
        let block_bytes: [u8; 8] = self.block.to_be_bytes();
        let tx_bytes: [u8; 4] = self.tx.to_be_bytes();
        combined[..8].copy_from_slice(&block_bytes);
        combined[8..].copy_from_slice(&tx_bytes);
        combined.to_vec()
    }
}

pub type OutPointValue = [u8; 36];

impl Entry for OutPoint {
    type Value = OutPointValue;

    fn load(value: Self::Value) -> Self {
        Decodable::consensus_decode(&mut Cursor::new(value)).unwrap()
    }

    fn store(self) -> Self::Value {
        let mut value = [0; 36];
        self.consensus_encode(&mut value.as_mut_slice()).unwrap();
        value
    }
}


#[derive(Debug, PartialEq, Copy, Clone, Serialize, Deserialize)]
pub enum OutpointStatus {
    Spent = 0,
    Unspent = 1,
}

// (confirmed_height, spent_height, sat, spk, rune_balance)
pub type RuneBalanceEntry = (u32, u32, u64, Vec<u8>, Vec<u8>);

impl Entry for RuneBalanceEntry {
    type Value = (u32, u32, u64, Vec<u8>, Vec<u8>);
    fn load((confirmed_height, spent_height, sat, spk, rune_balance): Self::Value) -> Self {
        (
            confirmed_height,
            spent_height,
            sat,
            spk,
            rune_balance,
        )
    }
    fn store(self) -> Self::Value {
        (
            self.0,
            self.1,
            self.2,
            self.3,
            self.4,
        )
    }
}

impl EntryBytes for RuneBalanceEntry {
    fn load_bytes(bytes: &[u8]) -> Self {
        Self::load(bincode::deserialize_little(bytes).unwrap())
    }

    fn store_bytes(self) -> Vec<u8> {
        bincode::serialize_little(&self.store()).unwrap()
    }
}

pub type SatPointValue = [u8; 44];

impl Entry for SatPoint {
    type Value = SatPointValue;

    fn load(value: Self::Value) -> Self {
        Decodable::consensus_decode(&mut Cursor::new(value)).unwrap()
    }

    fn store(self) -> Self::Value {
        let mut value = [0; 44];
        self.consensus_encode(&mut value.as_mut_slice()).unwrap();
        value
    }
}

pub type SatRange = (u64, u64);

impl Entry for SatRange {
    type Value = [u8; 11];

    fn load([b0, b1, b2, b3, b4, b5, b6, b7, b8, b9, b10]: Self::Value) -> Self {
        let raw_base = u64::from_le_bytes([b0, b1, b2, b3, b4, b5, b6, 0]);

        // 51 bit base
        let base = raw_base & ((1 << 51) - 1);

        let raw_delta = u64::from_le_bytes([b6, b7, b8, b9, b10, 0, 0, 0]);

        // 33 bit delta
        let delta = raw_delta >> 3;

        (base, base + delta)
    }

    fn store(self) -> Self::Value {
        let base = self.0;
        let delta = self.1 - self.0;
        let n = u128::from(base) | u128::from(delta) << 51;
        n.to_le_bytes()[0..11].try_into().unwrap()
    }
}

pub type TxidValue = [u8; 32];

impl Entry for Txid {
    type Value = TxidValue;

    fn load(value: Self::Value) -> Self {
        Txid::from_byte_array(value)
    }

    fn store(self) -> Self::Value {
        Txid::to_byte_array(self)
    }
}

impl EntryBytes for Txid {
    fn load_bytes(bytes: &[u8]) -> Self {
        Self::load(bytes.try_into().unwrap())
    }

    fn store_bytes(self) -> Vec<u8> {
        self.to_byte_array().to_vec()
    }
}


#[derive(Copy, Clone)]
pub enum Statistic {
    Schema = 0,
    BlessedInscriptions = 1,
    Commits = 2,
    CursedInscriptions = 3,
    IndexRunes = 4,
    IndexSats = 5,
    LostSats = 6,
    OutputsTraversed = 7,
    ReservedRunes = 8,
    Runes = 9,
    SatRanges = 10,
    UnboundInscriptions = 11,
    IndexTransactions = 12,
    IndexSpentSats = 13,
    InitialSyncTime = 14,
    LatestHeight = u8::MAX as _,
}

impl Statistic {
    pub fn key(self) -> u8 {
        self.into()
    }
}

impl From<Statistic> for u8 {
    fn from(statistic: Statistic) -> Self {
        statistic as u8
    }
}

pub type Result<T = (), E = Error> = std::result::Result<T, E>;

pub trait BitcoinCoreRpcResultExt<T> {
    fn into_option(self) -> Result<Option<T>>;
}

impl<T> BitcoinCoreRpcResultExt<T> for Result<T, bitcoincore_rpc::Error> {
    fn into_option(self) -> Result<Option<T>> {
        match self {
            Ok(ok) => Ok(Some(ok)),
            Err(bitcoincore_rpc::Error::JsonRpc(bitcoincore_rpc::jsonrpc::error::Error::Rpc(
                                                    bitcoincore_rpc::jsonrpc::error::RpcError { code: -8, .. },
                                                ))) => Ok(None),
            Err(bitcoincore_rpc::Error::JsonRpc(bitcoincore_rpc::jsonrpc::error::Error::Rpc(
                                                    bitcoincore_rpc::jsonrpc::error::RpcError { message, .. },
                                                )))
            if message.ends_with("not found") =>
                {
                    Ok(None)
                }
            Err(err) => Err(err.into()),
        }
    }
}

#[cfg(test)]
mod tests {
    use ordinals::RuneId;

    use crate::entry::EntryBytes;

    #[test]
    fn test_bincode() {
        let rune_id = RuneId::new(1, 2).unwrap();
        println!("{:?}", &rune_id.store_bytes());
        let x: [u8; 4] = 2u32.to_be_bytes();
        let x1: [u8; 8] = 1u64.to_be_bytes();
        let mut combined: [u8; 12] = [0; 12];
        combined[..8].copy_from_slice(&x1);
        combined[8..].copy_from_slice(&x);
        println!("{:?}", combined);
    }
}