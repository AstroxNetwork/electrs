use std::collections::{HashMap, HashSet};

use serde::{Deserialize, Serialize};

use ordinals::RuneId;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RuneEntryForQueryInsert {
    pub rune_id: String,
    pub etching: String,
    pub number: u64,
    pub rune: String,
    pub spaced_rune: String,
    pub symbol: Option<String>,
    pub divisibility: u8,
    pub premine: String,
    pub amount: Option<String>,
    pub cap: Option<String>,
    pub start_height: Option<u32>,
    pub end_height: Option<u32>,
    pub start_offset: Option<u32>,
    pub end_offset: Option<u32>,
    pub mints: String,
    pub turbo: bool,
    pub burned: String,
    pub mintable: bool,
    pub fairmint: bool,
    pub holders: u32,
    pub transactions: u32,
    pub height: u32,
    pub ts: u32,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RuneEntryForUpdate {
    pub rune_id: String,
    pub mints: String,
    pub burned: String,
    pub mintable: bool,
}


#[derive(Debug, Clone, Hash, Eq, PartialEq)]
pub enum RuneOpType {
    Premine,
    Mint,
    Burn,
    Cenotaph,
    Transfer,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RuneBalanceForQuery {
    pub id: u32,
    pub txid: String,
    pub vout: u32,
    pub value: u64,
    pub rune_id: String,
    pub rune_amount: String,
    pub address: String,
    pub premine: bool,
    pub mint: bool,
    pub burn: bool,
    pub cenotaph: bool,
    pub transfer: bool,
    pub height: u32,
    pub idx: u32,
    pub ts: u32,
    pub spent_height: u32,
    pub spent_txid: Option<String>,
    pub spent_vin: Option<u32>,
    pub spent_ts: Option<u32>,
}

impl RuneBalanceForQuery {
    pub fn with_actions(&self, actions: &mut HashSet<String>) {
        if self.premine {
            actions.insert("premine".into());
        }
        if self.mint {
            actions.insert("mint".into());
        }
        if self.burn {
            actions.insert("burned".into());
        }
        if self.cenotaph {
            actions.insert("cenotaph".into());
        }
        if self.transfer {
            actions.insert("transfer".into());
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RuneBalanceForInsert {
    pub txid: String,
    pub vout: u32,
    pub value: u64,
    pub rune_id: String,
    pub rune_amount: String,
    pub address: String,
    pub premine: bool,
    pub mint: bool,
    pub burn: bool,
    pub cenotaph: bool,
    pub transfer: bool,
    pub height: u32,
    pub idx: u32,
    pub ts: u32,
    pub spent_height: u32,
    pub spent_txid: Option<String>,
    pub spent_vin: Option<u32>,
    pub spent_ts: Option<u32>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RuneBalanceForUpdate {
    pub txid: String,
    pub vout: u32,
    pub rune_id: String,
    pub spent_height: u32,
    pub spent_txid: String,
    pub spent_vin: u32,
    pub spent_ts: u32,
}

pub struct RuneEntryCompatPageParams{
    pub offset: u64,
    pub limit: u64,
    pub mint_type: Option<String>,
    pub search: Option<String>,
    pub sort: Option<String>,
}


#[derive(Debug, Clone, Default)]
pub struct RuneEntryForTemp {
    pub inserts: HashMap<RuneId, RuneEntryForQueryInsert>,
    pub updates: HashMap<RuneId, RuneEntryForUpdate>,
}

impl RuneEntryForTemp {
    pub fn insert(&mut self, key: &RuneId, insert: RuneEntryForQueryInsert) {
        self.inserts.insert(*key, insert);
    }

    pub fn try_update(&mut self, key: RuneId, update: RuneEntryForUpdate) {
        if self.inserts.contains_key(&key) {
            let mut x = self.inserts.get(&key).unwrap().clone();
            x.mints = update.mints;
            x.burned = update.burned;
            x.mintable = update.mintable;
            self.inserts.insert(key, x);
        } else {
            self.updates.insert(key, update);
        }
    }
}

#[derive(Debug, Clone, Default, Hash, Eq, PartialEq)]
pub struct RuneBalanceKey {
    pub txid: String,
    pub vout: u32,
    pub rune_id: String,
}

#[derive(Debug, Clone, Default)]
pub struct RuneBalanceForTemp {
    pub inserts: HashMap<RuneBalanceKey, RuneBalanceForInsert>,
    pub updates: HashMap<RuneBalanceKey, RuneBalanceForUpdate>,
    pub tx_ops: HashMap<String, HashSet<RuneOpType>>,
}

impl RuneBalanceForTemp {
    pub fn insert(&mut self, key: RuneBalanceKey, insert: RuneBalanceForInsert) {
        self.inserts.insert(key, insert);
    }

    pub fn try_update(&mut self, key: &RuneBalanceKey, update: RuneBalanceForUpdate) {
        if self.inserts.contains_key(key) {
            let mut x = self.inserts.get(key).unwrap().clone();
            x.spent_vin = Some(update.spent_vin);
            x.spent_txid = Some(update.spent_txid);
            x.spent_height = update.spent_height;
            x.spent_ts = Some(update.spent_ts);
            self.inserts.insert(key.clone(), x);
        } else {
            self.updates.insert(key.clone(), update);
        }
    }

    pub fn insert_tx_op(&mut self, txid: String, op: RuneOpType) {
        self.tx_ops.entry(txid).or_insert_with(HashSet::new).insert(op);
    }

    pub fn update_inserts(&mut self) {
        for (_, insert) in self.inserts.iter_mut() {
            if let Some(ops) = self.tx_ops.get(&insert.txid) {
                if ops.contains(&RuneOpType::Burn) {
                    insert.burn = true;
                }
                if ops.contains(&RuneOpType::Mint) {
                    insert.mint = true;
                }
                if ops.contains(&RuneOpType::Transfer) {
                    insert.transfer = true;
                }
                if ops.contains(&RuneOpType::Cenotaph) {
                    insert.cenotaph = true;
                }
                if ops.contains(&RuneOpType::Premine) {
                    insert.premine = true;
                }
            }
        }
    }
}