use std::collections::HashSet;
use std::path::Path;

use bitcoin::block::Header;
use bitcoin::OutPoint;
use bitcoin::ScriptBuf;
use log::{error, info};
use rocksdb::{ColumnFamily, ColumnFamilyDescriptor, DB, Error, IteratorMode, Options, WriteBatch};

use ordinals::{Rune, RuneId};

use crate::entry::{Entry, EntryBytes, OutPointValue, RuneBalanceEntry, RuneEntry, Statistic};
use crate::updater::REORG_DEPTH;

pub struct RunesDB {
    db: DB,
}

pub const HEIGHT_TO_BLOCK_HEADER: &str = "HEIGHT_TO_BLOCK_HEADER";
pub const HEIGHT_TO_STATISTIC_COUNT: &str = "HEIGHT_TO_STATISTIC_COUNT";
pub const STATISTIC_TO_VALUE: &str = "STATISTIC_TO_VALUE";
pub const OUTPOINT_TO_RUNE_BALANCES: &str = "OUTPOINT_TO_RUNE_BALANCES";
pub const SPK_OUTPOINT_TO_SPENT_HEIGHT: &str = "SPK_OUTPOINT_TO_SPENT_HEIGHT";
pub const RUNE_ID_TO_RUNE_ENTRY: &str = "RUNE_ID_TO_RUNE_ENTRY";
pub const RUNE_TO_RUNE_ID: &str = "RUNE_TO_RUNE_ID";

pub const RUNE_ID_HEIGHT_TO_MINTS: &str = "RUNE_ID_HEIGHT_TO_MINTS";
pub const RUNE_ID_HEIGHT_TO_BURNED: &str = "RUNE_ID_HEIGHT_TO_BURNED";

pub const RUNE_ID_TO_MINTS: &str = "RUNE_ID_TO_MINTS";
pub const RUNE_ID_TO_BURNED: &str = "RUNE_ID_TO_BURNED";


impl RunesDB {
    pub fn new<P: AsRef<Path>>(path: P) -> Self {
        let mut db_opts = Options::default();
        db_opts.create_if_missing(true);
        db_opts.create_missing_column_families(true);
        db_opts.set_compaction_style(rocksdb::DBCompactionStyle::Level);
        db_opts.set_compression_type(rocksdb::DBCompressionType::Zstd);

        let cf_names = [
            HEIGHT_TO_BLOCK_HEADER,
            HEIGHT_TO_STATISTIC_COUNT,
            STATISTIC_TO_VALUE,
            OUTPOINT_TO_RUNE_BALANCES,
            SPK_OUTPOINT_TO_SPENT_HEIGHT,
            RUNE_ID_TO_RUNE_ENTRY,
            RUNE_TO_RUNE_ID,
            RUNE_ID_HEIGHT_TO_MINTS,
            RUNE_ID_HEIGHT_TO_BURNED,
            RUNE_ID_TO_MINTS,
            RUNE_ID_TO_BURNED,
        ];
        let cf_descriptors: Vec<_> = cf_names.iter()
            .map(|name| ColumnFamilyDescriptor::new(*name, Options::default()))
            .collect();


        let db = DB::open_cf_descriptors(&db_opts, path, cf_descriptors).unwrap();

        RunesDB { db }
    }

    #[inline]
    pub fn get_cf(&self, cf_name: &str) -> &ColumnFamily {
        self.db.cf_handle(cf_name).unwrap_or_else(|| panic!("Column family {} not found", cf_name))
    }

    pub fn put(&self, cf_name: &str, key: &[u8], value: &[u8]) -> Result<(), Error> {
        let cf = self.get_cf(cf_name);
        self.db.put_cf(cf, key, value)
    }

    pub fn insert(&self, cf_name: &str, key: &[u8], value: &[u8]) -> Result<(), Error> {
        self.put(cf_name, key, value)
    }

    pub fn get(&self, cf_name: &str, key: &[u8]) -> Result<Option<Vec<u8>>, Error> {
        let cf = self.get_cf(cf_name);
        self.db.get_cf(cf, key)
    }

    pub fn del(&self, cf_name: &str, key: &[u8]) -> Result<(), Error> {
        let cf = self.get_cf(cf_name);
        self.db.delete_cf(cf, key)
    }

    pub fn remove(&self, cf_name: &str, key: &[u8]) -> Result<(), Error> {
        self.del(cf_name, key)
    }

    pub fn list(&self, cf_name: &str) -> Vec<(Vec<u8>, Vec<u8>)> {
        let cf = self.get_cf(cf_name);
        self.db.iterator_cf(cf, IteratorMode::Start)
            .map(|r| {
                let (k, v) = r.unwrap();
                (k.to_vec(), v.to_vec())
            })
            .collect()
    }

    pub fn write_batch(&self, batch: WriteBatch) -> Result<(), Error> {
        self.db.write(batch)
    }


    // specific methods
    pub fn statistic_to_value_put(&self, statistic: &Statistic, value: u32) {
        self.put(STATISTIC_TO_VALUE, &[statistic.key()], &value.to_be_bytes()).unwrap()
    }

    pub fn statistic_to_value_put_with_batch(&self, wtx: &mut WriteBatch, statistic: &Statistic, value: u32) {
        wtx.put_cf(self.get_cf(STATISTIC_TO_VALUE), [statistic.key()], value.to_be_bytes())
    }

    pub fn statistic_to_value_get(&self, statistic: &Statistic) -> Option<u32> {
        self.get(STATISTIC_TO_VALUE, &[statistic.key()])
            .map(|opt| opt.map(|bytes| u32::from_be_bytes(bytes.try_into().unwrap()))).unwrap()
    }

    pub fn statistic_to_value_inc(&self, statistic: &Statistic) {
        let current = self.statistic_to_value_get(statistic).unwrap_or_default() + 1;
        self.put(STATISTIC_TO_VALUE, &[statistic.key()], &current.to_be_bytes()).unwrap()
    }

    pub fn rune_id_to_mints_put(&self, key: &RuneId, value: u128) {
        self.put(RUNE_ID_TO_MINTS, &key.store_bytes(), &value.to_be_bytes()).unwrap()
    }

    pub fn rune_id_to_mints_get(&self, key: &RuneId) -> Option<u128> {
        self.get(RUNE_ID_TO_MINTS, &key.store_bytes())
            .map(|opt| opt.map(|bytes| u128::from_be_bytes(bytes.try_into().unwrap()))).unwrap()
    }

    pub fn rune_id_to_mints_inc(&self, key: &RuneId) -> u128 {
        let current = self.rune_id_to_mints_get(key).unwrap_or_default() + 1;
        self.put(RUNE_ID_TO_MINTS, &key.store_bytes(), &current.to_be_bytes()).unwrap();
        current
    }

    pub fn rune_id_to_burned_put(&self, key: &RuneId, value: u128) {
        self.put(RUNE_ID_TO_BURNED, &key.store_bytes(), &value.to_be_bytes()).unwrap()
    }

    pub fn rune_id_to_burned_get(&self, key: &RuneId) -> Option<u128> {
        self.get(RUNE_ID_TO_BURNED, &key.store_bytes())
            .map(|opt| opt.map(|bytes| u128::from_be_bytes(bytes.try_into().unwrap()))).unwrap()
    }

    pub fn rune_id_to_burned_inc(&self, key: &RuneId) -> u128 {
        let current = self.rune_id_to_burned_get(key).unwrap_or_default() + 1;
        self.put(RUNE_ID_TO_BURNED, &key.store_bytes(), &current.to_be_bytes()).unwrap();
        current
    }


    pub fn rune_id_height_to_mints_put(&self, rune_id: &RuneId, height: u32, value: u128) {
        let mut combined_key = rune_id.store_bytes();
        combined_key.extend_from_slice(&height.to_be_bytes());
        self.put(RUNE_ID_HEIGHT_TO_MINTS, &combined_key, &value.to_be_bytes()).unwrap()
    }

    pub fn rune_id_height_to_mints_get(&self, rune_id: &RuneId, height: u32) -> Option<u128> {
        let mut combined_key = rune_id.store_bytes();
        combined_key.extend_from_slice(&height.to_be_bytes());
        self.get(RUNE_ID_HEIGHT_TO_MINTS, &combined_key)
            .map(|opt| opt.map(|bytes| u128::from_be_bytes(bytes.try_into().unwrap()))).unwrap()
    }

    pub fn rune_id_height_to_mints_inc(&self, rune_id: &RuneId, height: u32) {
        let mut combined_key = rune_id.store_bytes();
        combined_key.extend_from_slice(&height.to_be_bytes());
        let current = self.rune_id_height_to_mints_get(rune_id, height).unwrap_or_default() + 1;
        self.put(RUNE_ID_HEIGHT_TO_MINTS, &combined_key, &current.to_be_bytes()).unwrap()
    }

    pub fn rune_id_to_mints_sum_to_height(&self, rune_id: &RuneId, to_height: u32) -> u128 {
        let cf = self.get_cf(RUNE_ID_HEIGHT_TO_MINTS);
        let prefix = rune_id.store_bytes();
        let prefix_len = prefix.len();
        let iter = self.db.prefix_iterator_cf(cf, &prefix);
        let mut count = 0;
        for x in iter {
            let (k, v) = x.unwrap();

            if prefix != k[0..prefix_len] {
                break;
            }

            let height = u32::from_be_bytes([k[0], k[1], k[2], k[3]]);
            if height <= to_height {
                let v = u128::from_be_bytes([
                    v[0], v[1], v[2], v[3], v[4], v[5], v[6], v[7],
                    v[8], v[9], v[10], v[11], v[12], v[13], v[14], v[15],
                ]);
                count += v;
            }
        }
        count
    }

    pub fn rune_id_height_to_burned_put(&self, rune_id: &RuneId, height: u32, value: u128) {
        let mut combined_key = rune_id.store_bytes();
        combined_key.extend_from_slice(&height.to_be_bytes());
        self.put(RUNE_ID_HEIGHT_TO_BURNED, &combined_key, &value.to_be_bytes()).unwrap()
    }

    pub fn rune_id_height_to_burned_put_with_batch(&self, wtx: &mut WriteBatch, rune_id: &RuneId, height: u32, value: u128) {
        let mut combined_key = rune_id.store_bytes();
        combined_key.extend_from_slice(&height.to_be_bytes());
        wtx.put_cf(self.get_cf(RUNE_ID_HEIGHT_TO_BURNED), &combined_key, value.to_be_bytes())
    }

    pub fn rune_id_height_to_burned_get(&self, rune_id: &RuneId, height: u32) -> Option<u128> {
        let mut combined_key = rune_id.store_bytes();
        combined_key.extend_from_slice(&height.to_be_bytes());
        self.get(RUNE_ID_HEIGHT_TO_BURNED, &combined_key)
            .map(|opt| opt.map(|bytes| u128::from_be_bytes(bytes.try_into().unwrap()))).unwrap()
    }

    pub fn rune_id_height_to_burned_sum_to_height(&self, rune_id: &RuneId, to_height: u32) -> u128 {
        let cf = self.get_cf(RUNE_ID_HEIGHT_TO_BURNED);
        let prefix = rune_id.store_bytes();
        let prefix_len = prefix.len();
        let iter = self.db.prefix_iterator_cf(cf, &prefix);
        let mut count = 0;
        for x in iter {
            let (k, v) = x.unwrap();

            if prefix != k[0..prefix_len] {
                break;
            }

            let height = u32::from_be_bytes([k[0], k[1], k[2], k[3]]);
            if height <= to_height {
                let v = u128::from_be_bytes([
                    v[0], v[1], v[2], v[3], v[4], v[5], v[6], v[7],
                    v[8], v[9], v[10], v[11], v[12], v[13], v[14], v[15],
                ]);
                count += v;
            }
        }
        count
    }

    pub fn outpoint_to_rune_balances_put(&self, key: &OutPoint, value: RuneBalanceEntry) {
        self.put(OUTPOINT_TO_RUNE_BALANCES, &key.store(), &value.store_bytes()).unwrap()
    }

    pub fn outpoint_to_rune_balances_get(&self, key: &OutPoint) -> Option<RuneBalanceEntry> {
        self.get(OUTPOINT_TO_RUNE_BALANCES, &key.store())
            .map(|opt| opt.map(|bytes| RuneBalanceEntry::load_bytes(&bytes))).unwrap()
    }


    pub fn rune_id_to_rune_entry_put(&self, key: &RuneId, value: &RuneEntry) {
        self.put(RUNE_ID_TO_RUNE_ENTRY, &key.store_bytes(), &value.store_bytes()).unwrap()
    }

    pub fn rune_id_to_rune_entry_get(&self, key: &RuneId) -> Option<RuneEntry> {
        self.get(RUNE_ID_TO_RUNE_ENTRY, &key.store_bytes())
            .map(|opt| opt.map(|bytes| RuneEntry::load_bytes(&bytes))).unwrap()
    }
    pub fn rune_id_to_rune_entry_del(&self, key: &RuneId) {
        self.del(RUNE_ID_TO_RUNE_ENTRY, &key.store_bytes()).unwrap()
    }

    pub fn rune_entry_paged(&self, cursor: usize, size: usize, keywords: Option<String>, sort: Option<String>) -> (bool, Vec<(RuneId, RuneEntry)>) {
        let cf = self.get_cf(RUNE_ID_TO_RUNE_ENTRY);
        let keywords = keywords.map(|x| x.to_uppercase());
        let mode = match sort.as_deref() {
            Some("asc") => IteratorMode::Start,
            Some("desc") => IteratorMode::End,
            _ => IteratorMode::Start,
        };
        let mut iter = self.db.iterator_cf(cf, mode);
        let mut list = vec![];
        let mut cursor = cursor;
        while cursor > 0 {
            if let Some(keywords) = &keywords {
                if let Some(v) = iter.next() {
                    let (k, v) = v.unwrap();
                    let key = RuneId::load_bytes(&k);
                    let value = RuneEntry::load_bytes(&v);
                    if value.spaced_rune.rune.to_string().contains(keywords) || value.spaced_rune.to_string().contains(keywords) || key.to_string().contains(keywords) {
                        cursor -= 1;
                    }
                } else {
                    return (false, list);
                }
            } else {
                if iter.next().is_none() {
                    return (false, list);
                }
                cursor -= 1;
            }
        }
        while let Some(v) = iter.next() {
            let (k, v) = v.unwrap();
            let key = RuneId::load_bytes(&k);
            let value = RuneEntry::load_bytes(&v);
            if let Some(keywords) = &keywords {
                if !value.spaced_rune.rune.to_string().contains(keywords) && !value.spaced_rune.to_string().contains(keywords) && !key.to_string().contains(keywords) {
                    continue;
                }
            }
            list.push((key, value));
            if list.len() >= size {
                return (iter.next().is_some(), list);
            }
        }
        (false, list)
    }

    pub fn rune_to_rune_id_put(&self, key: &Rune, value: &RuneId) {
        self.put(RUNE_TO_RUNE_ID, &key.store_bytes(), &value.store_bytes()).unwrap()
    }

    pub fn rune_to_rune_id_del(&self, key: &Rune) {
        self.del(RUNE_TO_RUNE_ID, &key.store_bytes()).unwrap()
    }

    pub fn rune_to_rune_id_get(&self, key: &Rune) -> Option<RuneId> {
        self.get(RUNE_TO_RUNE_ID, &key.store_bytes())
            .map(|opt| opt.map(|bytes| RuneId::load_bytes(&bytes))).unwrap()
    }

    pub fn spk_outpoint_to_spent_height_put(&self, key: &ScriptBuf, value: &OutPoint) {
        let mut combined_key = key.as_bytes().to_vec();
        combined_key.extend_from_slice(&value.store());
        self.put(SPK_OUTPOINT_TO_SPENT_HEIGHT, &combined_key, &[]).unwrap()
    }

    pub fn spk_outpoint_to_spent_height_spent(&self, key: &ScriptBuf, value: &OutPoint, height: u32) {
        let mut combined_key = key.as_bytes().to_vec();
        combined_key.extend_from_slice(&value.store());
        self.put(SPK_OUTPOINT_TO_SPENT_HEIGHT, &combined_key, &height.to_be_bytes()).unwrap()
    }

    pub fn spk_outpoint_to_spent_height_del(&self, key: &ScriptBuf, value: &OutPoint) {
        let mut combined_key = key.as_bytes().to_vec();
        combined_key.extend_from_slice(&value.store());
        self.del(SPK_OUTPOINT_TO_SPENT_HEIGHT, &combined_key).unwrap()
    }

    pub fn spk_outpoint_to_del_spent_height_gt_reorg_depth_batch(&self, keys: &HashSet<ScriptBuf>, height: u32) {
        if keys.is_empty() {
            return;
        }
        let cf = self.get_cf(SPK_OUTPOINT_TO_SPENT_HEIGHT);
        let mut write_batch = WriteBatch::default();
        for key in keys {
            let prefix = key.as_bytes();
            let prefix_len = prefix.len();
            let iter = self.db.prefix_iterator_cf(cf, prefix);
            for x in iter {
                let (k, v) = x.unwrap();

                if prefix != &k[0..prefix_len] {
                    break;
                }

                if v.is_empty() {
                    continue;
                }
                let spent_height = u32::from_be_bytes([v[0], v[1], v[2], v[3]]);
                if spent_height == 0 || height - spent_height < REORG_DEPTH {
                    continue;
                }
                write_batch.delete_cf(cf, &k);
            }
        }
        self.write_batch(write_batch).unwrap();
    }

    pub fn spk_to_rune_balance_entries(&self, key: &ScriptBuf) -> Vec<(OutPoint, RuneBalanceEntry)> {
        let cf = self.get_cf(SPK_OUTPOINT_TO_SPENT_HEIGHT);
        let mut list = vec![];
        let prefix = key.as_bytes();
        let prefix_len = prefix.len();
        let iter = self.db.prefix_iterator_cf(cf, prefix);
        for x in iter {
            let (k, v) = x.unwrap();

            if prefix != &k[0..prefix_len] {
                break;
            }

            if !v.is_empty() {
                continue;
            }
            let x1 = &k[prefix_len..];
            if x1.len() != 36 {
                error!("Invalid outpoint length: {}", x1.len());
                continue;
            }
            let mut outpoint: OutPointValue = [0; 36];
            outpoint.copy_from_slice(x1);
            let outpoint = OutPoint::load(outpoint);
            if let Some(v) = self.outpoint_to_rune_balances_get(&outpoint) {
                if v.1 == 0 {
                    list.push((outpoint, v));
                }
            }
        }
        list
    }

    pub fn height_to_block_header_put(&self, key: u32, value: &Header) {
        self.put(HEIGHT_TO_BLOCK_HEADER, &key.to_be_bytes(), &value.store_bytes()).unwrap()
    }

    pub fn height_to_block_header_get(&self, key: u32) -> Option<Header> {
        self.get(HEIGHT_TO_BLOCK_HEADER, &key.to_be_bytes())
            .map(|opt| opt.map(|bytes| Header::load_bytes(&bytes))).unwrap()
    }

    pub fn latest_indexed_height(&self) -> Option<u32> {
        let cf = self.get_cf(HEIGHT_TO_BLOCK_HEADER);
        let mut iter = self.db.iterator_cf(cf, IteratorMode::End);
        match iter.next() {
            None => None,
            Some(v) => {
                let k = v.unwrap().0;
                let height = u32::from_be_bytes([k[0], k[1], k[2], k[3]]);
                Some(height)
            }
        }
    }

    pub fn latest_height(&self) -> Option<u32> {
        self.statistic_to_value_get(&Statistic::LatestHeight)
    }

    pub fn height_to_statistic_count_put(&self, statistic: &Statistic, height: u32, value: u32) {
        let mut combined_key: [u8; 5] = [0; 5];
        combined_key[0] = statistic.key();
        combined_key[1..].copy_from_slice(&height.to_be_bytes());
        self.put(HEIGHT_TO_STATISTIC_COUNT, &combined_key, &value.to_be_bytes()).unwrap()
    }

    pub fn height_to_statistic_count_inc(&self, statistic: &Statistic, height: u32) {
        let mut combined_key: [u8; 5] = [0; 5];
        combined_key[0] = statistic.key();
        combined_key[1..].copy_from_slice(&height.to_be_bytes());
        let current = self.height_to_statistic_count_get(statistic, height).unwrap_or_default() + 1;
        self.put(HEIGHT_TO_STATISTIC_COUNT, &combined_key, &current.to_be_bytes()).unwrap()
    }

    pub fn height_to_statistic_count_get(&self, statistic: &Statistic, height: u32) -> Option<u32> {
        let mut combined_key: [u8; 5] = [0; 5];
        combined_key[0] = statistic.key();
        combined_key[1..].copy_from_slice(&height.to_be_bytes());
        self.get(HEIGHT_TO_STATISTIC_COUNT, &combined_key)
            .map(|opt| opt.map(|bytes| u32::from_be_bytes(bytes.try_into().unwrap()))).unwrap()
    }

    pub fn height_to_statistic_count_sum_to_height(&self, statistic: &Statistic, to_height: u32) -> u32 {
        let cf = self.get_cf(HEIGHT_TO_STATISTIC_COUNT);
        let prefix = statistic.key();
        let iter = self.db.prefix_iterator_cf(cf, [prefix]);
        let mut count = 0;
        for x in iter {
            let (k, v) = x.unwrap();
            if k[0] != prefix {
                break;
            }
            let height = u32::from_be_bytes([k[1], k[2], k[3], k[4]]);
            if height <= to_height {
                let v = u32::from_be_bytes([v[0], v[1], v[2], v[3]]);
                count += v;
            }
        }
        count
    }

    pub fn reorg_to_height(&self, height: u32) {
        info!("Reorg to height: {}", height);

        // Delete all data after height
        info!("<= HEIGHT_TO_BLOCK_HEADER ...");
        let cf = self.get_cf(HEIGHT_TO_BLOCK_HEADER);
        let iter = self.db.iterator_cf(cf, IteratorMode::End);
        let mut batch = WriteBatch::default();
        let mut count = 0;
        for v in iter {
            count += 1;
            let (k, _) = v.unwrap();
            let h = u32::from_be_bytes([k[0], k[1], k[2], k[3]]);
            if h >= height {
                batch.delete_cf(cf, &k);
            }
        }
        info!("<= HEIGHT_TO_BLOCK_HEADER {}", count);

        info!("<= HEIGHT_TO_STATISTIC_COUNT ...");
        let cf = self.get_cf(HEIGHT_TO_STATISTIC_COUNT);
        let iter = self.db.iterator_cf(cf, IteratorMode::End);
        let mut count = 0;
        for v in iter {
            count += 1;
            let (k, _) = v.unwrap();
            let h = u32::from_be_bytes([k[1], k[2], k[3], k[4]]);
            if h >= height {
                batch.delete_cf(cf, &k);
            }
        }
        info!("<= HEIGHT_TO_STATISTIC_COUNT {}", count);

        info!("<= RUNE_ID_HEIGHT_TO_MINTS ...");
        let cf = self.get_cf(RUNE_ID_HEIGHT_TO_MINTS);
        let iter = self.db.iterator_cf(cf, IteratorMode::End);
        let mut count = 0;
        for v in iter {
            count += 1;
            let (k, _) = v.unwrap();
            let h = u64::from_be_bytes(k[0..8].try_into().unwrap());
            if h >= height as _ {
                batch.delete_cf(cf, &k);
            }
        }
        info!("<= RUNE_ID_HEIGHT_TO_MINTS {}", count);

        info!("<= RUNE_ID_HEIGHT_TO_BURNED ...");
        let cf = self.get_cf(RUNE_ID_HEIGHT_TO_BURNED);
        let iter = self.db.iterator_cf(cf, IteratorMode::End);
        let mut count = 0;
        for v in iter {
            count += 1;
            let (k, _) = v.unwrap();
            let h = u64::from_be_bytes(k[0..8].try_into().unwrap());
            if h >= height as _ {
                batch.delete_cf(cf, &k);
            }
        }
        info!("<= RUNE_ID_HEIGHT_TO_BURNED {}", count);


        info!("<= RUNE_ID_TO_RUNE_ENTRY/RUNE_TO_RUNE_ID ...");
        let cf = self.get_cf(RUNE_ID_TO_RUNE_ENTRY);
        let iter = self.db.iterator_cf(cf, IteratorMode::End);
        let mut count = 0;
        for v in iter {
            count += 1;
            let (k, _) = v.unwrap();
            let h = u64::from_be_bytes(k[0..8].try_into().unwrap());
            if h >= height as _ {
                {
                    let rune_id = RuneId::load_bytes(&k);
                    let entry = self.rune_id_to_rune_entry_get(&rune_id).unwrap();
                    let cf = self.get_cf(RUNE_TO_RUNE_ID);
                    batch.delete_cf(cf, &entry.spaced_rune.rune.store_bytes());
                }
                batch.delete_cf(cf, &k);
            }
        }
        info!("<= RUNE_ID_TO_RUNE_ENTRY/RUNE_TO_RUNE_ID {}", count);


        info!("<= OUTPOINT_TO_RUNE_BALANCES ...");
        let cf = self.get_cf(OUTPOINT_TO_RUNE_BALANCES);
        let iter = self.db.iterator_cf(cf, IteratorMode::End);
        let mut count = 0;
        let mut deleted = 0;
        let mut changed = 0;
        for v in iter {
            count += 1;
            let (k, v) = v.unwrap();
            let confirmed_height = u32::from_le_bytes(v[0..4].try_into().unwrap());
            if confirmed_height >= height {
                batch.delete_cf(cf, &k);
                deleted += 1;
                continue;
            }
            let spent_height = u32::from_le_bytes(v[4..8].try_into().unwrap());
            if spent_height >= height {
                let mut entry = RuneBalanceEntry::load_bytes(&v);
                entry.1 = 0;
                batch.put_cf(cf, &k, &entry.store_bytes());
                changed += 1;
            }
        }

        info!("<= OUTPOINT_TO_RUNE_BALANCES {}, deleted: {}, changed: {}", count, deleted, changed);

        self.db.write(batch).unwrap();

        info!("Write stage 1 done.");


        // Update rune info
        let mut batch = WriteBatch::default();

        info!("<= STATISTIC_TO_VALUE Statistic::Runes ...");
        let runes_count = self.height_to_statistic_count_sum_to_height(&Statistic::Runes, height - 1);
        batch.put_cf(self.get_cf(STATISTIC_TO_VALUE), [Statistic::Runes.key()], runes_count.to_be_bytes());
        info!("<= STATISTIC_TO_VALUE Statistic::Runes {}", runes_count);

        info!("<= STATISTIC_TO_VALUE Statistic::ReservedRunes ...");
        let reserved_runes_count = self.height_to_statistic_count_sum_to_height(&Statistic::ReservedRunes, height - 1);
        batch.put_cf(self.get_cf(STATISTIC_TO_VALUE), [Statistic::ReservedRunes.key()], reserved_runes_count.to_be_bytes());
        info!("<= STATISTIC_TO_VALUE Statistic::ReservedRunes {}", reserved_runes_count);


        info!("<= RUNE_ID_TO_RUNE_ENTRY ...");
        let cf = self.get_cf(RUNE_ID_TO_RUNE_ENTRY);
        let iter = self.db.iterator_cf(cf, IteratorMode::Start);

        let mut runes_total = 0;
        for (number, v) in iter.enumerate() {
            runes_total += 1;
            let (k, v) = v.unwrap();
            let key = RuneId::load_bytes(&k);
            let mut value = RuneEntry::load_bytes(&v);
            let burned = self.rune_id_height_to_burned_sum_to_height(&key, height);
            batch.put_cf(self.get_cf(RUNE_ID_TO_BURNED), &k, burned.to_be_bytes());
            value.burned = burned;
            let mints = self.rune_id_to_mints_sum_to_height(&key, height);
            batch.put_cf(self.get_cf(RUNE_ID_TO_MINTS), &k, mints.to_be_bytes());
            value.mints = mints;
            value.number = number as _;
            batch.put_cf(cf, &k, &value.store_bytes());
        }
        info!("<= RUNE_ID_TO_RUNE_ENTRY {}", runes_total);
        if runes_count != runes_total {
            panic!("Runes count mismatch: {} != {}", runes_count, runes_total);
        }
        self.db.write(batch).unwrap();
        info!("Write stage 2 done.");
    }

    pub fn flush(&self) {
        self.db.flush_wal(true).unwrap();
        self.db.flush().unwrap();
    }
}