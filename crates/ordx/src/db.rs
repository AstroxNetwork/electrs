use std::collections::HashSet;
use std::path::Path;
use std::time::Instant;

use bitcoin::{OutPoint, ScriptBuf};
use bitcoin::block::Header;
use itertools::Itertools;
use log::info;
use rocksdb::{ColumnFamily, ColumnFamilyDescriptor, DB, Error, IteratorMode, Options, WriteBatch};

use ordinals::{Rune, RuneId};

use crate::entry::{Entry, EntryBytes, RuneBalanceEntry, RuneEntry, Statistic};
use crate::updater::REORG_DEPTH;

pub struct RunesDB {
    db: DB,
}

pub const HEIGHT_TO_BLOCK_HEADER: &str = "HEIGHT_TO_BLOCK_HEADER";
pub const HEIGHT_TO_STATISTIC_COUNT: &str = "HEIGHT_TO_STATISTIC_COUNT";
pub const STATISTIC_TO_VALUE: &str = "STATISTIC_TO_VALUE";
pub const OUTPOINT_TO_RUNE_BALANCES: &str = "OUTPOINT_TO_RUNE_BALANCES";
pub const SPK_TO_OUTPOINTS: &str = "SPK_TO_OUTPOINTS";
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

        db_opts.set_max_open_files(100_000); // TODO: make sure to `ulimit -n` this process correctly
        db_opts.set_compaction_style(rocksdb::DBCompactionStyle::Level);
        db_opts.set_compression_type(rocksdb::DBCompressionType::Snappy);
        db_opts.set_target_file_size_base(1_073_741_824);
        db_opts.set_write_buffer_size(256 << 20);
        db_opts.set_disable_auto_compactions(true); // for initial bulk load

        // db_opts.set_advise_random_on_open(???);
        db_opts.set_compaction_readahead_size(1 << 20);
        db_opts.increase_parallelism(2);

        let cf_names = [
            HEIGHT_TO_BLOCK_HEADER,
            HEIGHT_TO_STATISTIC_COUNT,
            STATISTIC_TO_VALUE,
            OUTPOINT_TO_RUNE_BALANCES,
            SPK_TO_OUTPOINTS,
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
        self.db.iterator_cf(cf, rocksdb::IteratorMode::Start)
            .map(|r| {
                let (k, v) = r.unwrap();
                (k.to_vec(), v.to_vec())
            })
            .collect()
    }


    // specific methods

    pub fn statistic_to_value_put(&self, statistic: &Statistic, value: u32) {
        self.put(STATISTIC_TO_VALUE, &[statistic.key()], &value.to_be_bytes()).unwrap()
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
        let iter = self.db.prefix_iterator_cf(cf, rune_id.store_bytes());
        let mut count = 0;
        for x in iter {
            let (k, v) = x.unwrap();
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

    pub fn rune_id_height_to_burned_get(&self, rune_id: &RuneId, height: u32) -> Option<u128> {
        let mut combined_key = rune_id.store_bytes();
        combined_key.extend_from_slice(&height.to_be_bytes());
        self.get(RUNE_ID_HEIGHT_TO_BURNED, &combined_key)
            .map(|opt| opt.map(|bytes| u128::from_be_bytes(bytes.try_into().unwrap()))).unwrap()
    }

    pub fn rune_id_to_burned_sum_to_height(&self, rune_id: &RuneId, to_height: u32) -> u128 {
        let cf = self.get_cf(RUNE_ID_HEIGHT_TO_BURNED);
        let iter = self.db.prefix_iterator_cf(cf, rune_id.store_bytes());
        let mut count = 0;
        for x in iter {
            let (k, v) = x.unwrap();
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

    pub fn spk_to_rune_balance_entries(&self, key: &ScriptBuf) -> Vec<(OutPoint, RuneBalanceEntry)> {
        let entries = self.spk_to_outpoints_get(key).unwrap_or_default();
        let mut list = vec![];
        for outpoint in entries {
            if let Some(v) = self.outpoint_to_rune_balances_get(&outpoint) {
                if v.1 == 0 {
                    list.push((outpoint, v));
                }
            }
        }
        list
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
        for _ in 0..cursor {
            if iter.next().is_none() {
                return (false, list);
            }
        }
        for _ in 0..size {
            match iter.next() {
                None => return (false, list),
                Some(v) => {
                    let (k, v) = v.unwrap();
                    let key = RuneId::load_bytes(&k);
                    let value = RuneEntry::load_bytes(&v);
                    if let Some(keywords) = &keywords {
                        if !value.spaced_rune.rune.to_string().contains(keywords) && !value.spaced_rune.to_string().contains(keywords) && !key.to_string().contains(keywords) {
                            continue;
                        }
                    }
                    list.push((key, value));
                }
            }
        }
        (iter.next().is_some(), list)
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

    pub fn spk_to_outpoints_put(&self, key: &ScriptBuf, value: &[OutPoint]) {
        let value = value.iter().map(|x| x.store()).collect_vec().concat();
        self.put(SPK_TO_OUTPOINTS, key.as_bytes(), &value).unwrap()
    }

    pub fn spk_to_outpoints_get(&self, key: &ScriptBuf) -> Option<Vec<OutPoint>> {
        self.get(SPK_TO_OUTPOINTS, key.as_bytes())
            .map(|opt| opt.map(|bytes| {
                if bytes.is_empty() {
                    return vec![];
                }
                bytes.chunks(36).map(|x| OutPoint::load(x.try_into().unwrap())).collect()
            })).unwrap()
    }

    pub fn spk_to_outpoints_del(&self, key: &ScriptBuf) {
        self.del(SPK_TO_OUTPOINTS, key.as_bytes()).unwrap()
    }

    pub fn spk_to_outpoints_one_put(&self, key: &ScriptBuf, value: &OutPoint) {
        let mut exist = self.spk_to_outpoints_get(key).unwrap_or_default();
        exist.push(*value);
        self.spk_to_outpoints_put(key, &exist);
    }

    pub fn spk_to_outpoints_del_spent_height_gt_reorg_depth(&self, key: &ScriptBuf, height: u32) {
        let start = Instant::now();
        let mut exist = self.spk_to_outpoints_get(key).unwrap_or_default();
        exist.retain(|x| {
            match self.outpoint_to_rune_balances_get(x) {
                None => true,
                Some(v) => v.1 == 0 || height - v.1 < REORG_DEPTH
            }
        });
        if exist.is_empty() {
            self.spk_to_outpoints_del(key);
        } else {
            self.spk_to_outpoints_put(key, &exist);
        }
        info!("spk_to_outpoints_del_spent_height_gt_reorg_depth: {:?}", start.elapsed());
    }

    pub fn spk_to_outpoints_del_spent_height_gt_reorg_depth_batch(&self, keys: &HashSet<ScriptBuf>, height: u32) {
        if keys.is_empty() {
            return;
        }
        let start = Instant::now();
        let mut batch = WriteBatch::default();
        let cf = self.get_cf(SPK_TO_OUTPOINTS);
        for key in keys {
            if let Some(mut exist) = self.spk_to_outpoints_get(key) {
                exist.retain(|x| {
                    match self.outpoint_to_rune_balances_get(x) {
                        None => true,
                        Some(v) => v.1 == 0 || height - v.1 < REORG_DEPTH
                    }
                });
                if exist.is_empty() {
                    batch.delete_cf(cf, key.as_bytes());
                } else {
                    let value = exist.iter().map(|x| x.store()).collect_vec().concat();
                    batch.put_cf(cf, key.as_bytes(), &value);
                }
            }
        }
        self.db.write(batch).unwrap();
        info!("spk_to_outpoints_del_spent_height_gt_reorg_depth_batch: {:?}", start.elapsed());
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

    pub fn height_to_statistic_count_sum_to_height(&self, statistic: &Statistic, to_height: u32) -> u64 {
        let cf = self.get_cf(HEIGHT_TO_STATISTIC_COUNT);
        let iter = self.db.prefix_iterator_cf(cf, [statistic.key()]);
        let mut count = 0;
        for x in iter {
            let (k, v) = x.unwrap();
            let height = u32::from_be_bytes([k[1], k[2], k[3], k[4]]);
            if height <= to_height {
                let v = u64::from_be_bytes([v[0], v[1], v[2], v[3], v[4], v[5], v[6], v[7]]);
                count += v;
            }
        }
        count
    }

    pub fn reorg_to_height(&self, height: u32) {
        // Delete all data after height
        let cf = self.get_cf(HEIGHT_TO_BLOCK_HEADER);
        let iter = self.db.iterator_cf(cf, IteratorMode::End);
        let mut batch = WriteBatch::default();
        for v in iter {
            let (k, _) = v.unwrap();
            let h = u32::from_be_bytes([k[0], k[1], k[2], k[3]]);
            if h >= height {
                batch.delete_cf(cf, &k);
            }
        }

        let cf = self.get_cf(HEIGHT_TO_STATISTIC_COUNT);
        let iter = self.db.iterator_cf(cf, IteratorMode::End);
        for v in iter {
            let (k, _) = v.unwrap();
            let h = u32::from_be_bytes([k[1], k[2], k[3], k[4]]);
            if h >= height {
                batch.delete_cf(cf, &k);
            }
        }

        let cf = self.get_cf(RUNE_ID_HEIGHT_TO_MINTS);
        let iter = self.db.iterator_cf(cf, IteratorMode::End);
        for v in iter {
            let (k, _) = v.unwrap();
            let h = u64::from_be_bytes(k[0..8].try_into().unwrap());
            if h >= height as _ {
                batch.delete_cf(cf, &k);
            }
        }

        let cf = self.get_cf(RUNE_ID_HEIGHT_TO_BURNED);
        let iter = self.db.iterator_cf(cf, IteratorMode::End);
        for v in iter {
            let (k, _) = v.unwrap();
            let h = u64::from_be_bytes(k[0..8].try_into().unwrap());
            if h >= height as _ {
                batch.delete_cf(cf, &k);
            }
        }

        let cf = self.get_cf(RUNE_ID_TO_RUNE_ENTRY);
        let iter = self.db.iterator_cf(cf, IteratorMode::End);
        for v in iter {
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

        let cf = self.get_cf(OUTPOINT_TO_RUNE_BALANCES);
        let iter = self.db.iterator_cf(cf, IteratorMode::End);
        for v in iter {
            let (k, v) = v.unwrap();
            let confirmed_height = u32::from_be_bytes(v[0..4].try_into().unwrap());
            if confirmed_height >= height {
                batch.delete_cf(cf, &k);
                continue;
            }
            let spent_height = u32::from_be_bytes(v[4..8].try_into().unwrap());
            if spent_height >= height {
                let mut entry = RuneBalanceEntry::load_bytes(&v);
                entry.1 = 0;
                batch.put_cf(cf, &k, &entry.store_bytes());
            }
        }

        self.db.write(batch).unwrap();


        // Update rune info
        let mut batch = WriteBatch::default();

        let runes_count = self.height_to_statistic_count_sum_to_height(&Statistic::Runes, height - 1);
        batch.put_cf(self.get_cf(STATISTIC_TO_VALUE), [Statistic::Runes.key()], runes_count.to_be_bytes());

        let reserved_runes_count = self.height_to_statistic_count_sum_to_height(&Statistic::ReservedRunes, height - 1);
        batch.put_cf(self.get_cf(STATISTIC_TO_VALUE), [Statistic::ReservedRunes.key()], reserved_runes_count.to_be_bytes());


        let cf = self.get_cf(RUNE_ID_TO_RUNE_ENTRY);
        let iter = self.db.iterator_cf(cf, IteratorMode::Start);

        let mut runes_total = 0;
        for (number, v) in iter.enumerate() {
            let (k, v) = v.unwrap();
            let key = RuneId::load_bytes(&k);
            let mut value = RuneEntry::load_bytes(&v);
            let burned = self.rune_id_to_burned_sum_to_height(&key, height);
            batch.put_cf(self.get_cf(RUNE_ID_TO_BURNED), &k, burned.to_be_bytes());
            value.burned = burned;
            let mints = self.rune_id_to_mints_sum_to_height(&key, height);
            batch.put_cf(self.get_cf(RUNE_ID_TO_MINTS), &k, mints.to_be_bytes());
            value.mints = mints;
            value.number = number as _;
            batch.put_cf(cf, &k, &value.store_bytes());
            runes_total += 1;
        }
        if runes_count != runes_total {
            panic!("Runes count mismatch: {} != {}", runes_count, runes_total);
        }
        self.db.write(batch).unwrap();
    }
}