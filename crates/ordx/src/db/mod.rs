use std::collections::{HashMap, HashSet};
use std::fmt::Debug;
use std::path::Path;
use std::time::Instant;

use bitcoin::block::Header;
use bitcoin::OutPoint;
use log::info;
use r2d2::{CustomizeConnection, Pool};
use r2d2_sqlite::SqliteConnectionManager;
use rocksdb::{ColumnFamily, ColumnFamilyDescriptor, DB, Error, IteratorMode, Options, WriteBatch};
use rusqlite::{Connection, params, params_from_iter, ToSql};
use rusqlite::types::ToSqlOutput;

use ordinals::{Rune, RuneId};

use crate::db::model::{RuneBalanceForInsert, RuneBalanceForTemp, RuneBalanceForUpdate, RuneEntryForQueryInsert, RuneEntryForTemp, RuneEntryForUpdate};
use crate::entry::{Entry, EntryBytes, RuneBalanceEntry, RuneEntry, Statistic};
use crate::updater::REORG_DEPTH;

pub mod model;

#[derive(Copy, Clone, Debug)]
struct Customizer;


impl CustomizeConnection<Connection, rusqlite::Error> for Customizer {
    fn on_acquire(&self, conn: &mut Connection) -> Result<(), rusqlite::Error> {
        let ok = conn.execute_batch(include_str!("../../sql/pragma.sql")).is_ok();
        info!("Acquired connection: {}", ok);
        Ok(())
    }
}

type SqlitePool = Pool<SqliteConnectionManager>;

pub struct RunesDB {
    pub rocksdb: DB,
    pub sqlite: SqlitePool,
}

pub const HEIGHT_TO_BLOCK_HEADER: &str = "HEIGHT_TO_BLOCK_HEADER";
pub const HEIGHT_TO_STATISTIC_COUNT: &str = "HEIGHT_TO_STATISTIC_COUNT";
pub const STATISTIC_TO_VALUE: &str = "STATISTIC_TO_VALUE";
pub const OUTPOINT_TO_RUNE_BALANCES: &str = "OUTPOINT_TO_RUNE_BALANCES";
pub const RUNE_ID_TO_RUNE_ENTRY: &str = "RUNE_ID_TO_RUNE_ENTRY";
pub const RUNE_TO_RUNE_ID: &str = "RUNE_TO_RUNE_ID";

pub const HEIGHT_OUTPOINT_TO_RUNE_IDS: &str = "HEIGHT_OUTPOINT_TO_RUNE_IDS";

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
        db_opts.set_compression_type(rocksdb::DBCompressionType::Snappy);

        let cf_names = [
            HEIGHT_TO_BLOCK_HEADER,
            HEIGHT_TO_STATISTIC_COUNT,
            STATISTIC_TO_VALUE,
            OUTPOINT_TO_RUNE_BALANCES,
            RUNE_ID_TO_RUNE_ENTRY,
            RUNE_TO_RUNE_ID,
            RUNE_ID_HEIGHT_TO_MINTS,
            RUNE_ID_HEIGHT_TO_BURNED,
            RUNE_ID_TO_MINTS,
            RUNE_ID_TO_BURNED,
            HEIGHT_OUTPOINT_TO_RUNE_IDS,
        ];
        let cf_descriptors: Vec<_> = cf_names.iter()
            .map(|name| ColumnFamilyDescriptor::new(*name, Options::default()))
            .collect();

        let rocksdb_path = path.as_ref().join("rocksdb");
        info!("Using rocksdb at {:?}", &rocksdb_path);
        let open_rocksdb = Instant::now();
        let rocksdb = DB::open_cf_descriptors(&db_opts, rocksdb_path, cf_descriptors).unwrap();
        info!("Rocksdb opened, {:?}", open_rocksdb.elapsed());

        let sqlite_path = path.as_ref().join("sqlite.db");
        info!("Using sqlite at {:?}", &sqlite_path);
        let manager = SqliteConnectionManager::file(sqlite_path);
        let sqlite = Pool::builder()
            .min_idle(Some(1))
            .max_size(100)
            .connection_customizer(Box::new(Customizer))
            .build(manager)
            .unwrap();
        RunesDB { rocksdb, sqlite }
    }

    pub fn init_sqlite(&self) -> anyhow::Result<()> {
        let conn = self.sqlite.get()?;
        conn.execute_batch(include_str!("../../sql/init.sql"))?;
        Ok(())
    }


    #[inline]
    pub fn get_cf(&self, cf_name: &str) -> &ColumnFamily {
        self.rocksdb.cf_handle(cf_name).unwrap_or_else(|| panic!("Column family {} not found", cf_name))
    }

    pub fn put(&self, cf_name: &str, key: &[u8], value: &[u8]) -> Result<(), Error> {
        let cf = self.get_cf(cf_name);
        self.rocksdb.put_cf(cf, key, value)
    }

    pub fn insert(&self, cf_name: &str, key: &[u8], value: &[u8]) -> Result<(), Error> {
        self.put(cf_name, key, value)
    }

    pub fn get(&self, cf_name: &str, key: &[u8]) -> Result<Option<Vec<u8>>, Error> {
        let cf = self.get_cf(cf_name);
        self.rocksdb.get_cf(cf, key)
    }

    pub fn del(&self, cf_name: &str, key: &[u8]) -> Result<(), Error> {
        let cf = self.get_cf(cf_name);
        self.rocksdb.delete_cf(cf, key)
    }

    pub fn remove(&self, cf_name: &str, key: &[u8]) -> Result<(), Error> {
        self.del(cf_name, key)
    }

    pub fn list(&self, cf_name: &str) -> Vec<(Vec<u8>, Vec<u8>)> {
        let cf = self.get_cf(cf_name);
        self.rocksdb.iterator_cf(cf, IteratorMode::Start)
            .map(|r| {
                let (k, v) = r.unwrap();
                (k.to_vec(), v.to_vec())
            })
            .collect()
    }

    pub fn write_batch(&self, batch: WriteBatch) -> Result<(), Error> {
        self.rocksdb.write(batch)
    }


    // specific methods
    pub fn height_outpoint_to_rune_ids_batch_put_and_del(&self, height: u32, outpoints: &HashMap<OutPoint, HashSet<RuneId>>) {
        let mut batch = WriteBatch::default();
        let cf = self.get_cf(HEIGHT_OUTPOINT_TO_RUNE_IDS);
        let iter = self.rocksdb.iterator_cf(cf, IteratorMode::Start);
        let mut deleted = 0;
        for x in iter {
            let (k, _) = x.unwrap();
            let h = u32::from_be_bytes([k[0], k[1], k[2], k[3]]) as i64;
            if (height as i64) - h < (REORG_DEPTH as i64) {
                break;
            }
            batch.delete_cf(cf, &k);
            deleted += 1;
        }
        if outpoints.is_empty() {
            if deleted > 0 {
                info!("<= HEIGHT_OUTPOINT_TO_RUNE_IDS, inserted: {}, deleted: {}", outpoints.len(), deleted);
                self.rocksdb.write(batch).unwrap();
            }
            return;
        }
        for (outpoint, value) in outpoints {
            let mut key = height.to_be_bytes().to_vec();
            key.extend_from_slice(&outpoint.store());
            batch.put_cf(cf, &key, value.iter().map(|x| x.store_bytes()).collect::<Vec<_>>().concat().as_slice());
        }
        self.rocksdb.write(batch).unwrap();
        info!("<= HEIGHT_OUTPOINT_TO_RUNE_IDS, inserted: {}, deleted: {}", outpoints.len(), deleted);
    }

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
        let iter = self.rocksdb.prefix_iterator_cf(cf, &prefix);
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
        let iter = self.rocksdb.prefix_iterator_cf(cf, &prefix);
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
        let mut iter = self.rocksdb.iterator_cf(cf, mode);
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


    pub fn height_to_block_header_put(&self, key: u32, value: &Header) {
        self.put(HEIGHT_TO_BLOCK_HEADER, &key.to_be_bytes(), &value.store_bytes()).unwrap()
    }

    pub fn height_to_block_header_get(&self, key: u32) -> Option<Header> {
        self.get(HEIGHT_TO_BLOCK_HEADER, &key.to_be_bytes())
            .map(|opt| opt.map(|bytes| Header::load_bytes(&bytes))).unwrap()
    }

    pub fn latest_indexed_height(&self) -> Option<u32> {
        let cf = self.get_cf(HEIGHT_TO_BLOCK_HEADER);
        let mut iter = self.rocksdb.iterator_cf(cf, IteratorMode::End);
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
        let iter = self.rocksdb.prefix_iterator_cf(cf, [prefix]);
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

    pub fn reorg_to_height(&self, height: u32, latest_height: u32) -> anyhow::Result<()> {
        info!("Reorg to height: {}", height);

        // Delete all data after height
        info!("<= HEIGHT_TO_BLOCK_HEADER ...");
        let cf = self.get_cf(HEIGHT_TO_BLOCK_HEADER);
        let iter = self.rocksdb.iterator_cf(cf, IteratorMode::End);
        let mut batch = WriteBatch::default();
        let mut deleted = 0;
        for v in iter {
            let (k, _) = v.unwrap();
            let h = u32::from_be_bytes([k[0], k[1], k[2], k[3]]);
            if h >= height {
                batch.delete_cf(cf, &k);
                deleted += 1;
            } else {
                break;
            }
        }
        info!("<= HEIGHT_TO_BLOCK_HEADER deleted: {}", deleted);

        info!("<= HEIGHT_TO_STATISTIC_COUNT ...");
        let cf = self.get_cf(HEIGHT_TO_STATISTIC_COUNT);
        let iter = self.rocksdb.iterator_cf(cf, IteratorMode::End);
        let mut deleted = 0;
        for v in iter {
            let (k, _) = v.unwrap();
            let h = u32::from_be_bytes([k[1], k[2], k[3], k[4]]);
            if h >= height {
                batch.delete_cf(cf, &k);
                deleted += 1;
            } else {
                break;
            }
        }
        info!("<= HEIGHT_TO_STATISTIC_COUNT deleted: {}",  deleted);

        info!("<= RUNE_ID_HEIGHT_TO_MINTS ...");
        let cf = self.get_cf(RUNE_ID_HEIGHT_TO_MINTS);
        let iter = self.rocksdb.iterator_cf(cf, IteratorMode::End);
        let mut deleted = 0;
        for v in iter {
            let (k, _) = v.unwrap();
            let h = u64::from_be_bytes(k[0..8].try_into().unwrap());
            if h >= height as _ {
                batch.delete_cf(cf, &k);
                deleted += 1;
            } else {
                break;
            }
        }
        info!("<= RUNE_ID_HEIGHT_TO_MINTS deleted: {}", deleted);

        info!("<= RUNE_ID_HEIGHT_TO_BURNED ...");
        let cf = self.get_cf(RUNE_ID_HEIGHT_TO_BURNED);
        let iter = self.rocksdb.iterator_cf(cf, IteratorMode::End);
        let mut deleted = 0;
        for v in iter {
            let (k, _) = v.unwrap();
            let h = u64::from_be_bytes(k[0..8].try_into().unwrap());
            if h >= height as _ {
                batch.delete_cf(cf, &k);
                deleted += 1;
            } else {
                break;
            }
        }
        info!("<= RUNE_ID_HEIGHT_TO_BURNED deleted: {}",deleted);


        info!("<= RUNE_ID_TO_RUNE_ENTRY/RUNE_TO_RUNE_ID ...");
        let cf = self.get_cf(RUNE_ID_TO_RUNE_ENTRY);
        let iter = self.rocksdb.iterator_cf(cf, IteratorMode::End);
        let mut deleted = 0;
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
                deleted += 1;
            } else {
                break;
            }
        }
        info!("<= RUNE_ID_TO_RUNE_ENTRY deleted: {}", deleted);


        info!("<= OUTPOINT_TO_RUNE_BALANCES ...");
        let temp_cf = self.get_cf(HEIGHT_OUTPOINT_TO_RUNE_IDS);
        let otrb_cf = self.get_cf(OUTPOINT_TO_RUNE_BALANCES);
        let iter = self.rocksdb.iterator_cf(temp_cf, IteratorMode::End);
        let mut deleted = 0;
        let mut changed = 0;
        let mut changed_rune_ids = HashSet::new();
        for x in iter {
            let (tk, _) = x.unwrap();
            let h = u32::from_be_bytes([tk[0], tk[1], tk[2], tk[3]]);
            if h >= height {
                batch.delete_cf(temp_cf, &tk);
                let k = &tk[4..];
                let v = self.rocksdb.get_cf(otrb_cf, k).unwrap().unwrap();
                let confirmed_height = u32::from_le_bytes(v[0..4].try_into().unwrap());
                if confirmed_height >= height {
                    batch.delete_cf(otrb_cf, k);
                    deleted += 1;
                    continue;
                }
                let spent_height = u32::from_le_bytes(v[4..8].try_into().unwrap());
                if spent_height >= height {
                    let mut entry = RuneBalanceEntry::load_bytes(&v);
                    entry.1 = 0;
                    batch.put_cf(otrb_cf, k, &entry.store_bytes());
                    changed += 1;
                    v.chunks(12).for_each(|x| {
                        let rune_id = RuneId::load_bytes(x);
                        changed_rune_ids.insert(rune_id);
                    });
                }
            } else {
                break;
            }
        }
        info!("<= OUTPOINT_TO_RUNE_BALANCES deleted: {}, changed: {}", deleted, changed);

        self.rocksdb.write(batch).unwrap();

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


        info!("<= SQLITE: Deleting/Updating rune_balances, rune_entry ...");
        let mut conn = self.sqlite.get().unwrap();
        let del_rune_balance_count = conn.execute("DELETE FROM rune_balance WHERE height >= ?", params![height])?;
        let update_rune_balance_count = conn.execute("UPDATE rune_balance SET spent_height = 0, spent_txid = null, spent_vin = null, spent_ts = null WHERE spent_height >= ?", params![height])?;
        let del_rune_count = conn.execute("DELETE FROM rune_entry WHERE height >= ?", params![height])?;
        info!("<= SQLITE: Deleted rune_balances {}, Updated rune_balances {}, Deleted rune_entry {}", del_rune_balance_count, update_rune_balance_count, del_rune_count);


        info!("Write stage 2 done.");


        info!("<= RUNE_ID_TO_RUNE_ENTRY ...");
        let cf = self.get_cf(RUNE_ID_TO_RUNE_ENTRY);
        let iter = self.rocksdb.iterator_cf(cf, IteratorMode::Start);

        let mut runes_total = 0;
        let mut changed_runes = HashMap::new();
        for (number, v) in iter.enumerate() {
            runes_total += 1;
            let mut has_changed = false;
            let (k, v) = v.unwrap();
            let key = RuneId::load_bytes(&k);
            let mut entry = RuneEntry::load_bytes(&v);
            let burned = self.rune_id_height_to_burned_sum_to_height(&key, height);
            batch.put_cf(self.get_cf(RUNE_ID_TO_BURNED), &k, burned.to_be_bytes());

            if entry.burned != burned {
                entry.burned = burned;
                has_changed = true;
            }

            let mints = self.rune_id_to_mints_sum_to_height(&key, height);
            batch.put_cf(self.get_cf(RUNE_ID_TO_MINTS), &k, mints.to_be_bytes());

            if entry.mints != mints {
                entry.mints = mints;
                has_changed = true;
            }

            let number = number as u64;

            if entry.number != number {
                entry.number = number;
                has_changed = true;
            }

            if has_changed {
                batch.put_cf(cf, &k, &entry.store_bytes());
            }

            if has_changed || changed_rune_ids.contains(&key) {
                changed_runes.insert(key.to_string(), RuneEntryForUpdate {
                    rune_id: key.to_string(),
                    mints: entry.mints.to_string(),
                    burned: entry.burned.to_string(),
                    mintable: entry.mintable(latest_height as _).unwrap_or(0) > 0,
                });
            }
        }
        info!("<= RUNE_ID_TO_RUNE_ENTRY {}", runes_total);
        if runes_count != runes_total {
            panic!("Runes count mismatch: {} != {}", runes_count, runes_total);
        }
        self.rocksdb.write(batch).unwrap();
        info!("Write stage 3 done.");

        info!("<= SQLITE: Updating rune entries {}", changed_runes.len());

        let mut runes_txs = HashMap::new();
        let mut runes_holders = HashMap::new();
        if !changed_runes.is_empty() {
            let t = Instant::now();
            let need_update_runes = changed_runes.keys().collect::<Vec<&String>>();
            for sub in need_update_runes.chunks(100) {
                let placeholders = sub.iter().map(|_| "?").collect::<Vec<&str>>().join(",");
                let sql = format!("SELECT rune_id, COUNT(DISTINCT _txid) AS txs FROM (SELECT rune_id, txid AS _txid FROM rune_balance where rune_id in ({}) UNION ALL SELECT rune_id, spent_txid AS _txid FROM rune_balance WHERE rune_id in ({}) AND spent_height > 0) AS _ GROUP BY rune_id", &placeholders, &placeholders);
                let mut stmt = conn.prepare_cached(&sql)?;
                stmt.query_map(params_from_iter(sub.iter().chain(sub.iter())), |row| {
                    Ok((row.get::<_, String>(0)?, row.get::<_, u32>(1)?))
                })?.for_each(|x| {
                    let (rune_id, txs) = x.unwrap();
                    runes_txs.insert(rune_id, txs);
                });
                let sql = format!("SELECT rune_id, COUNT(DISTINCT address) AS addresses FROM rune_balance where rune_id in ({}) and spent_height = 0 GROUP BY rune_id", &placeholders);
                let mut stmt = conn.prepare_cached(&sql)?;
                stmt.query_map(params_from_iter(sub.iter()), |row| {
                    Ok((row.get::<_, String>(0)?, row.get::<_, u32>(1)?))
                })?.for_each(|x| {
                    let (rune_id, holders) = x.unwrap();
                    runes_holders.insert(rune_id, holders);
                });
            }
            info!("Querying {} runes txs and holders from sqlite, {:?}", need_update_runes.len(), t.elapsed());
        }


        let tx = conn.transaction()?;
        let update_rune_entries: Vec<&RuneEntryForUpdate> = changed_runes.values().collect();

        if !update_rune_entries.is_empty() {
            let t = Instant::now();
            let mut stmt = tx.prepare_cached("UPDATE rune_entry SET mintable = ?, mints = ?, burned = ?, holders = ?, transactions = ? WHERE rune_id = ?")?;
            for entry in &update_rune_entries {
                stmt.execute(params![
                    entry.mintable,
                    entry.mints,
                    entry.burned,
                    runes_holders.get(&entry.rune_id).unwrap_or(&0),
                    runes_txs.get(&entry.rune_id).unwrap_or(&0),
                    entry.rune_id,
                ])?;
            }
            info!("Updating {} rune entries in sqlite, {:?}", update_rune_entries.len(), t.elapsed());
        }

        tx.commit()?;
        info!("Write stage 4 done.");
        Ok(())
    }

    pub fn flush_rocksdb(&self) {
        self.rocksdb.flush_wal(true).unwrap();
        self.rocksdb.flush().unwrap();
    }


    pub fn to_sqlite(&self, rune_temp: RuneEntryForTemp, mut balance_temp: RuneBalanceForTemp) -> anyhow::Result<()> {
        let now = Instant::now();
        let mut conn = self.sqlite.get()?;
        let tx = conn.transaction()?;

        let mut need_update_runes = HashSet::new();

        let mut has_op = false;

        balance_temp.update_inserts();
        let insert_rune_balances: Vec<&RuneBalanceForInsert> = balance_temp.inserts.values().collect();
        if !insert_rune_balances.is_empty() {
            has_op = true;
            let t = Instant::now();
            for items in insert_rune_balances.chunks(1000) {
                let mut sql = String::from(
                    "INSERT INTO rune_balance(txid, vout, value, rune_id, rune_amount, address, premine, mint, burn, cenotaph, transfer, height, idx, ts, spent_height, spent_ts, spent_txid, spent_vin) VALUES ",
                );
                let mut values: Vec<&dyn ToSql> = Vec::new();
                let len = items.len();
                for (index, entry) in items.iter().enumerate() {
                    sql.push_str("(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)");
                    if index != len - 1 {
                        sql.push(',');
                    }
                    values.push(&entry.txid);
                    values.push(&entry.vout);
                    values.push(&entry.value);
                    values.push(&entry.rune_id);
                    values.push(&entry.rune_amount);
                    values.push(&entry.address);
                    values.push(&entry.premine);
                    values.push(&entry.mint);
                    values.push(&entry.burn);
                    values.push(&entry.cenotaph);
                    values.push(&entry.transfer);
                    values.push(&entry.height);
                    values.push(&entry.idx);
                    values.push(&entry.ts);
                    values.push(&entry.spent_height);
                    values.push(&entry.spent_ts);
                    values.push(&entry.spent_txid);
                    values.push(&entry.spent_vin);
                    need_update_runes.insert(entry.rune_id.clone());
                }
                tx.execute(&sql, values.as_slice())?;
            }
            info!("Inserting {} rune balances to sqlite, {:?}", insert_rune_balances.len(), t.elapsed());
        }

        let update_rune_balances: Vec<&RuneBalanceForUpdate> = balance_temp.updates.values().collect();
        if !update_rune_balances.is_empty() {
            has_op = true;
            let t = Instant::now();
            let mut stmt = tx.prepare_cached("UPDATE rune_balance SET spent_height = ?, spent_txid = ?, spent_vin = ?, spent_ts = ? WHERE txid = ? AND vout = ? AND rune_id = ?")?;
            for entry in &update_rune_balances {
                stmt.execute(params![
                    entry.spent_height,
                    entry.spent_txid,
                    entry.spent_vin,
                    entry.spent_ts,
                    entry.txid,
                    entry.vout,
                    entry.rune_id,
                ])?;
                need_update_runes.insert(entry.rune_id.clone());
            }
            info!("Updating {} rune balances in sqlite, {:?}", update_rune_balances.len(), t.elapsed());
        }

        tx.commit()?;

        for x in rune_temp.updates.values() {
            need_update_runes.insert(x.rune_id.clone());
        }
        for x in rune_temp.inserts.values() {
            if x.mints.parse::<u128>().unwrap() > 0 || x.premine.parse::<u128>().unwrap() > 0 || x.burned.parse::<u128>().unwrap() > 0 {
                need_update_runes.insert(x.rune_id.clone());
            }
        }
        let mut runes_txs = HashMap::new();
        let mut runes_holders = HashMap::new();
        if !need_update_runes.is_empty() {
            has_op = true;
            let t = Instant::now();
            let need_update_runes = need_update_runes.clone().into_iter().collect::<Vec<String>>();
            for sub in need_update_runes.chunks(100) {
                let placeholders = sub.iter().map(|_| "?").collect::<Vec<&str>>().join(",");
                let t = Instant::now();
                let sql = format!("SELECT rune_id, COUNT(DISTINCT _txid) AS txs FROM (SELECT rune_id, txid AS _txid FROM rune_balance where rune_id in ({}) UNION ALL SELECT rune_id, spent_txid AS _txid FROM rune_balance WHERE rune_id in ({}) AND spent_height > 0) AS _ GROUP BY rune_id", &placeholders, &placeholders);
                let mut stmt = conn.prepare_cached(&sql)?;
                stmt.query_map(params_from_iter(sub.iter().chain(sub.iter())), |row| {
                    Ok((row.get::<_, String>(0)?, row.get::<_, u32>(1)?))
                })?.for_each(|x| {
                    let (rune_id, txs) = x.unwrap();
                    runes_txs.insert(rune_id, txs);
                });
                info!("Querying {} runes txs from sqlite, {:?}", sub.len(), t.elapsed());
                let t = Instant::now();
                let sql = format!("SELECT rune_id, COUNT(DISTINCT address) AS addresses FROM rune_balance where rune_id in ({}) and spent_height = 0 GROUP BY rune_id", &placeholders);
                let mut stmt = conn.prepare_cached(&sql)?;
                stmt.query_map(params_from_iter(sub.iter()), |row| {
                    Ok((row.get::<_, String>(0)?, row.get::<_, u32>(1)?))
                })?.for_each(|x| {
                    let (rune_id, holders) = x.unwrap();
                    runes_holders.insert(rune_id, holders);
                });
                info!("Querying {} runes holders from sqlite, {:?}", sub.len(), t.elapsed());
            }
            info!("Querying {} runes txs and holders from sqlite, {:?}", need_update_runes.len(), t.elapsed());
        }


        let tx = conn.transaction()?;

        let mut used_rune_ids = HashSet::new();

        let insert_rune_entries: Vec<&RuneEntryForQueryInsert> = rune_temp.inserts.values().collect();
        if !insert_rune_entries.is_empty() {
            has_op = true;
            let t = Instant::now();
            for items in insert_rune_entries.chunks(500) {
                let mut sql = String::from(
                    "INSERT INTO rune_entry (rune_id, etching, number, rune, spaced_rune, symbol, divisibility, premine, amount, cap, start_height, end_height, start_offset, end_offset, turbo, fairmint, height, ts, mintable, mints, burned, holders, transactions) VALUES ",
                );
                let mut values: Vec<ToSqlOutput> = Vec::new();
                let len = items.len();
                for (index, entry) in items.iter().enumerate() {
                    sql.push_str("(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)");
                    if index != len - 1 {
                        sql.push(',');
                    }
                    values.push(entry.rune_id.to_sql()?);
                    values.push(entry.etching.to_sql()?);
                    values.push(entry.number.to_sql()?);
                    values.push(entry.rune.to_sql()?);
                    values.push(entry.spaced_rune.to_sql()?);
                    values.push(entry.symbol.to_sql()?);
                    values.push(entry.divisibility.to_sql()?);
                    values.push(entry.premine.to_sql()?);
                    values.push(entry.amount.to_sql()?);
                    values.push(entry.cap.to_sql()?);
                    values.push(entry.start_height.to_sql()?);
                    values.push(entry.end_height.to_sql()?);
                    values.push(entry.start_offset.to_sql()?);
                    values.push(entry.end_offset.to_sql()?);
                    values.push(entry.turbo.to_sql()?);
                    values.push(entry.fairmint.to_sql()?);
                    values.push(entry.height.to_sql()?);
                    values.push(entry.ts.to_sql()?);
                    values.push(entry.mintable.to_sql()?);
                    values.push(entry.mints.to_sql()?);
                    values.push(entry.burned.to_sql()?);
                    values.push(runes_holders.get(&entry.rune_id).unwrap_or(&0).to_sql()?);
                    values.push(runes_txs.get(&entry.rune_id).unwrap_or(&0).to_sql()?);
                    used_rune_ids.insert(entry.rune_id.clone());
                }
                tx.execute(&sql, params_from_iter(values.iter()))?;
            }
            info!("Inserting {} rune entries to sqlite, {:?}", insert_rune_entries.len(), t.elapsed());
        }

        let update_rune_entries: Vec<&RuneEntryForUpdate> = rune_temp.updates.values().collect();

        let t = Instant::now();
        let mut updated_rune_count = 0;
        if !update_rune_entries.is_empty() {
            has_op = true;
            let mut stmt = tx.prepare_cached("UPDATE rune_entry SET mintable = ?, mints = ?, burned = ?, holders = ?, transactions = ? WHERE rune_id = ?")?;
            for entry in &update_rune_entries {
                stmt.execute(params![
                    entry.mintable,
                    entry.mints,
                    entry.burned,
                    runes_holders.get(&entry.rune_id).unwrap_or(&0),
                    runes_txs.get(&entry.rune_id).unwrap_or(&0),
                    entry.rune_id,
                ])?;
                used_rune_ids.insert(entry.rune_id.clone());
                updated_rune_count += 1;
            }
        }

        {
            let mut stmt = tx.prepare_cached("UPDATE rune_entry SET holders = ?, transactions = ? WHERE rune_id = ?")?;
            for rune_id in need_update_runes {
                if used_rune_ids.contains(&rune_id) {
                    continue;
                }
                has_op = true;
                stmt.execute(params![
                    runes_holders.get(&rune_id).unwrap_or(&0),
                    runes_txs.get(&rune_id).unwrap_or(&0),
                    rune_id,
                ])?;
                updated_rune_count += 1;
            }
        }

        if updated_rune_count > 0 {
            info!("Updating {} rune entries in sqlite, {:?}", updated_rune_count, t.elapsed());
        }


        tx.commit()?;

        if has_op {
            info!("Sqlite updated, {:?}", now.elapsed());
        }

        Ok(())
    }
}