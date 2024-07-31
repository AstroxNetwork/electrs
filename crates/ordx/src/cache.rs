use std::hash::Hash;
use std::time::Duration;

use moka::future::Cache;
use serde_json::Value;

use crate::settings::Settings;

#[derive(Debug, Clone)]
pub struct CacheKey(pub CacheMethod, pub Value);

#[derive(Debug, Clone, Hash, Eq, PartialEq)]
pub enum CacheMethod {
    HandlerAddressUtxos = 0,
    CompatAddressUtxos = 1,
    HandlerPagedRunes = 2,
}

impl CacheKey {
    pub fn new(method: CacheMethod, params: Value) -> Self {
        Self(method, params)
    }
}

impl Hash for CacheKey {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.0.hash(state);
        self.1.hash(state);
    }
}

impl PartialEq for CacheKey {
    fn eq(&self, other: &Self) -> bool {
        self.0 == other.0 && self.1 == other.1
    }
}

impl Eq for CacheKey {}

pub type MokaCache = Cache<CacheKey, Value>;

pub fn create_cache(settings: &Settings) -> MokaCache {
    Cache::builder()
        .max_capacity(settings.cache_max_entries)
        .time_to_live(Duration::from_secs(settings.cache_time_to_live_secs))
        .time_to_idle(Duration::from_secs(settings.cache_time_to_idle_secs))
        .build()
}

