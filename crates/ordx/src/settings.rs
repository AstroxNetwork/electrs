use std::{env, fmt};
use std::fmt::{Display, Formatter};

use config::Config;
use dotenv::dotenv;
use serde_derive::{Deserialize, Serialize};

#[derive(Default, Clone, Serialize, Deserialize, PartialEq)]
pub struct Settings {
    pub network: Option<String>,
    pub data_dir: Option<String>,
    pub bitcoin_rpc_url: Option<String>,
    pub bitcoin_rpc_username: Option<String>,
    pub bitcoin_rpc_password: Option<String>,
    pub max_block_queue_size: Option<u8>,
}

impl Display for Settings {
    fn fmt(&self, f: &mut Formatter) -> fmt::Result {
        write!(f, "Settings from env: \n\
        ========================================\n\
        network: {}\n\
        data_dir: {}\n\
        bitcoin_rpc_url: {}\n\
        bitcoin_rpc_username: {}\n\
        bitcoin_rpc_password: {} \n\
        max_block_queue_size: {}\n\
        build_version: {}\n\
        build_timestamp: {}\n\
        target_triple: {}\n\
        rustc_semver: {}\n\
        ========================================",
               self.network.clone().unwrap_or_default(),
               self.data_dir.clone().unwrap_or_default(),
               self.bitcoin_rpc_url.clone().unwrap_or_default(),
               self.bitcoin_rpc_username.as_ref().map(|_| "***").unwrap_or_default(),
               self.bitcoin_rpc_password.as_ref().map(|_| "********").unwrap_or_default(),
               self.max_block_queue_size.map(|x| x.to_string()).unwrap_or_default(),
               env!("CARGO_PKG_VERSION"),
               env!("VERGEN_BUILD_TIMESTAMP"),
               env!("VERGEN_CARGO_TARGET_TRIPLE"),
               env!("VERGEN_RUSTC_SEMVER"),
        )
    }
}

impl Settings {
    pub fn load() -> Self {
        dotenv().ok();
        let config = Config::builder()
            .add_source(
                config::Environment::default()
            )
            .build()
            .unwrap();
        config.try_deserialize().unwrap()
    }
}