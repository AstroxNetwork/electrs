use std::fmt;
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
    pub cargo_pkg_version: String,
    pub vergen_build_timestamp: String,
    pub vergen_cargo_target_triple: String,
    pub vergen_rustc_semver: String,
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
        cargo_pkg_version: {}\n\
        vergen_build_timestamp: {}\n\
        vergen_cargo_target_triple: {}\n\
        vergen_rustc_semver: {}\n\
        ========================================", self.network.clone().unwrap_or_default(), self.data_dir.clone().unwrap_or_default(), self.bitcoin_rpc_url.clone().unwrap_or_default(), self.bitcoin_rpc_username.as_ref().map(|_| "***").unwrap_or_default(), self.bitcoin_rpc_password.as_ref().map(|_| "********").unwrap_or_default(), self.max_block_queue_size.map(|x| x.to_string()).unwrap_or_default(), self.cargo_pkg_version,
               self.vergen_build_timestamp,
               self.vergen_cargo_target_triple,
               self.vergen_rustc_semver)
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