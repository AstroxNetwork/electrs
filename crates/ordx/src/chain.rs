use std::fmt;
use std::fmt::{Display, Formatter};
use std::path::{Path, PathBuf};
use std::str::FromStr;

use anyhow::{bail, Error};
use bitcoin::{Address, Block, Network, OutPoint, Script};
use serde::{Deserialize, Serialize};

use ordinals::Rune;

#[derive(Default, Copy, Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum Chain {
    #[default]
    Mainnet,
    Testnet,
    Testnet4,
    Signet,
    Regtest,
}

impl Chain {
    pub fn network(self) -> Network {
        self.into()
    }

    pub fn default_rpc_port(self) -> u16 {
        match self {
            Self::Mainnet => 8332,
            Self::Regtest => 18443,
            Self::Signet => 38332,
            Self::Testnet => 18332,
            Self::Testnet4 => 28332,
        }
    }

    pub fn inscription_content_size_limit(self) -> Option<usize> {
        match self {
            Self::Mainnet | Self::Regtest => None,
            Self::Testnet | Self::Testnet4 | Self::Signet => Some(1024),
        }
    }

    pub fn first_inscription_height(self) -> u32 {
        match self {
            Self::Mainnet => 767430,
            Self::Regtest => 0,
            Self::Signet => 112402,
            Self::Testnet => 2413343,
            Self::Testnet4 => 0,
        }
    }

    pub fn first_rune_height(self) -> u32 {
        Rune::first_rune_height(self.into())
    }

    pub fn jubilee_height(self) -> u32 {
        match self {
            Self::Mainnet => 824544,
            Self::Regtest => 110,
            Self::Signet => 175392,
            Self::Testnet => 2544192,
            Self::Testnet4 => 0,
        }
    }

    pub fn genesis_block(self) -> Block {
        bitcoin::blockdata::constants::genesis_block(self.network())
    }

    pub fn genesis_coinbase_outpoint(self) -> OutPoint {
        OutPoint {
            txid: self.genesis_block().coinbase().unwrap().txid(),
            vout: 0,
        }
    }

    pub fn address_from_script(
        self,
        script: &Script,
    ) -> Result<Address, bitcoin::address::Error> {
        Address::from_script(script, self.network())
    }

    pub fn join_with_data_dir(self, data_dir: impl AsRef<Path>) -> PathBuf {
        match self {
            Self::Mainnet => data_dir.as_ref().to_owned(),
            Self::Testnet => data_dir.as_ref().join("testnet3"),
            Self::Testnet4 => data_dir.as_ref().join("testnet4"),
            Self::Signet => data_dir.as_ref().join("signet"),
            Self::Regtest => data_dir.as_ref().join("regtest"),
        }
    }
}

impl From<Chain> for Network {
    fn from(chain: Chain) -> Network {
        match chain {
            Chain::Mainnet => Network::Bitcoin,
            Chain::Testnet => Network::Testnet,
            Chain::Testnet4 => Network::Testnet,
            Chain::Signet => Network::Signet,
            Chain::Regtest => Network::Regtest,
        }
    }
}

impl Display for Chain {
    fn fmt(&self, f: &mut Formatter) -> fmt::Result {
        write!(
            f,
            "{}",
            match self {
                Self::Mainnet => "mainnet",
                Self::Regtest => "regtest",
                Self::Signet => "signet",
                Self::Testnet => "testnet",
                Self::Testnet4 => "testnet4",
            }
        )
    }
}

impl FromStr for Chain {
    type Err = Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "mainnet" | "main" => Ok(Self::Mainnet),
            "regtest" => Ok(Self::Regtest),
            "signet" => Ok(Self::Signet),
            "testnet" | "test" => Ok(Self::Testnet),
            "testnet4" | "test4" => Ok(Self::Testnet4),
            _ => bail!("invalid chain `{s}`"),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn from_str() {
        assert_eq!("mainnet".parse::<Chain>().unwrap(), Chain::Mainnet);
        assert_eq!("regtest".parse::<Chain>().unwrap(), Chain::Regtest);
        assert_eq!("signet".parse::<Chain>().unwrap(), Chain::Signet);
        assert_eq!("testnet".parse::<Chain>().unwrap(), Chain::Testnet);
        assert_eq!(
            "foo".parse::<Chain>().unwrap_err().to_string(),
            "invalid chain `foo`"
        );
    }
}
