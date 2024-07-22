use std::collections::{HashMap, HashSet};
use std::time::Duration;

use bitcoin::{OutPoint, ScriptBuf, Transaction, Txid};
use bitcoincore_rpc::{Client, RpcApi};
use log::info;

use ordinals::*;

use crate::db::RunesDB;
use crate::entry::*;
use crate::into_usize::IntoUsize;
use crate::lot::*;
use crate::rpc::with_retry;

pub type Result<T = (), E = anyhow::Error> = std::result::Result<T, E>;

pub const REORG_DEPTH: u32 = 10;

pub struct RuneUpdater<'a, > {
    pub block_time: u32,
    pub burned: HashMap<RuneId, Lot>,
    pub client: &'a Client,
    pub height: u32,
    pub minimum: Rune,
    pub runes: u32,
    pub runes_db: &'a RunesDB,
    pub block_spks: &'a mut HashSet<ScriptBuf>,
    pub block_outpoints: &'a mut HashSet<OutPoint>,
}

impl<'a> RuneUpdater<'a> {
    pub async fn index_runes(
        &mut self,
        tx_index: u32,
        tx: &Transaction,
    ) -> Result<()> {
        let txid = tx.txid();
        let artifact = Runestone::decipher(tx);

        let mut unallocated = self.unallocated(tx)?;

        let mut allocated: Vec<HashMap<RuneId, Lot>> = vec![HashMap::new(); tx.output.len()];

        if let Some(artifact) = &artifact {
            if let Some(id) = artifact.mint() {
                if let Some(amount) = self.mint(id)? {
                    *unallocated.entry(id).or_default() += amount;
                }
            }

            let etched = self.etched(tx_index, tx, artifact).await?;

            if let Artifact::Runestone(runestone) = artifact {
                if let Some((id, ..)) = etched {
                    *unallocated.entry(id).or_default() +=
                        runestone.etching.unwrap().premine.unwrap_or_default();
                }

                for Edict { id, amount, output } in runestone.edicts.iter().copied() {
                    let amount = Lot(amount);

                    // edicts with output values greater than the number of outputs
                    // should never be produced by the edict parser
                    let output = usize::try_from(output).unwrap();
                    assert!(output <= tx.output.len());

                    let id = if id == RuneId::default() {
                        let Some((id, ..)) = etched else {
                            continue;
                        };

                        id
                    } else {
                        id
                    };

                    let Some(balance) = unallocated.get_mut(&id) else {
                        continue;
                    };

                    let mut allocate = |balance: &mut Lot, amount: Lot, output: usize| {
                        if amount > 0 {
                            *balance -= amount;
                            *allocated[output].entry(id).or_default() += amount;
                        }
                    };

                    if output == tx.output.len() {
                        // find non-OP_RETURN outputs
                        let destinations = tx
                            .output
                            .iter()
                            .enumerate()
                            .filter_map(|(output, tx_out)| {
                                (!tx_out.script_pubkey.is_op_return()).then_some(output)
                            })
                            .collect::<Vec<usize>>();

                        if !destinations.is_empty() {
                            if amount == 0 {
                                // if amount is zero, divide balance between eligible outputs
                                let amount = *balance / destinations.len() as u128;
                                let remainder = usize::try_from(*balance % destinations.len() as u128).unwrap();

                                for (i, output) in destinations.iter().enumerate() {
                                    allocate(
                                        balance,
                                        if i < remainder { amount + 1 } else { amount },
                                        *output,
                                    );
                                }
                            } else {
                                // if amount is non-zero, distribute amount to eligible outputs
                                for output in destinations {
                                    allocate(balance, amount.min(*balance), output);
                                }
                            }
                        }
                    } else {
                        // Get the allocatable amount
                        let amount = if amount == 0 {
                            *balance
                        } else {
                            amount.min(*balance)
                        };

                        allocate(balance, amount, output);
                    }
                }
            }

            if let Some((id, rune)) = etched {
                self.create_rune_entry(txid, artifact, id, rune)?;
            }
        }

        let mut burned: HashMap<RuneId, Lot> = HashMap::new();

        if let Some(Artifact::Cenotaph(_)) = artifact {
            for (id, balance) in unallocated {
                *burned.entry(id).or_default() += balance;
            }
        } else {
            let pointer = artifact
                .map(|artifact| match artifact {
                    Artifact::Runestone(runestone) => runestone.pointer,
                    Artifact::Cenotaph(_) => unreachable!(),
                })
                .unwrap_or_default();

            // assign all un-allocated runes to the default output, or the first non
            // OP_RETURN output if there is no default
            if let Some(vout) = pointer
                .map(|pointer| pointer.into_usize())
                .inspect(|&pointer| assert!(pointer < allocated.len()))
                .or_else(|| {
                    tx.output
                        .iter()
                        .enumerate()
                        .find(|(_vout, tx_out)| !tx_out.script_pubkey.is_op_return())
                        .map(|(vout, _tx_out)| vout)
                })
            {
                for (id, balance) in unallocated {
                    if balance > 0 {
                        *allocated[vout].entry(id).or_default() += balance;
                    }
                }
            } else {
                for (id, balance) in unallocated {
                    if balance > 0 {
                        *burned.entry(id).or_default() += balance;
                    }
                }
            }
        }

        // update outpoint balances
        let mut buffer: Vec<u8> = Vec::new();
        for (vout, balances) in allocated.into_iter().enumerate() {
            if balances.is_empty() {
                continue;
            }

            // increment burned balances
            if tx.output[vout].script_pubkey.is_op_return() {
                for (id, balance) in &balances {
                    *burned.entry(*id).or_default() += *balance;
                }
                continue;
            }

            buffer.clear();

            let mut balances = balances.into_iter().collect::<Vec<(RuneId, Lot)>>();

            // Sort balances by id so tests can assert balances in a fixed order
            balances.sort();

            let outpoint = OutPoint {
                txid,
                vout: vout.try_into().unwrap(),
            };


            for (id, balance) in balances {
                Self::encode_rune_balance(id, balance.n(), &mut buffer);
            }

            let sat = tx.output[vout].value.to_sat();
            let balance: RuneBalanceEntry = (self.height, 0, sat, tx.output[vout].script_pubkey.to_bytes(), buffer.clone());
            self.runes_db.outpoint_to_rune_balances_put(&outpoint, balance);
            self.runes_db.spk_outpoint_to_spent_height_put(&tx.output[vout].script_pubkey, &outpoint);
            
            self.block_outpoints.insert(outpoint);
        }

        // increment entries with burned runes
        for (id, amount) in burned {
            *self.burned.entry(id).or_default() += amount;
        }

        Ok(())
    }

    pub fn update(&self) -> Result {
        for (rune_id, burned) in &self.burned {
            let mut entry = self.runes_db.rune_id_to_rune_entry_get(rune_id).unwrap();
            self.runes_db.rune_id_height_to_burned_put(rune_id, self.height, burned.n());
            entry.burned = self.runes_db.rune_id_to_burned_inc(rune_id);
            self.runes_db.rune_id_to_rune_entry_put(rune_id, &entry);
        }
        Ok(())
    }

    pub fn runes_num(&self) -> u32 {
        self.runes
    }

    fn create_rune_entry(
        &mut self,
        txid: Txid,
        artifact: &Artifact,
        id: RuneId,
        rune: Rune,
    ) -> Result {
        self.runes_db.rune_to_rune_id_put(&rune, &id);

        let number: u64 = self.runes as _;
        self.runes += 1;

        self.runes_db.statistic_to_value_put(&Statistic::Runes, self.runes);

        let entry = match artifact {
            Artifact::Cenotaph(_) => RuneEntry {
                block: id.block,
                burned: 0,
                divisibility: 0,
                etching: txid,
                terms: None,
                mints: 0,
                number,
                premine: 0,
                spaced_rune: SpacedRune { rune, spacers: 0 },
                symbol: None,
                timestamp: self.block_time.into(),
                turbo: false,
            },
            Artifact::Runestone(Runestone { etching, .. }) => {
                let Etching {
                    divisibility,
                    terms,
                    premine,
                    spacers,
                    symbol,
                    turbo,
                    ..
                } = etching.unwrap();

                RuneEntry {
                    block: id.block,
                    burned: 0,
                    divisibility: divisibility.unwrap_or_default(),
                    etching: txid,
                    terms,
                    mints: 0,
                    number,
                    premine: premine.unwrap_or_default(),
                    spaced_rune: SpacedRune {
                        rune,
                        spacers: spacers.unwrap_or_default(),
                    },
                    symbol,
                    timestamp: self.block_time.into(),
                    turbo,
                }
            }
        };

        self.runes_db.rune_id_to_rune_entry_put(&id, &entry);
        info!("New RUNE: {}({}, {})", entry.spaced_rune, id, number);
        Ok(())
    }

    async fn etched(
        &mut self,
        tx_index: u32,
        tx: &Transaction,
        artifact: &Artifact,
    ) -> Result<Option<(RuneId, Rune)>> {
        let rune = match artifact {
            Artifact::Runestone(runestone) => match runestone.etching {
                Some(etching) => etching.rune,
                None => return Ok(None),
            },
            Artifact::Cenotaph(cenotaph) => match cenotaph.etching {
                Some(rune) => Some(rune),
                None => return Ok(None),
            },
        };

        let rune = if let Some(rune) = rune {
            if rune < self.minimum
                || rune.is_reserved()
                || self.runes_db.rune_to_rune_id_get(&rune).is_some()
                || !self.tx_commits_to_rune(tx, rune).await?
            {
                return Ok(None);
            }
            rune
        } else {
            self
                .runes_db.height_to_statistic_count_inc(&Statistic::ReservedRunes, self.height);
            self.runes_db.statistic_to_value_inc(&Statistic::ReservedRunes);
            Rune::reserved(self.height.into(), tx_index)
        };

        Ok(Some((
            RuneId {
                block: self.height.into(),
                tx: tx_index,
            },
            rune,
        )))
    }

    fn mint(&mut self, id: RuneId) -> Result<Option<Lot>> {
        let Some(entry) = self.runes_db.rune_id_to_rune_entry_get(&id) else {
            return Ok(None);
        };

        let mut rune_entry = entry;

        let Ok(amount) = rune_entry.mintable(self.height.into()) else {
            return Ok(None);
        };

        self.runes_db.rune_id_height_to_mints_inc(&id, self.height);

        rune_entry.mints = self.runes_db.rune_id_to_mints_inc(&id);

        self.runes_db.rune_id_to_rune_entry_put(&id, &rune_entry);

        Ok(Some(Lot(amount)))
    }

    async fn tx_commits_to_rune(&self, tx: &Transaction, rune: Rune) -> Result<bool> {
        let commitment = rune.commitment();

        for input in &tx.input {
            // extracting a tapscript does not indicate that the input being spent
            // was actually a taproot output. this is checked below, when we load the
            // output's entry from the database
            let Some(tapscript) = input.witness.tapscript() else {
                continue;
            };

            for instruction in tapscript.instructions() {
                // ignore errors, since the extracted script may not be valid
                let Ok(instruction) = instruction else {
                    break;
                };

                let Some(pushbytes) = instruction.push_bytes() else {
                    continue;
                };

                if pushbytes.as_bytes() != commitment {
                    continue;
                }

                let previus_txid = input.previous_output.txid;
                let Some(tx_info) = with_retry(|| match self
                    .client
                    .get_raw_transaction_info(&previus_txid, None)
                    .into_option() {
                    Ok(v) => Ok(v),
                    Err(e) => Err(e)
                }, 5, Duration::from_millis(100)).await.unwrap()
                else {
                    panic!(
                        "can't get input transaction: {}",
                        previus_txid
                    );
                };


                let taproot = tx_info.vout[input.previous_output.vout.into_usize()]
                    .script_pub_key
                    .script()?
                    .is_p2tr();

                if !taproot {
                    continue;
                }

                let commit_tx_height = self
                    .client
                    .get_block_header_info(&tx_info.blockhash.unwrap())
                    .into_option()?
                    .unwrap()
                    .height;

                let confirmations = self
                    .height
                    .checked_sub(commit_tx_height.try_into().unwrap())
                    .unwrap()
                    + 1;

                if confirmations >= Runestone::COMMIT_CONFIRMATIONS.into() {
                    return Ok(true);
                }
            }
        }

        Ok(false)
    }

    fn unallocated(&mut self, tx: &Transaction) -> Result<HashMap<RuneId, Lot>> {
        // map of rune ID to un-allocated balance of that rune
        let mut unallocated: HashMap<RuneId, Lot> = HashMap::new();

        // increment unallocated runes with the runes in tx inputs
        for input in &tx.input {
            if let Some(mut entry) = self
                .runes_db.outpoint_to_rune_balances_get(&input.previous_output)
            {
                let buffer = &entry.4;
                let mut i = 0;
                while i < buffer.len() {
                    let ((id, balance), len) = Self::decode_rune_balance(&buffer[i..]).unwrap();
                    i += len;
                    *unallocated.entry(id).or_default() += balance;
                }

                let spk = ScriptBuf::from_bytes(entry.3.to_vec());
                self.runes_db.spk_outpoint_to_spent_height_spent(&spk, &input.previous_output, self.height);

                entry.1 = self.height;
                self.runes_db.outpoint_to_rune_balances_put(&input.previous_output, entry);

                self.block_spks.insert(spk);
                self.block_outpoints.insert(input.previous_output);
            }
        }

        Ok(unallocated)
    }


    pub fn encode_rune_balance(id: RuneId, balance: u128, buffer: &mut Vec<u8>) {
        varint::encode_to_vec(id.block.into(), buffer);
        varint::encode_to_vec(id.tx.into(), buffer);
        varint::encode_to_vec(balance, buffer);
    }

    pub fn decode_rune_balance(buffer: &[u8]) -> Result<((RuneId, u128), usize)> {
        let mut len = 0;
        let (block, block_len) = varint::decode(&buffer[len..])?;
        len += block_len;
        let (tx, tx_len) = varint::decode(&buffer[len..])?;
        len += tx_len;
        let id = RuneId {
            block: block.try_into()?,
            tx: tx.try_into()?,
        };
        let (balance, balance_len) = varint::decode(&buffer[len..])?;
        len += balance_len;
        Ok(((id, balance), len))
    }
}

#[cfg(test)]
mod tests {
    use crate::updater::RuneUpdater;

    #[test]
    fn test_combine_vec() {
        let original_vec: Vec<u8> = vec![1, 2, 3, 4];
        let number: u64 = 123456789;
        let mut combined_vec = number.to_be_bytes().to_vec();
        combined_vec.extend_from_slice(&original_vec);
        let (number_bytes, original_vec_restored) = combined_vec.split_at(8);
        let number_restored = u64::from_be_bytes(number_bytes.try_into().unwrap());

        println!("number: {}", number);
        println!("number_restored: {}", number_restored);
        println!("original_vec: {:?}", original_vec);
        println!("original_vec_restored: {:?}", original_vec_restored);
    }
    #[test]
    fn test_decode_balance() {
        let combined_vec: [u8; 17] = [0, 0, 0, 0, 0, 0, 39, 16, 190, 233, 157, 1, 43, 160, 150, 128, 1];
        let (number_bytes, original_vec_restored) = combined_vec.split_at(8);
        let number_restored = u64::from_be_bytes(number_bytes.try_into().unwrap());
        println!("sat: {}", number_restored);
        let mut i = 0;
        while i < original_vec_restored.len() {
            let ((id, balance), len) = RuneUpdater::decode_rune_balance(&original_vec_restored[i..]).unwrap();
            println!("id: {:?}, balance: {}", id, balance);
            i += len;
        }
    }
}
