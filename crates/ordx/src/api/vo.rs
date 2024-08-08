#[derive(Debug, Clone, Hash, Eq, PartialEq)]
pub struct RuneBalanceGroupKey {
    pub txid: String,
    pub vout: u32,
}