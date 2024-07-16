use base64::Engine;
use base64::engine::general_purpose::STANDARD;

pub fn hex_to_base64(hex_str: &str) -> Result<String, hex::FromHexError> {
    let bytes = hex::decode(hex_str)?;
    let base64_str = STANDARD.encode(bytes);
    Ok(base64_str)
}