pub const MAX_KEY_LEN: usize = 250;

pub fn validate_key(key: &[u8]) -> Result<(), ()> {
    if key.is_empty() || key.len() > MAX_KEY_LEN {
        return Err(());
    }

    if key.iter().any(|&b| b <= 0x20 || b == 0x7F) {
        return Err(());
    }

    Ok(())
}

pub fn parse_field<T: std::str::FromStr>(tok: &[u8]) -> Result<T, ()> {
    std::str::from_utf8(tok)
        .ok()
        .and_then(|s| s.parse().ok())
        .ok_or(())
}
