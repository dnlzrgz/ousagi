use atoi::FromRadix10SignedChecked;
use bytes::Bytes;

use crate::commands::{ArithmeticOp, Command, Response, StoreArgs, StoreOp};

const MAX_KEY_LEN: usize = 250;
const MAX_ITEM_SIZE: usize = 1024 * 1024; // 1 MiB

#[derive(Debug)]
pub enum ParseErrorKind {
    BadFormat,
    NumericDelta,
    NumericDelay,
    TooLarge,
    Unknown,
}

#[derive(Debug)]
pub struct ParseError {
    kind: ParseErrorKind,
    discard: Option<usize>,
}

impl ParseError {
    fn new(kind: ParseErrorKind) -> Self {
        Self {
            kind,
            discard: None,
        }
    }

    fn with_discard(mut self, n: usize) -> Self {
        self.discard = Some(n);
        self
    }

    pub fn discard(&self) -> Option<usize> {
        self.discard
    }

    pub fn response(&self) -> Response {
        match self.kind {
            ParseErrorKind::BadFormat => Response::ClientError("bad command line format"),
            ParseErrorKind::NumericDelta => Response::ClientError("invalid numeric delta"),
            ParseErrorKind::NumericDelay => Response::ClientError("invalid numeric delay"),
            ParseErrorKind::TooLarge => Response::ServerError("object too large for cache"),
            ParseErrorKind::Unknown => Response::Error,
        }
    }
}

#[derive(Debug)]
pub struct PendingStore {
    pub op: StoreOp,
    pub key: Bytes,
    pub flags: u32,
    pub exptime: i64,
    pub len: usize,
    pub cas: Option<u64>,
    pub noreply: bool,
}

impl PendingStore {
    pub fn into_command(self, data: Bytes) -> Command {
        Command::Store(
            self.op,
            StoreArgs {
                key: self.key,
                flags: self.flags,
                exptime: self.exptime,
                data,
                noreply: self.noreply,
                cas: self.cas,
            },
        )
    }
}

#[derive(Debug)]
pub enum CommandHeader {
    Immediate(Command),
    Store(PendingStore),
}

#[derive(Debug)]
pub struct Tokenizer<'a> {
    line: &'a Bytes,
    rest: &'a [u8],
}

impl<'a> Tokenizer<'a> {
    pub fn new(line: &'a Bytes) -> Self {
        Self {
            line,
            rest: line.as_ref(),
        }
    }

    /// Helper to extract and validate zero-copy keys.
    #[inline]
    pub fn extract_key(&self, key: &[u8]) -> Result<Bytes, ParseError> {
        validate_key(key).map_err(|_| ParseError::new(ParseErrorKind::BadFormat))?;
        Ok(self.line.slice_ref(key))
    }
}

impl<'a> Iterator for Tokenizer<'a> {
    type Item = &'a [u8];

    #[inline]
    fn next(&mut self) -> Option<Self::Item> {
        while let Some((&b' ', rem)) = self.rest.split_first() {
            self.rest = rem;
        }

        if self.rest.is_empty() {
            return None;
        }

        let pos = self
            .rest
            .iter()
            .position(|&b| b == b' ')
            .unwrap_or(self.rest.len());
        let token = &self.rest[..pos];
        self.rest = &self.rest[pos..];
        Some(token)
    }
}

pub fn parse_command_line(line: &Bytes) -> Result<CommandHeader, ParseError> {
    let mut tokenizer = Tokenizer::new(line);

    let op = match tokenizer.next() {
        Some(op) => op,
        None => return Err(ParseError::new(ParseErrorKind::Unknown)),
    };

    match op {
        b"get" | b"gets" => parse_get(op == b"gets", &mut tokenizer),
        b"add" => parse_store(StoreOp::Add, &mut tokenizer),
        b"set" => parse_store(StoreOp::Set, &mut tokenizer),
        b"replace" => parse_store(StoreOp::Replace, &mut tokenizer),
        b"append" => parse_store(StoreOp::Append, &mut tokenizer),
        b"prepend" => parse_store(StoreOp::Prepend, &mut tokenizer),
        b"cas" => parse_store(StoreOp::Cas, &mut tokenizer),
        b"delete" => parse_delete(&mut tokenizer),
        b"incr" => parse_arithmetic(ArithmeticOp::Incr, &mut tokenizer),
        b"decr" => parse_arithmetic(ArithmeticOp::Decr, &mut tokenizer),
        b"flush_all" => parse_flush_all(&mut tokenizer),
        _ => Err(ParseError::new(ParseErrorKind::Unknown)),
    }
}

#[inline]
fn validate_key(key: &[u8]) -> Result<(), ()> {
    if key.is_empty() || key.len() > MAX_KEY_LEN {
        return Err(());
    }

    if key.iter().any(|&b| (b <= 0x20) | (b == 0x7F)) {
        return Err(());
    }

    Ok(())
}

/// Parse a field (e.g. flags) directly from ASCII bytes.
#[inline]
fn parse_field<T: FromRadix10SignedChecked>(tok: &[u8]) -> Result<T, ()> {
    let (value, used) = T::from_radix_10_signed_checked(tok);
    if used == tok.len() {
        value.ok_or(())
    } else {
        Err(())
    }
}

#[inline]
fn parse_noreply(tokenizer: &mut Tokenizer) -> Result<bool, ParseError> {
    match tokenizer.next() {
        None => Ok(false),
        Some(b"noreply") if tokenizer.next().is_none() => Ok(true),
        _ => Err(ParseError::new(ParseErrorKind::BadFormat)),
    }
}

fn parse_get(with_cas: bool, tokenizer: &mut Tokenizer) -> Result<CommandHeader, ParseError> {
    let first_key = match tokenizer.next() {
        Some(k) => k,
        None => return Err(ParseError::new(ParseErrorKind::Unknown)),
    };

    let mut keys = Vec::with_capacity(6);
    keys.push(tokenizer.extract_key(first_key)?);
    while let Some(k) = tokenizer.next() {
        keys.push(tokenizer.extract_key(k)?);
    }

    Ok(CommandHeader::Immediate(Command::Get { keys, with_cas }))
}

fn parse_store(op: StoreOp, tokenizer: &mut Tokenizer) -> Result<CommandHeader, ParseError> {
    let (key, flags, exptime, len) = match (
        tokenizer.next(),
        tokenizer.next(),
        tokenizer.next(),
        tokenizer.next(),
    ) {
        (Some(key), Some(flags), Some(exptime), Some(len)) => (key, flags, exptime, len),
        _ => return Err(ParseError::new(ParseErrorKind::Unknown)),
    };

    let key = tokenizer.extract_key(key)?;
    let flags =
        parse_field::<u32>(flags).map_err(|_| ParseError::new(ParseErrorKind::BadFormat))?;
    let exptime =
        parse_field::<i64>(exptime).map_err(|_| ParseError::new(ParseErrorKind::BadFormat))?;
    let len = parse_field::<usize>(len).map_err(|_| ParseError::new(ParseErrorKind::BadFormat))?;

    if len > MAX_ITEM_SIZE {
        return Err(ParseError::new(ParseErrorKind::TooLarge).with_discard(len + 2));
    }

    let cas = if op == StoreOp::Cas {
        match tokenizer.next() {
            Some(cas_token) => {
                Some(parse_field::<u64>(cas_token).map_err(|_| {
                    ParseError::new(ParseErrorKind::BadFormat).with_discard(len + 2)
                })?)
            }
            None => return Err(ParseError::new(ParseErrorKind::BadFormat).with_discard(len + 2)),
        }
    } else {
        None
    };

    let noreply = parse_noreply(tokenizer).map_err(|e| e.with_discard(len + 2))?;

    Ok(CommandHeader::Store(PendingStore {
        op,
        key,
        flags,
        exptime,
        len,
        cas,
        noreply,
    }))
}

fn parse_delete(tokenizer: &mut Tokenizer) -> Result<CommandHeader, ParseError> {
    let key = match tokenizer.next() {
        Some(key) => key,
        None => return Err(ParseError::new(ParseErrorKind::Unknown)),
    };

    let key = tokenizer.extract_key(key)?;
    let noreply = parse_noreply(tokenizer)?;

    Ok(CommandHeader::Immediate(Command::Delete { key, noreply }))
}

fn parse_arithmetic(
    op: ArithmeticOp,
    tokenizer: &mut Tokenizer,
) -> Result<CommandHeader, ParseError> {
    let (key, delta) = match (tokenizer.next(), tokenizer.next()) {
        (Some(key), Some(delta)) => (key, delta),
        _ => return Err(ParseError::new(ParseErrorKind::Unknown)),
    };

    let key = tokenizer.extract_key(key)?;
    let delta =
        parse_field::<u64>(delta).map_err(|_| ParseError::new(ParseErrorKind::NumericDelta))?;
    let noreply = parse_noreply(tokenizer)?;

    Ok(CommandHeader::Immediate(Command::Arithmetic {
        op,
        key,
        delta,
        noreply,
    }))
}

fn parse_flush_all(tokenizer: &mut Tokenizer) -> Result<CommandHeader, ParseError> {
    let (delay, noreply) = match tokenizer.next() {
        None => (None, false),
        Some(b"noreply") => {
            if tokenizer.next().is_some() {
                return Err(ParseError::new(ParseErrorKind::BadFormat));
            }
            (None, true)
        }
        Some(d) => {
            let delay =
                parse_field::<u32>(d).map_err(|_| ParseError::new(ParseErrorKind::NumericDelay))?;
            let noreply = parse_noreply(tokenizer)?;
            (Some(delay), noreply)
        }
    };

    Ok(CommandHeader::Immediate(Command::FlushAll {
        delay,
        noreply,
    }))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn line(s: &str) -> Bytes {
        Bytes::copy_from_slice(s.as_bytes())
    }

    #[test]
    fn get_single_key_parses_correctly() {
        let header = parse_command_line(&line("get foo")).expect("should parse correctly");

        match header {
            CommandHeader::Immediate(Command::Get { keys, with_cas }) => {
                assert_eq!(keys, vec![Bytes::from_static(b"foo")]);
                assert!(!with_cas);
            }
            _ => panic!("expected Get command without cas"),
        }
    }

    #[test]
    fn get_multiple_keys_parses_correctly() {
        let header = parse_command_line(&line("get foo bar baz")).expect("should parse correctly");

        match header {
            CommandHeader::Immediate(Command::Get { keys, with_cas }) => {
                assert_eq!(
                    keys,
                    vec![
                        Bytes::from_static(b"foo"),
                        Bytes::from_static(b"bar"),
                        Bytes::from_static(b"baz")
                    ]
                );
                assert!(!with_cas);
            }
            _ => panic!("expected Get command without cas"),
        }
    }

    #[test]
    fn get_with_no_keys_fails() {
        let err = parse_command_line(&line("get")).unwrap_err();
        assert!(matches!(err.kind, ParseErrorKind::Unknown));
    }

    #[test]
    fn set_returns_pendig_store_with_fields_parsed_correctly() {
        let header = parse_command_line(&line("set foo 42 0 5")).expect("should parse correctly");

        match header {
            CommandHeader::Store(pending) => {
                assert!(matches!(pending.op, StoreOp::Set));
                assert_eq!(pending.key, Bytes::from_static(b"foo"));
                assert_eq!(pending.flags, 42);
                assert_eq!(pending.len, 5);
                assert!(!pending.noreply)
            }
            CommandHeader::Immediate(_) => panic!("expected a pending store"),
        }
    }

    #[test]
    fn set_item_larger_than_max_item_size_fails() {
        let command = format!("set foo 0 0 {}", MAX_ITEM_SIZE + 1);
        let err = parse_command_line(&line(&command)).unwrap_err();

        assert!(matches!(err.kind, ParseErrorKind::TooLarge));
        assert_eq!(err.discard(), Some(MAX_ITEM_SIZE + 1 + 2));
    }

    #[test]
    fn unknown_command_fails() {
        let err = parse_command_line(&line("foo get")).unwrap_err();
        assert!(matches!(err.kind, ParseErrorKind::Unknown));
    }
}
