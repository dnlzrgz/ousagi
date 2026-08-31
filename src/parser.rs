use atoi::FromRadix10SignedChecked;
use bytes::Bytes;

use crate::commands::{ArithmeticOp, Command, Response, StoreArgs, StoreOp};

const MAX_KEY_LEN: usize = 250;
const MAX_ITEM_SIZE: usize = 1024 * 1024; // 1 MiB

pub enum ParseErrorKind {
    BadFormat,
    NumericDelta,
    NumericDelay,
    TooLarge,
    Unknown,
}

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

pub enum CommandHeader {
    Immediate(Command),
    Store(PendingStore),
}

pub fn parse_command_line(line: &Bytes) -> Result<CommandHeader, ParseError> {
    let mut tokens = line.split(|&b| b == b' ').filter(|s| !s.is_empty());

    let op = match tokens.next() {
        Some(op) => op,
        None => return Err(ParseError::new(ParseErrorKind::Unknown)),
    };

    match op {
        b"get" | b"gets" => parse_get(op == b"gets", tokens, line),
        b"add" => parse_store(StoreOp::Add, tokens, line),
        b"set" => parse_store(StoreOp::Set, tokens, line),
        b"replace" => parse_store(StoreOp::Replace, tokens, line),
        b"append" => parse_store(StoreOp::Append, tokens, line),
        b"prepend" => parse_store(StoreOp::Prepend, tokens, line),
        b"cas" => parse_store(StoreOp::Cas, tokens, line),
        b"delete" => parse_delete(tokens, line),
        b"incr" => parse_arithmetic(ArithmeticOp::Incr, tokens, line),
        b"decr" => parse_arithmetic(ArithmeticOp::Decr, tokens, line),
        b"flush_all" => parse_flush_all(tokens),
        _ => Err(ParseError::new(ParseErrorKind::Unknown)),
    }
}

fn validate_key(key: &[u8]) -> Result<(), ()> {
    if key.is_empty() || key.len() > MAX_KEY_LEN {
        return Err(());
    }

    if key.iter().any(|&b| b <= 0x20 || b == 0x7F) {
        return Err(());
    }

    Ok(())
}

/// Parse a field (e.g. flags) directly from ASCII bytes.
fn parse_field<T: FromRadix10SignedChecked>(tok: &[u8]) -> Result<T, ()> {
    let (value, used) = T::from_radix_10_signed_checked(tok);
    if used == tok.len() {
        value.ok_or(())
    } else {
        Err(())
    }
}

fn parse_key(key: &[u8], line: &Bytes) -> Result<Bytes, ParseError> {
    validate_key(key).map_err(|_| ParseError::new(ParseErrorKind::BadFormat))?;
    Ok(line.slice_ref(key))
}

fn parse_noreply<'a>(mut rest: impl Iterator<Item = &'a [u8]>) -> Result<bool, ParseError> {
    match rest.next() {
        None => Ok(false),
        Some(b"noreply") if rest.next().is_none() => Ok(true),
        _ => Err(ParseError::new(ParseErrorKind::BadFormat)),
    }
}

fn parse_get<'a>(
    with_cas: bool,
    tokens: impl Iterator<Item = &'a [u8]>,
    line: &Bytes,
) -> Result<CommandHeader, ParseError> {
    let mut tokens = tokens.peekable();
    if tokens.peek().is_none() {
        return Err(ParseError::new(ParseErrorKind::Unknown));
    }

    let keys = tokens
        .map(|k| parse_key(k, line))
        .collect::<Result<Vec<_>, _>>()?;

    Ok(CommandHeader::Immediate(Command::Get { keys, with_cas }))
}

fn parse_store<'a>(
    op: StoreOp,
    mut tokens: impl Iterator<Item = &'a [u8]>,
    line: &Bytes,
) -> Result<CommandHeader, ParseError> {
    // If any of these four required tokens is missing, the old slice
    // pattern (`[op, key, flags, exptime, len, rest @ ..]`) would have
    // failed to match at all and fallen through to the catch-all Unknown
    // arm, never reaching field validation. Preserve that here.
    let (key, flags, exptime, len) =
        match (tokens.next(), tokens.next(), tokens.next(), tokens.next()) {
            (Some(key), Some(flags), Some(exptime), Some(len)) => (key, flags, exptime, len),
            _ => return Err(ParseError::new(ParseErrorKind::Unknown)),
        };

    let key = parse_key(key, line)?;
    let flags =
        parse_field::<u32>(flags).map_err(|_| ParseError::new(ParseErrorKind::BadFormat))?;
    let exptime =
        parse_field::<i64>(exptime).map_err(|_| ParseError::new(ParseErrorKind::BadFormat))?;
    let len = parse_field::<usize>(len).map_err(|_| ParseError::new(ParseErrorKind::BadFormat))?;

    if len > MAX_ITEM_SIZE {
        return Err(ParseError::new(ParseErrorKind::TooLarge).with_discard(len + 2));
    }

    let cas = if op == StoreOp::Cas {
        match tokens.next() {
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

    let noreply = parse_noreply(tokens).map_err(|e| e.with_discard(len + 2))?;

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

fn parse_delete<'a>(
    mut tokens: impl Iterator<Item = &'a [u8]>,
    line: &Bytes,
) -> Result<CommandHeader, ParseError> {
    let key = match tokens.next() {
        Some(key) => key,
        None => return Err(ParseError::new(ParseErrorKind::Unknown)),
    };

    let key = parse_key(key, line)?;
    let noreply = parse_noreply(tokens)?;
    Ok(CommandHeader::Immediate(Command::Delete { key, noreply }))
}

fn parse_arithmetic<'a>(
    op: ArithmeticOp,
    mut tokens: impl Iterator<Item = &'a [u8]>,
    line: &Bytes,
) -> Result<CommandHeader, ParseError> {
    let (key, delta) = match (tokens.next(), tokens.next()) {
        (Some(key), Some(delta)) => (key, delta),
        _ => return Err(ParseError::new(ParseErrorKind::Unknown)),
    };

    let key = parse_key(key, line)?;
    let delta =
        parse_field::<u64>(delta).map_err(|_| ParseError::new(ParseErrorKind::NumericDelta))?;
    let noreply = parse_noreply(tokens)?;

    Ok(CommandHeader::Immediate(Command::Arithmetic {
        op,
        key,
        delta,
        noreply,
    }))
}

fn parse_flush_all<'a>(
    tokens: impl Iterator<Item = &'a [u8]>,
) -> Result<CommandHeader, ParseError> {
    let mut tokens = tokens.peekable();

    let delay = match tokens.peek() {
        Some(&d) if d != b"noreply" => {
            let delay =
                parse_field::<u32>(d).map_err(|_| ParseError::new(ParseErrorKind::NumericDelay))?;
            tokens.next(); // consume the delay token now that it parsed successfully
            Some(delay)
        }
        _ => None,
    };

    let noreply = parse_noreply(tokens)?;

    Ok(CommandHeader::Immediate(Command::FlushAll {
        delay,
        noreply,
    }))
}
