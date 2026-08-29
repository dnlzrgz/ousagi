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
    let parts: Vec<&[u8]> = line
        .split(|&b| b == b' ')
        .filter(|s| !s.is_empty())
        .collect();

    match parts.as_slice() {
        [op @ (b"get" | b"gets"), keys @ ..] if !keys.is_empty() => {
            parse_get(*op == b"gets", keys, line)
        }
        [
            op @ (b"add" | b"set" | b"replace" | b"append" | b"prepend" | b"cas"),
            key,
            flags,
            exptime,
            len,
            rest @ ..,
        ] => parse_store(op, key, flags, exptime, len, rest, line),
        [b"delete", key, rest @ ..] => parse_delete(key, rest, line),
        [op @ (b"incr" | b"decr"), key, delta, rest @ ..] => {
            parse_arithmetic(op, key, delta, rest, line)
        }
        [b"flush_all", rest @ ..] => parse_flush_all(rest),
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

fn parse_field<T: std::str::FromStr>(tok: &[u8]) -> Result<T, ()> {
    std::str::from_utf8(tok)
        .ok()
        .and_then(|s| s.parse().ok())
        .ok_or(())
}

fn parse_key(key: &[u8], line: &Bytes) -> Result<Bytes, ParseError> {
    validate_key(key).map_err(|_| ParseError::new(ParseErrorKind::BadFormat))?;
    Ok(line.slice_ref(key))
}

fn parse_noreply(rest: &[&[u8]]) -> Result<bool, ParseError> {
    match rest {
        [] => Ok(false),
        [b"noreply"] => Ok(true),
        _ => Err(ParseError::new(ParseErrorKind::BadFormat)),
    }
}

fn parse_get(with_cas: bool, keys: &[&[u8]], line: &Bytes) -> Result<CommandHeader, ParseError> {
    let keys = keys
        .iter()
        .map(|k| parse_key(k, line))
        .collect::<Result<Vec<_>, _>>()?;

    Ok(CommandHeader::Immediate(Command::Get { keys, with_cas }))
}

fn parse_store(
    op: &[u8],
    key: &[u8],
    flags: &[u8],
    exptime: &[u8],
    len: &[u8],
    rest: &[&[u8]],
    line: &Bytes,
) -> Result<CommandHeader, ParseError> {
    let op = match op {
        b"add" => StoreOp::Add,
        b"set" => StoreOp::Set,
        b"replace" => StoreOp::Replace,
        b"append" => StoreOp::Append,
        b"prepend" => StoreOp::Prepend,
        b"cas" => StoreOp::Cas,
        _ => unreachable!("guarded by the calling match arm"),
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

    let (cas, rest) = if op == StoreOp::Cas {
        match rest {
            [cas_token, rest @ ..] => {
                let cas = parse_field::<u64>(cas_token).map_err(|_| {
                    ParseError::new(ParseErrorKind::BadFormat).with_discard(len + 2)
                })?;
                (Some(cas), rest)
            }
            [] => return Err(ParseError::new(ParseErrorKind::BadFormat).with_discard(len + 2)),
        }
    } else {
        (None, rest)
    };

    let noreply = parse_noreply(rest).map_err(|e| e.with_discard(len + 2))?;

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

fn parse_delete(key: &[u8], rest: &[&[u8]], line: &Bytes) -> Result<CommandHeader, ParseError> {
    let key = parse_key(key, line)?;
    let noreply = parse_noreply(rest)?;
    Ok(CommandHeader::Immediate(Command::Delete { key, noreply }))
}

fn parse_arithmetic(
    op: &[u8],
    key: &[u8],
    delta: &[u8],
    rest: &[&[u8]],
    line: &Bytes,
) -> Result<CommandHeader, ParseError> {
    let op = if op == b"incr" {
        ArithmeticOp::Incr
    } else {
        ArithmeticOp::Decr
    };
    let key = parse_key(key, line)?;
    let delta =
        parse_field::<u64>(delta).map_err(|_| ParseError::new(ParseErrorKind::NumericDelta))?;
    let noreply = parse_noreply(rest)?;

    Ok(CommandHeader::Immediate(Command::Arithmetic {
        op,
        key,
        delta,
        noreply,
    }))
}

fn parse_flush_all(rest: &[&[u8]]) -> Result<CommandHeader, ParseError> {
    let (delay, rest) = match rest {
        [d, tail @ ..] if *d != b"noreply" => {
            let delay =
                parse_field::<u32>(d).map_err(|_| ParseError::new(ParseErrorKind::NumericDelay))?;
            (Some(delay), tail)
        }
        _ => (None, rest),
    };
    let noreply = parse_noreply(rest)?;

    Ok(CommandHeader::Immediate(Command::FlushAll {
        delay,
        noreply,
    }))
}
