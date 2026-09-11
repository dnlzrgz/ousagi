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
    Quit,
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
        b"gat" | b"gats" => parse_get_and_touch(op == b"gats", &mut tokenizer),
        b"add" => parse_store(StoreOp::Add, &mut tokenizer),
        b"set" => parse_store(StoreOp::Set, &mut tokenizer),
        b"replace" => parse_store(StoreOp::Replace, &mut tokenizer),
        b"append" => parse_store(StoreOp::Append, &mut tokenizer),
        b"prepend" => parse_store(StoreOp::Prepend, &mut tokenizer),
        b"cas" => parse_store(StoreOp::Cas, &mut tokenizer),
        b"delete" => parse_delete(&mut tokenizer),
        b"incr" => parse_arithmetic(ArithmeticOp::Incr, &mut tokenizer),
        b"decr" => parse_arithmetic(ArithmeticOp::Decr, &mut tokenizer),
        b"touch" => parse_touch(&mut tokenizer),
        b"flush_all" => parse_flush_all(&mut tokenizer),
        b"version" => parse_version(&mut tokenizer),
        b"verbosity" => parse_verbosity(&mut tokenizer),
        b"stats" => parse_stats(&mut tokenizer),
        b"quit" => Ok(CommandHeader::Quit),
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
fn expect_end(tokenizer: &mut Tokenizer) -> Result<(), ParseError> {
    match tokenizer.next() {
        None => Ok(()),
        Some(_) => Err(ParseError::new(ParseErrorKind::BadFormat)),
    }
}

#[inline]
fn parse_noreply(tokenizer: &mut Tokenizer) -> Result<bool, ParseError> {
    match tokenizer.next() {
        None => Ok(false),
        Some(b"noreply") => {
            expect_end(tokenizer)?;
            Ok(true)
        }
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

fn parse_get_and_touch(
    with_cas: bool,
    tokenizer: &mut Tokenizer,
) -> Result<CommandHeader, ParseError> {
    let exptime = tokenizer
        .next()
        .ok_or_else(|| ParseError::new(ParseErrorKind::Unknown))?;
    let exptime: i64 =
        parse_field(exptime).map_err(|_| ParseError::new(ParseErrorKind::BadFormat))?;

    let first_key = tokenizer
        .next()
        .ok_or_else(|| ParseError::new(ParseErrorKind::Unknown))?;

    let mut keys = Vec::with_capacity(6);
    keys.push(tokenizer.extract_key(first_key)?);
    while let Some(k) = tokenizer.next() {
        keys.push(tokenizer.extract_key(k)?);
    }

    Ok(CommandHeader::Immediate(Command::GetAndTouch {
        keys,
        exptime,
        with_cas,
    }))
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

    let len: usize = parse_field(len).map_err(|_| ParseError::new(ParseErrorKind::BadFormat))?;
    if len > MAX_ITEM_SIZE {
        return Err(ParseError::new(ParseErrorKind::TooLarge).with_discard(len + 2));
    }

    let key = tokenizer
        .extract_key(key)
        .map_err(|e| e.with_discard(len + 2))?;
    let flags: u32 = parse_field(flags)
        .map_err(|_| ParseError::new(ParseErrorKind::BadFormat).with_discard(len + 2))?;
    let exptime: i64 = parse_field(exptime)
        .map_err(|_| ParseError::new(ParseErrorKind::BadFormat).with_discard(len + 2))?;

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
    let delta: u64 =
        parse_field(delta).map_err(|_| ParseError::new(ParseErrorKind::NumericDelta))?;
    let noreply = parse_noreply(tokenizer)?;

    Ok(CommandHeader::Immediate(Command::Arithmetic {
        op,
        key,
        delta,
        noreply,
    }))
}

fn parse_touch(tokenizer: &mut Tokenizer) -> Result<CommandHeader, ParseError> {
    let (key, exptime) = match (tokenizer.next(), tokenizer.next()) {
        (Some(key), Some(exptime)) => (key, exptime),
        _ => return Err(ParseError::new(ParseErrorKind::Unknown)),
    };
    let key = tokenizer.extract_key(key)?;
    let exptime: i64 =
        parse_field(exptime).map_err(|_| ParseError::new(ParseErrorKind::BadFormat))?;
    let noreply = parse_noreply(tokenizer)?;

    Ok(CommandHeader::Immediate(Command::Touch {
        key,
        exptime,
        noreply,
    }))
}

fn parse_flush_all(tokenizer: &mut Tokenizer) -> Result<CommandHeader, ParseError> {
    let (delay, noreply) = match tokenizer.next() {
        None => (None, false),
        Some(b"noreply") => {
            expect_end(tokenizer)?;
            (None, true)
        }
        Some(d) => {
            let delay: u32 =
                parse_field(d).map_err(|_| ParseError::new(ParseErrorKind::NumericDelay))?;
            let noreply = parse_noreply(tokenizer)?;
            (Some(delay), noreply)
        }
    };

    Ok(CommandHeader::Immediate(Command::FlushAll {
        delay,
        noreply,
    }))
}

fn parse_version(tokenizer: &mut Tokenizer) -> Result<CommandHeader, ParseError> {
    expect_end(tokenizer)?;
    Ok(CommandHeader::Immediate(Command::Version))
}

fn parse_verbosity(tokenizer: &mut Tokenizer) -> Result<CommandHeader, ParseError> {
    let level = tokenizer
        .next()
        .ok_or_else(|| ParseError::new(ParseErrorKind::Unknown))?;
    let level: u32 = parse_field(level).map_err(|_| ParseError::new(ParseErrorKind::BadFormat))?;

    let noreply = parse_noreply(tokenizer)?;

    Ok(CommandHeader::Immediate(Command::Verbosity {
        level,
        noreply,
    }))
}

fn parse_stats(tokenizer: &mut Tokenizer) -> Result<CommandHeader, ParseError> {
    // TODO: add optional subcommands
    expect_end(tokenizer)?;
    Ok(CommandHeader::Immediate(Command::Stats))
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::panic;

    /// Build a `Bytes` command line the same way the client would do
    fn line(s: &str) -> Bytes {
        Bytes::copy_from_slice(s.as_bytes())
    }

    /// Parse and unwrap an `Immediate` command
    fn expect_immediate(input: &str) -> Command {
        match parse_command_line(&line(input)).expect("should parse correctly") {
            CommandHeader::Immediate(cmd) => cmd,
            other => panic!("expected Immediate command, got {other:?}"),
        }
    }

    /// Parse and unwrap a `Store` header
    fn expect_store(input: &str) -> PendingStore {
        match parse_command_line(&line(input)).expect("should parse correctly") {
            CommandHeader::Store(pending) => pending,
            other => panic!("expected Immediate command, got {other:?}"),
        }
    }

    /// Parse and unwrap a bad input, panics if it unexpectly success
    fn expect_error(input: &str) -> ParseError {
        parse_command_line(&line(input)).expect_err("should fail to parse")
    }

    #[test]
    fn empty_line_fails_with_unknown() {
        let err = expect_error("");
        assert!(matches!(err.kind, ParseErrorKind::Unknown));
        assert_eq!(err.discard(), None);
    }

    #[test]
    fn tokenizer_skips_leading_and_trailing_spaces() {
        match expect_immediate(" get foo ") {
            Command::Get { keys, with_cas } => {
                assert_eq!(keys, vec![Bytes::from_static(b"foo")]);
                assert!(!with_cas);
            }
            other => panic!("expected Get command, got {other:?}"),
        }
    }

    #[test]
    fn tokenizer_skips_repeated_spaces_between_tokens() {
        match expect_immediate("get    foo") {
            Command::Get { keys, with_cas } => {
                assert_eq!(keys, vec![Bytes::from_static(b"foo")]);
                assert!(!with_cas);
            }
            other => panic!("expected Get command, got {other:?}"),
        }
    }

    #[test]
    fn unknown_command_fails() {
        let err = expect_error("foo get");

        assert!(matches!(err.kind, ParseErrorKind::Unknown));
        assert_eq!(err.discard(), None);
    }

    #[test]
    fn key_at_max_length_parses_correctly() {
        let key = "a".repeat(MAX_KEY_LEN);
        let input = format!("get {key}");

        match expect_immediate(&input) {
            Command::Get { keys, with_cas } => {
                assert_eq!(keys, vec![Bytes::copy_from_slice(key.as_bytes())]);
                assert!(!with_cas);
            }
            other => panic!("expected Get command, got {other:?}"),
        }
    }

    #[test]
    fn key_exceeding_max_length_fails() {
        let key = "a".repeat(MAX_KEY_LEN + 1);
        let input = format!("get {key}");

        let err = expect_error(&input);

        assert!(matches!(err.kind, ParseErrorKind::BadFormat));
        assert_eq!(err.discard(), None);
    }

    #[test]
    fn key_with_control_character_fails() {
        let err = expect_error("get foo\x01bar");

        assert!(matches!(err.kind, ParseErrorKind::BadFormat));
        assert_eq!(err.discard(), None);
    }

    #[test]
    fn key_with_del_byte_fails() {
        let err = expect_error("get foo\x7Fbar");

        assert!(matches!(err.kind, ParseErrorKind::BadFormat));
        assert_eq!(err.discard(), None);
    }

    #[test]
    fn get_single_key_parses_correctly() {
        match expect_immediate("get foo") {
            Command::Get { keys, with_cas } => {
                assert_eq!(keys, vec![Bytes::from_static(b"foo")]);
                assert!(!with_cas);
            }
            other => panic!("expected GetAndTouch command, got {other:?}"),
        }
    }

    #[test]
    fn get_multiple_keys_parses_correctly() {
        match expect_immediate("get foo bar baz") {
            Command::Get { keys, with_cas } => {
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
            other => panic!("expected GetAndTouch command, got {other:?}"),
        }
    }

    #[test]
    fn get_with_no_keys_fails() {
        let err = expect_error("get");

        assert!(matches!(err.kind, ParseErrorKind::Unknown));
        assert_eq!(err.discard(), None);
    }

    #[test]
    fn gets_sets_with_cas_true() {
        match expect_immediate("gets foo") {
            Command::Get { keys, with_cas } => {
                assert_eq!(keys, vec![Bytes::from_static(b"foo")]);
                assert!(with_cas);
            }
            other => panic!("expected Get command, got {other:?}"),
        }
    }

    #[test]
    fn gat_single_key_parses_correctly() {
        match expect_immediate("gat 100 foo") {
            Command::GetAndTouch {
                keys,
                exptime,
                with_cas,
            } => {
                assert_eq!(keys, vec![Bytes::from_static(b"foo")]);
                assert_eq!(exptime, 100);
                assert!(!with_cas);
            }
            other => panic!("expected GetAndTouch command, got {other:?}"),
        }
    }

    #[test]
    fn gat_multiple_keys_parses_correctly() {
        match expect_immediate("gat 100 foo bar baz") {
            Command::GetAndTouch {
                keys,
                exptime,
                with_cas,
            } => {
                assert_eq!(
                    keys,
                    vec![
                        Bytes::from_static(b"foo"),
                        Bytes::from_static(b"bar"),
                        Bytes::from_static(b"baz")
                    ]
                );
                assert_eq!(exptime, 100);
                assert!(!with_cas);
            }
            other => panic!("expected GetAndTouch command, got {other:?}"),
        }
    }

    #[test]
    fn gats_sets_with_cas_true() {
        match expect_immediate("gats 100 foo") {
            Command::GetAndTouch {
                keys,
                exptime,
                with_cas,
            } => {
                assert_eq!(keys, vec![Bytes::from_static(b"foo")]);
                assert_eq!(exptime, 100);
                assert!(with_cas);
            }
            other => panic!("expected GetAndTouch command, got {other:?}"),
        }
    }

    #[test]
    fn gat_with_missing_exptime_fails() {
        let err = expect_error("gat foo");

        assert!(matches!(err.kind, ParseErrorKind::BadFormat));
        assert_eq!(err.discard(), None);
    }

    #[test]
    fn gat_non_numeric_exptime_fails() {
        let err = expect_error("gat abc foo");

        assert!(matches!(err.kind, ParseErrorKind::BadFormat));
        assert_eq!(err.discard(), None);
    }

    #[test]
    fn gat_with_exptime_but_no_keys_fails() {
        let err = expect_error("gat 100");

        assert!(matches!(err.kind, ParseErrorKind::Unknown));
        assert_eq!(err.discard(), None);
    }

    #[test]
    fn set_with_invalid_flags_discards_payload() {
        let err = expect_error("set foo not_a_number 0 5");

        assert!(matches!(err.kind, ParseErrorKind::BadFormat));
        assert_eq!(err.discard(), Some(5 + 2));
    }
    #[test]
    fn set_parses_all_fields_correctly() {
        let pending = expect_store("set foo 42 0 5");

        assert!(matches!(pending.op, StoreOp::Set));
        assert_eq!(pending.key, Bytes::from_static(b"foo"));
        assert_eq!(pending.flags, 42);
        assert_eq!(pending.exptime, 0);
        assert_eq!(pending.len, 5);
        assert_eq!(pending.cas, None);
        assert!(!pending.noreply);
    }

    #[test]
    fn set_item_larger_than_max_item_size_fails() {
        let input = format!("set foo 0 0 {}", MAX_ITEM_SIZE + 1);
        let err = expect_error(&input);

        assert!(matches!(err.kind, ParseErrorKind::TooLarge));
        assert_eq!(err.discard(), Some(MAX_ITEM_SIZE + 1 + 2));
    }

    #[test]
    fn set_item_at_max_item_size_parses_correctly() {
        let input = format!("set foo 0 0 {MAX_ITEM_SIZE}");
        let pending = expect_store(&input);

        assert_eq!(pending.len, MAX_ITEM_SIZE);
    }

    #[test]
    fn set_with_invalid_key_and_valid_len_discards_payload() {
        let err = expect_error("set fo\x01o 0 0 5");

        assert!(matches!(err.kind, ParseErrorKind::BadFormat));
        assert_eq!(err.discard(), Some(5 + 2));
    }

    #[test]
    fn set_with_invalid_exptime_discards_payload() {
        let err = expect_error("set foo 0 abc 5");

        assert!(matches!(err.kind, ParseErrorKind::BadFormat));
        assert_eq!(err.discard(), Some(5 + 2));
    }

    #[test]
    fn set_missing_fields_fails() {
        let err = expect_error("set foo 0 0");

        assert!(matches!(err.kind, ParseErrorKind::Unknown));
        assert_eq!(err.discard(), None);
    }

    #[test]
    fn set_noreply_parses_correctly() {
        let pending = expect_store("set foo 0 0 5 noreply");

        assert!(pending.noreply);
    }

    #[test]
    fn add_parses_correctly() {
        let pending = expect_store("add foo 0 0 5");

        assert!(matches!(pending.op, StoreOp::Add));
    }

    #[test]
    fn replace_parses_correctly() {
        let pending = expect_store("replace foo 0 0 5");

        assert!(matches!(pending.op, StoreOp::Replace));
    }

    #[test]
    fn append_parses_correctly() {
        let pending = expect_store("append foo 0 0 5");

        assert!(matches!(pending.op, StoreOp::Append));
    }

    #[test]
    fn prepend_parses_correctly() {
        let pending = expect_store("prepend foo 0 0 5");

        assert!(matches!(pending.op, StoreOp::Prepend));
    }

    #[test]
    fn cas_with_valid_token_parses_correctly() {
        let pending = expect_store("cas foo 0 0 5 8");

        assert!(matches!(pending.op, StoreOp::Cas));
        assert_eq!(pending.cas, Some(8));
    }

    #[test]
    fn cas_missing_cas_token_fails() {
        let err = expect_error("cas foo 0 0 5");

        assert!(matches!(err.kind, ParseErrorKind::BadFormat));
        assert_eq!(err.discard(), Some(5 + 2));
    }

    #[test]
    fn cas_non_numeric_cas_token_fails() {
        let err = expect_error("cas foo 0 0 5 abc");

        assert!(matches!(err.kind, ParseErrorKind::BadFormat));
        assert_eq!(err.discard(), Some(5 + 2));
    }

    #[test]
    fn delete_parses_correctly() {
        match expect_immediate("delete foo") {
            Command::Delete { key, noreply } => {
                assert_eq!(key, Bytes::from_static(b"foo"));
                assert!(!noreply);
            }
            other => panic!("expected Delete command, got {other:?}"),
        }
    }

    #[test]
    fn delete_missing_key_fails() {
        let err = expect_error("delete");

        assert!(matches!(err.kind, ParseErrorKind::Unknown));
        assert_eq!(err.discard(), None);
    }

    #[test]
    fn delete_noreply_parses_correctly() {
        match expect_immediate("delete foo noreply") {
            Command::Delete { noreply, .. } => assert!(noreply),
            other => panic!("expected Delete command, got {other:?}"),
        }
    }

    #[test]
    fn incr_parses_correctly() {
        match expect_immediate("incr foo 5") {
            Command::Arithmetic {
                op,
                key,
                delta,
                noreply,
            } => {
                assert!(matches!(op, ArithmeticOp::Incr));
                assert_eq!(key, Bytes::from_static(b"foo"));
                assert_eq!(delta, 5);
                assert!(!noreply);
            }
            other => panic!("expected Arithmetic command, got {other:?}"),
        }
    }

    #[test]
    fn decr_parses_correctly() {
        match expect_immediate("decr foo 5") {
            Command::Arithmetic { op, .. } => assert!(matches!(op, ArithmeticOp::Decr)),
            other => panic!("expected Arithmetic command, got {other:?}"),
        }
    }

    #[test]
    fn incr_non_numeric_delta_fails() {
        let err = expect_error("incr foo abc");

        assert!(matches!(err.kind, ParseErrorKind::NumericDelta));
        assert_eq!(err.discard(), None);
    }

    #[test]
    fn incr_negative_delta_fails() {
        let err = expect_error("incr foo -5");

        assert!(matches!(err.kind, ParseErrorKind::NumericDelta));
        assert_eq!(err.discard(), None);
    }

    #[test]
    fn touch_parses_correctly() {
        match expect_immediate("touch foo 100") {
            Command::Touch {
                key,
                exptime,
                noreply,
            } => {
                assert_eq!(key, Bytes::from_static(b"foo"));
                assert_eq!(exptime, 100);
                assert!(!noreply);
            }
            other => panic!("expected Touch command, got {other:?}"),
        }
    }

    #[test]
    fn touch_missing_exptime_fails() {
        let err = expect_error("touch foo");

        assert!(matches!(err.kind, ParseErrorKind::Unknown));
        assert_eq!(err.discard(), None);
    }

    #[test]
    fn touch_non_numeric_exptime_fails() {
        let err = expect_error("touch foo abc");

        assert!(matches!(err.kind, ParseErrorKind::BadFormat));
        assert_eq!(err.discard(), None);
    }

    #[test]
    fn flush_all_with_no_args_parses_correctly() {
        match expect_immediate("flush_all") {
            Command::FlushAll { delay, noreply } => {
                assert_eq!(delay, None);
                assert!(!noreply);
            }
            other => panic!("expected FlushAll command, got {other:?}"),
        }
    }

    #[test]
    fn flush_all_with_delay_parses_correctly() {
        match expect_immediate("flush_all 30") {
            Command::FlushAll { delay, noreply } => {
                assert_eq!(delay, Some(30));
                assert!(!noreply);
            }
            other => panic!("expected FlushAll command, got {other:?}"),
        }
    }

    #[test]
    fn flush_all_noreply_parses_correctly() {
        match expect_immediate("flush_all noreply") {
            Command::FlushAll { delay, noreply } => {
                assert_eq!(delay, None);
                assert!(noreply);
            }
            other => panic!("expected FlushAll command, got {other:?}"),
        }
    }

    #[test]
    fn flush_all_with_delay_and_noreply_parses_correctly() {
        match expect_immediate("flush_all 30 noreply") {
            Command::FlushAll { delay, noreply } => {
                assert_eq!(delay, Some(30));
                assert!(noreply);
            }
            other => panic!("expected FlushAll command, got {other:?}"),
        }
    }

    #[test]
    fn flush_all_noreply_followed_by_trailing_token_fails() {
        let err = expect_error("flush_all noreply foo");

        assert!(matches!(err.kind, ParseErrorKind::BadFormat));
        assert_eq!(err.discard(), None);
    }

    #[test]
    fn flush_all_non_numeric_delay_fails() {
        let err = expect_error("flush_all abc");

        assert!(matches!(err.kind, ParseErrorKind::NumericDelay));
        assert_eq!(err.discard(), None);
    }

    #[test]
    fn version_parses_correctly() {
        match expect_immediate("version") {
            Command::Version => {}
            other => panic!("expected Version command, got {other:?}"),
        }
    }

    #[test]
    fn verbosity_parses_correctly() {
        match expect_immediate("verbosity 100") {
            Command::Verbosity { level, noreply } => {
                assert_eq!(level, 100);
                assert!(!noreply);
            }
            other => panic!("expected Verbosity command, got {other:?}"),
        }
    }

    #[test]
    fn verbosity_with_trailing_token_fails() {
        let err = expect_error("verbosity 100 foo");
        assert!(matches!(err.kind, ParseErrorKind::BadFormat));
        assert_eq!(err.discard(), None);
    }

    #[test]
    fn verbosity_non_numeric_level_fails() {
        let err = expect_error("verbosity abc");
        assert!(matches!(err.kind, ParseErrorKind::BadFormat));
        assert_eq!(err.discard(), None);
    }

    #[test]
    fn stats_with_no_args_parses_correctly() {
        match expect_immediate("stats") {
            Command::Stats => {}
            other => panic!("expected Stats command, got {other:?}"),
        }
    }

    #[test]
    fn stats_with_subcommand_fails() {
        let err = expect_error("stats cmd_get");

        assert!(matches!(err.kind, ParseErrorKind::BadFormat));
        assert_eq!(err.discard(), None);
    }

    #[test]
    fn quit_parses_correctly() {
        match parse_command_line(&line("quit")).expect("should parse correctly") {
            CommandHeader::Quit => {}
            other => panic!("expected Quit, got {other:?}"),
        }
    }
}
