use bytes::Bytes;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StoreOp {
    Set,
    Add,
    Replace,
    Append,
    Prepend,
    Cas,
}

pub struct StoreArgs {
    pub key: Bytes,
    pub flags: u32,
    pub exptime: i64,
    pub data: Bytes,
    pub noreply: bool,
    pub cas: Option<u64>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ArithmeticOp {
    Incr,
    Decr,
}

pub enum Command {
    Get {
        keys: Vec<Bytes>,
        with_cas: bool,
    },
    Store(StoreOp, StoreArgs),
    Delete {
        key: Bytes,
        noreply: bool,
    },
    Arithmetic {
        op: ArithmeticOp,
        key: Bytes,
        delta: u64,
        noreply: bool,
    },
    FlushAll {
        delay: Option<u32>,
        noreply: bool,
    },
}

impl Command {
    /// Whether the client asked to avoid the reply for the command. `get` always replies, so `Get`
    /// always has to return `false`. Centralizing the check here means `process` doesn't need to
    /// match on `Command` again just to find the flag.
    pub(crate) fn noreply(&self) -> bool {
        match self {
            Command::Get { .. } => false,
            Command::Store(_, args) => args.noreply,
            Command::Delete { noreply, .. }
            | Command::Arithmetic { noreply, .. }
            | Command::FlushAll { noreply, .. } => *noreply,
        }
    }
}

/// Response written back to the client for a given `Command`.
#[derive(Debug)]
pub enum Response {
    Stored,
    NotStored,
    Deleted,
    NotFound,
    Exists,
    Values(Vec<(Bytes, u32, Bytes, Option<u64>)>),
    Number(u64),
    Ok,
    Error,
    ClientError(&'static str),
    ServerError(&'static str),
}
