use bytes::Bytes;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StoreOp {
    Set,
    Add,
    Replace,
    Append,
    Prepend,
    /// Carries the token item's current CAS must match.
    Cas(u64),
}

#[derive(Debug)]
pub struct StoreArgs {
    pub key: Bytes,
    pub flags: u32,
    pub exptime: i64,
    pub data: Bytes,
    pub noreply: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ArithmeticOp {
    Incr,
    Decr,
}

#[derive(Debug)]
pub enum Command {
    Get {
        keys: Vec<Bytes>,
        with_cas: bool,
    },
    Store(StoreOp, StoreArgs),
    StoreTooLarge {
        op: StoreOp,
        key: Bytes,
        noreply: bool,
    },
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
    GetAndTouch {
        keys: Vec<Bytes>,
        exptime: i64,
        with_cas: bool,
    },
    Touch {
        key: Bytes,
        exptime: i64,
        noreply: bool,
    },
    FlushAll {
        delay: Option<u32>,
        noreply: bool,
    },
    Version,
    Verbosity {
        level: u32,
        noreply: bool,
    },
    Stats,
}

impl Command {
    pub(crate) fn noreply(&self) -> bool {
        match self {
            Command::Get { .. } => false,
            Command::GetAndTouch { .. } => false,
            Command::Stats => false,
            Command::Version => false,
            Command::Store(_, args) => args.noreply,
            Command::StoreTooLarge { noreply, .. } => *noreply,
            Command::Delete { noreply, .. }
            | Command::Arithmetic { noreply, .. }
            | Command::Touch { noreply, .. }
            | Command::FlushAll { noreply, .. }
            | Command::Verbosity { noreply, .. } => *noreply,
        }
    }
}

/// Single item in a `get`/`gets`/`gat`/`gats` response.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Value<'a> {
    pub key: &'a [u8],
    pub flags: u32,
    pub data: &'a Bytes,
    /// Only for `gets`/`gats`.
    pub cas: Option<u64>,
}

/// Response written back to the client for a given `Command`. Values and
/// stats are encoded one by one (`Output::value`, `Output::stat`) before `End`.
#[derive(Debug)]
pub enum Response {
    Stored,
    NotStored,
    Deleted,
    NotFound,
    Exists,
    Touched,
    End,
    Number(u64),
    Ok,
    Version(&'static str),
    Error,
    ClientError(&'static str),
    ServerError(&'static str),
}
