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

#[derive(Debug)]
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

#[derive(Debug)]
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
            Command::Stats { .. } => false,
            Command::Version => false,
            Command::Store(_, args) => args.noreply,
            Command::Delete { noreply, .. }
            | Command::Arithmetic { noreply, .. }
            | Command::Touch { noreply, .. }
            | Command::FlushAll { noreply, .. }
            | Command::Verbosity { noreply, .. } => *noreply,
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
    Touched,
    Values(Vec<(Bytes, u32, Bytes, Option<u64>)>),
    Number(u64),
    Ok,
    Version(&'static str),
    Error,
    ClientError(&'static str),
    ServerError(&'static str),
    Stats(Vec<(&'static str, String)>),
}
