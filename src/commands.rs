use bytes::Bytes;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StoreOp {
    Set,
    Add,
}

pub struct StoreArgs {
    pub key: String,
    pub flags: u32,
    pub exptime: i64,
    pub data: Bytes,
    pub noreply: bool,
}

pub enum Command {
    Get { keys: Vec<String> },
    Store(StoreOp, StoreArgs),
    Delete { key: String, noreply: bool },
}

impl Command {
    /// Whether the client asked to avoid the reply for the command. `get` always replies, so `Get`
    /// always has to return `false`. Centralizing the check here means `process` doesn't need to
    /// match on `Command` again just to find the flag.
    pub(crate) fn noreply(&self) -> bool {
        match self {
            Command::Store(_, args) => args.noreply,
            Command::Delete { noreply, .. } => *noreply,
            Command::Get { .. } => false,
        }
    }
}

/// Response written back to the client for a given `Command`.
pub enum Response {
    Stored,
    NotStored,
    Deleted,
    NotFound,
    Values(Vec<(String, u32, Bytes)>),
    Error,
}
