use bytes::Bytes;

/// A fully-parsed client request.
pub enum Command {
    Get {
        keys: Vec<String>,
    },
    Set {
        key: String,
        flags: u32,
        exptime: i64,
        data: Bytes,
        noreply: bool,
    },
    Delete {
        key: String,
        noreply: bool,
    },
}

impl Command {
    /// Whether the client asked to avoid the reply for the command. `get` always replies, so `Get`
    /// always has to return `false`. Centralizing the check here means `process` doesn't need to
    /// match on `Command` again just to find the flag.
    pub(crate) fn noreply(&self) -> bool {
        match self {
            Command::Set { noreply, .. } | Command::Delete { noreply, .. } => *noreply,
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
