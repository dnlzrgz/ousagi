use std::net::IpAddr;

use clap::{ArgAction, Parser};

#[derive(Parser, Debug)]
pub struct Cli {
    /// TCP port to listen on
    #[arg(short = 'p', long, default_value_t = 11211)]
    pub port: u16,

    /// Interface to listen on, default INADDR_ANY
    #[arg(short = 'l', long)]
    pub listen: Option<IpAddr>,

    /// Number of threads to process incoming requests
    #[arg(short = 't', long, default_value_t = 4, value_parser = parse_threads)]
    pub threads: usize,

    /// Max simultaneous client connections
    #[arg(short = 'c', long, default_value_t = 1024)]
    pub max_connections: usize,

    /// Max memory to use for cached items, in megabytes
    #[arg(short = 'm', long, default_value_t = 64)]
    pub memory_limit_mb: u64,

    /// Verbosity level (-v, -vv, -vvv)
    #[arg(short = 'v', long="verbose", action = ArgAction::Count)]
    pub verbose: u8,
}

fn parse_threads(s: &str) -> Result<usize, String> {
    let threads: usize = s
        .parse()
        .map_err(|_| format!("'{s}' isn't a valid number"))?;

    if threads == 0 {
        return Err("thread count must be at least 1".to_string());
    }

    Ok(threads)
}

#[cfg(test)]
mod tests {
    use super::*;
    use clap::CommandFactory;

    #[test]
    fn cli_parses_correctly() {
        Cli::command().debug_assert();
    }

    #[test]
    fn listen_accepts_ipv6() {
        let cli = Cli::try_parse_from(["ousagi", "-l", "::1"]).unwrap();
        assert_eq!(cli.listen, Some(IpAddr::from([0, 0, 0, 0, 0, 0, 0, 1])));
    }

    #[test]
    fn listen_rejects_invalid_address() {
        assert!(Cli::try_parse_from(["ousagi", "-l", "not-an-ip"]).is_err());
    }
}
