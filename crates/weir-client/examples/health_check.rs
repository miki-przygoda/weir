//! Query the weir daemon's health over the wire protocol.
//!
//! Usage:
//!   cargo run -p weir-client --example health_check -- [--socket <path>]
//!
//! Exit code 0 = healthy, 1 = unreachable or error.

#[cfg(not(unix))]
fn main() {
    eprintln!("health_check requires a Unix system (Unix domain sockets).");
    std::process::exit(1);
}

#[cfg(unix)]
fn main() {
    use weir_client::WeirClient;

    let mut args = std::env::args().skip(1);
    let mut socket = "/run/weir/weir.sock".to_string();

    while let Some(arg) = args.next() {
        match arg.as_str() {
            "--socket" => {
                socket = args.next().unwrap_or_else(|| {
                    eprintln!("--socket requires a value");
                    std::process::exit(1);
                });
            }
            other => {
                eprintln!("unknown argument: {other}");
                eprintln!("usage: health_check [--socket <path>]");
                std::process::exit(1);
            }
        }
    }

    let mut client = WeirClient::connect(&socket).unwrap_or_else(|e| {
        eprintln!("connect to {socket}: {e}");
        std::process::exit(1);
    });

    match client.health_check() {
        Ok(()) => {
            println!("healthy");
        }
        Err(e) => {
            eprintln!("error: {e}");
            std::process::exit(1);
        }
    }
}
