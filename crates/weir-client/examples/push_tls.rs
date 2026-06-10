//! Push records to a running weir daemon over a TCP+mTLS connection.
//!
//! Usage:
//!   cargo run -p weir-client --features tls --example push_tls -- \
//!     --addr 127.0.0.1:7100 \
//!     --ca /path/to/ca.crt \
//!     --cert /path/to/client.crt \
//!     --key /path/to/client.key \
//!     [--server-name weir-server] \
//!     [--count 5]
//!
//! Defaults: addr = 127.0.0.1:7100, server-name = weir-server, count = 5
//!
//! Build without the feature to see a usage hint instead.

#[cfg(not(all(unix, feature = "tls")))]
fn main() {
    eprintln!("push_tls requires --features tls.");
    eprintln!("Run: cargo run -p weir-client --features tls --example push_tls -- [options]");
    std::process::exit(1);
}

#[cfg(all(unix, feature = "tls"))]
fn main() {
    use std::{path::PathBuf, time::Instant};

    use weir_client::{ClientTlsConfig, WeirClient};
    use weir_core::Durability;

    let mut args = std::env::args().skip(1);

    let mut addr = "127.0.0.1:7100".to_string();
    let mut ca: Option<PathBuf> = None;
    let mut cert: Option<PathBuf> = None;
    let mut key: Option<PathBuf> = None;
    let mut server_name = "weir-server".to_string();
    let mut count = 5usize;

    while let Some(arg) = args.next() {
        match arg.as_str() {
            "--addr" => {
                addr = args.next().unwrap_or_else(|| {
                    eprintln!("--addr requires a value");
                    std::process::exit(1);
                });
            }
            "--ca" => {
                ca = Some(PathBuf::from(args.next().unwrap_or_else(|| {
                    eprintln!("--ca requires a value");
                    std::process::exit(1);
                })));
            }
            "--cert" => {
                cert = Some(PathBuf::from(args.next().unwrap_or_else(|| {
                    eprintln!("--cert requires a value");
                    std::process::exit(1);
                })));
            }
            "--key" => {
                key = Some(PathBuf::from(args.next().unwrap_or_else(|| {
                    eprintln!("--key requires a value");
                    std::process::exit(1);
                })));
            }
            "--server-name" => {
                server_name = args.next().unwrap_or_else(|| {
                    eprintln!("--server-name requires a value");
                    std::process::exit(1);
                });
            }
            "--count" => {
                let s = args.next().unwrap_or_else(|| {
                    eprintln!("--count requires a value");
                    std::process::exit(1);
                });
                count = s.parse().unwrap_or_else(|_| {
                    eprintln!("--count must be a positive integer");
                    std::process::exit(1);
                });
            }
            other => {
                eprintln!("unknown argument: {other}");
                eprintln!(
                    "usage: push_tls --addr <addr> --ca <path> --cert <path> --key <path> \
                     [--server-name <name>] [--count <n>]"
                );
                std::process::exit(1);
            }
        }
    }

    let ca = ca.unwrap_or_else(|| {
        eprintln!("--ca is required");
        std::process::exit(1);
    });
    let cert = cert.unwrap_or_else(|| {
        eprintln!("--cert is required");
        std::process::exit(1);
    });
    let key = key.unwrap_or_else(|| {
        eprintln!("--key is required");
        std::process::exit(1);
    });

    let tls_cfg = ClientTlsConfig {
        client_cert: &cert,
        client_key: &key,
        ca_cert: &ca,
        server_name: &server_name,
        default_durability: Some(Durability::Batched),
    };

    let mut client = WeirClient::connect_tls(&addr, tls_cfg).unwrap_or_else(|e| {
        eprintln!("connect_tls to {addr}: {e}");
        std::process::exit(1);
    });

    let mut ok = 0u32;
    let mut err = 0u32;

    for i in 0..count {
        let payload = format!("tls-record-{i:04}");
        let t = Instant::now();
        match client.push_default(payload.as_bytes()) {
            Ok(()) => {
                println!(
                    "ack  #{i:04}  {:.3}ms  {:?}",
                    t.elapsed().as_secs_f64() * 1000.0,
                    payload
                );
                ok += 1;
            }
            Err(e) => {
                eprintln!("nack #{i:04}  {e}");
                err += 1;
            }
        }
    }

    println!("\n{ok} acked, {err} errors");
    if err > 0 {
        std::process::exit(1);
    }
}
