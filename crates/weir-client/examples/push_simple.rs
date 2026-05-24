//! Push records to a running weir daemon across all three durability tiers.
//!
//! Usage:
//!   cargo run -p weir-client --example push_simple -- [--socket <path>] [--count <n>]
//!
//! Defaults: socket = /run/weir/weir.sock, count = 5
//!
//! Each push prints whether it was acked and how long it took.

#[cfg(not(unix))]
fn main() {
    eprintln!("push_simple requires a Unix system (Unix domain sockets).");
    std::process::exit(1);
}

#[cfg(unix)]
fn main() {
    use std::time::Instant;
    use weir_client::WeirClient;
    use weir_core::Durability;

    let mut args = std::env::args().skip(1);
    let mut socket = "/run/weir/weir.sock".to_string();
    let mut count = 5usize;

    while let Some(arg) = args.next() {
        match arg.as_str() {
            "--socket" => {
                socket = args.next().unwrap_or_else(|| {
                    eprintln!("--socket requires a value");
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
                eprintln!("usage: push_simple [--socket <path>] [--count <n>]");
                std::process::exit(1);
            }
        }
    }

    let mut client = WeirClient::connect(&socket).unwrap_or_else(|e| {
        eprintln!("connect to {socket}: {e}");
        std::process::exit(1);
    });

    let tiers = [
        ("Sync", Durability::Sync),
        ("Batched", Durability::Batched),
        ("Buffered", Durability::Buffered),
    ];

    let mut ok = 0u32;
    let mut err = 0u32;

    for (label, durability) in &tiers {
        for i in 0..count {
            let payload = format!("{label}-record-{i:04}");
            let t = Instant::now();
            match client.push(payload.as_bytes(), *durability) {
                Ok(()) => {
                    println!(
                        "ack  [{label}] #{i:04}  {:.3}ms  {:?}",
                        t.elapsed().as_secs_f64() * 1000.0,
                        payload
                    );
                    ok += 1;
                }
                Err(e) => {
                    eprintln!("nack [{label}] #{i:04}  {e}");
                    err += 1;
                }
            }
        }
    }

    println!("\n{ok} acked, {err} errors");
    if err > 0 {
        std::process::exit(1);
    }
}
