# weir-rs

Durable, high-throughput write buffer for Rust — microsecond fsync'd acks over a
Unix socket, N→1 batched commits to your database, and crash-safe WAB replay.

This crate is a **facade**: it re-exports the published `weir-*` library crates
under short module names so you can depend on one crate and one version line. It
contains no logic of its own — each module *is* the corresponding crate.

> Published as `weir-rs` (the bare `weir` name is already taken on crates.io by
> an unrelated project). The import root is `weir_rs`.

## Which piece do you need?

| You want to…                       | Use                                                                          |
|------------------------------------|------------------------------------------------------------------------------|
| Just the wire-protocol types       | `weir-rs = "1.3"` → `weir_rs::core` (always on)                              |
| Send records from your app         | `weir-rs = { version = "1.3", features = ["client"] }` → `weir_rs::client`   |
| Build a custom sink                | `weir-rs = { version = "1.3", features = ["sink-sdk"] }` → `weir_rs::sink_sdk` |
| Read on-disk WAB segments          | `weir-rs = { version = "1.3", features = ["wab"] }` → `weir_rs::wab`         |
| Run the daemon                     | `cargo install weir-server`                                                  |
| Operate / inspect a running daemon | `cargo install weir-ctl`                                                     |

`features = ["full"]` enables client + sink-sdk + wab together; `features = ["tls"]`
adds the mutual-TLS client.

## Example

```rust,ignore
use weir_rs::client::WeirClient;

// Connect to the daemon's Unix socket and push a durably-buffered record.
let client = WeirClient::connect("/run/weir/weir.sock")?;
client.push(b"hello")?;
```

## The crates behind the facade

- [`weir-core`](https://crates.io/crates/weir-core) — shared wire-protocol types and errors
- [`weir-client`](https://crates.io/crates/weir-client) — client library
- [`weir-sink-sdk`](https://crates.io/crates/weir-sink-sdk) — sink trait + error/result contract
- [`weir-wab`](https://crates.io/crates/weir-wab) — on-disk WAB segment format + reader
- [`weir-server`](https://crates.io/crates/weir-server) — the daemon (binary)
- [`weir-ctl`](https://crates.io/crates/weir-ctl) — admin / inspection CLI (binary)

See the [project README](https://github.com/miki-przygoda/weir) for architecture,
guides, and the wire protocol.

## License

MIT
