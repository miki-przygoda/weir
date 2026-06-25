//! Durable, high-throughput write buffer for Rust — a thin facade over the
//! [weir] crates.
//!
//! [weir]: https://github.com/miki-przygoda/weir
//!
//! `weir` re-exports the published `weir-*` **library** crates under short module
//! names so an application can depend on a single crate and a single version line.
//! It adds no functionality of its own — each module *is* the corresponding crate:
//!
//! | Module       | Crate           | Feature     | Purpose                                |
//! |--------------|-----------------|-------------|----------------------------------------|
//! | [`core`]     | `weir-core`     | (always on) | Shared wire-protocol types and errors  |
//! | [`client`]   | `weir-client`   | `client`    | Connect to the daemon and send records |
//! | [`sink_sdk`] | `weir-sink-sdk` | `sink-sdk`  | Build a custom downstream sink         |
//! | [`wab`]      | `weir-wab`      | `wab`       | Read on-disk WAB segments              |
//!
//! Enable `full` for all of the above at once, or `tls` for the mutual-TLS client.
//!
//! ```toml
//! # just the wire types
//! weir = "1.3"
//! # an app that talks to the daemon
//! weir = { version = "1.3", features = ["client"] }
//! ```
//!
//! The daemon (`weir-server`) and the admin CLI (`weir-ctl`) are **binaries**, not
//! libraries — install them with `cargo install weir-server` / `cargo install
//! weir-ctl`. They are intentionally not re-exported here, so depending on `weir`
//! never pulls the daemon's async/sink/TLS dependency tree.
#![deny(missing_docs)]

/// Shared wire-protocol types — [`Payload`](weir_core::Payload),
/// [`Durability`](weir_core::Durability), error and nack types — from the
/// `weir-core` crate. Always available.
pub use weir_core as core;

/// Client for connecting to the weir daemon and sending records (the
/// `weir-client` crate). Enable the `client` feature.
#[cfg(feature = "client")]
pub use weir_client as client;

/// `Sink` trait and error/result contract for building custom sinks (the
/// `weir-sink-sdk` crate). Enable the `sink-sdk` feature.
#[cfg(feature = "sink-sdk")]
pub use weir_sink_sdk as sink_sdk;

/// On-disk WAB segment format and [`SegmentReader`](weir_wab::SegmentReader)
/// (the `weir-wab` crate). Enable the `wab` feature.
#[cfg(feature = "wab")]
pub use weir_wab as wab;

#[cfg(all(test, feature = "full"))]
mod reexport_smoke {
    //! Compile-time proof that every gated re-export resolves to a real item in
    //! its crate. If a re-export breaks (a crate is renamed, or a named type is
    //! removed), this module fails to compile rather than shipping a dangling
    //! facade. Runs under `--features full` (which turns on every re-export).

    // One concrete public item per re-exported crate. The `use` itself is the
    // assertion: it only resolves if the facade module points at the real crate.
    use crate::client::WeirClient;
    use crate::core::Payload;
    use crate::sink_sdk::Sink;
    use crate::wab::SegmentReader;

    #[test]
    fn reexports_resolve_to_their_crates() {
        // Naming each type through the facade path forces resolution; the type
        // name carries the originating crate, so the facade really points there.
        assert!(std::any::type_name::<Payload>().contains("weir_core"));
        assert!(std::any::type_name::<WeirClient>().contains("weir_client"));
        assert!(std::any::type_name::<SegmentReader>().contains("weir_wab"));
        // `Sink` is a trait: naming it in a bound proves the re-export resolves.
        fn _accepts_a_sink<S: Sink>() {}
    }
}
