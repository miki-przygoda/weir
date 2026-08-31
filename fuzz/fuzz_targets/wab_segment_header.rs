//! Fuzz target: `weir_server::wab::format::parse_segment_header`.
//!
//! Feeds arbitrary bytes into the segment-header parser. The header is the
//! first thing read from every `.wab` / `.wab.sealed` file on disk, at daemon
//! startup (crash recovery scans the WAB directory) and on every drain; those
//! bytes are attacker-controlled after a host compromise, so a panic or an
//! unbounded allocation here is a denial-of-service vector.
//!
//! This target exists specifically because format v2 added *decision logic* to
//! the parser. v1 checked length, magic and one version byte; v2 additionally
//! routes on the version and validates a flags byte, rejecting any reserved bit
//! it does not understand. That routing is new attacker-reachable branching,
//! which is what makes it worth fuzzing — the decompression it gates is
//! libzstd's own code behind a constant output bound, not ours.
//!
//! Property under test: the parser never panics on any input. Errors are fine
//! (expected for most random inputs); panics are not. Where a parse succeeds,
//! the reported compression must agree with the flags byte — a mismatch would
//! mean a segment could be read under the wrong codec.

#![no_main]

use libfuzzer_sys::fuzz_target;
use weir_server::wab::format::{
    Compression, FLAG_ZSTD, FORMAT_VERSION_V1, FORMAT_VERSION_V2, parse_segment_header,
};

fuzz_target!(|data: &[u8]| {
    if let Ok(meta) = parse_segment_header(data) {
        // A successful parse implies a well-formed header, so these hold by
        // construction — assert them so a future refactor cannot quietly let a
        // segment be read under a codec its flags byte did not declare.
        assert!(
            meta.format_version == FORMAT_VERSION_V1 || meta.format_version == FORMAT_VERSION_V2,
            "accepted an out-of-range format version: {}",
            meta.format_version
        );
        let flags = data[5];
        match meta.compression {
            Compression::None => assert_eq!(
                flags & FLAG_ZSTD,
                0,
                "reported None while the ZSTD flag bit was set"
            ),
            Compression::Zstd => {
                assert_eq!(
                    meta.format_version, FORMAT_VERSION_V2,
                    "ZSTD is only expressible in v2"
                );
                assert_ne!(flags & FLAG_ZSTD, 0, "reported Zstd without the flag bit");
            }
        }
    }
});
