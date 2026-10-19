//! Preflight: every refuse-to-start check, run before recovery (spec §3.0).
//!
//! Before this existed, a duplicate daemon learned it could not start only
//! AFTER it had recovered its WAB, started its drain and replayed its backlog:
//! the metrics bind, the TLS load, the TCP bind and the producer-socket bind
//! all sat late in `main`. `main` then returned through `?` while the drain was
//! mid-commit. Under `Restart=on-failure` (or compose `restart:
//! unless-stopped`) every restart re-delivered the head of the oldest
//! unconfirmed segment and died before confirming it: unbounded duplicate
//! delivery at the restart cadence.
//!
//! [`preflight`] runs right after the `wab_dir` lock and before `Metrics::new`
//! and `wab::recover`, so nothing has read the WAB yet. It checks, in order:
//!
//! 1. `socket::validate_socket_path` (format only);
//! 2. what sits at `socket_path`: a non-socket entry refuses (the late bind
//!    would refuse it too), and a connect-only [`probe_socket`] refuses a live
//!    socket or one whose state it cannot tell. It **never unlinks**: a stale
//!    socket is left for `bind_hardened`, which removes it after recovery;
//! 3. a test-bind of the metrics listener, dropped at once;
//! 4. with `tcp_bind` set (`--features tls`): the TLS material, loaded once and
//!    thrown away, then a test-bind of `tcp_bind`, dropped at once.
//!
//! The authoritative operations stay where they were: the real binds, the real
//! TLS load and the probe-and-unlink in `bind_hardened`. Preflight narrows the
//! late-failure window to a race (something binding in between), which is
//! acceptable. It test-binds and releases rather than holding the listeners
//! because a bound but unserved metrics listener would make scrapes hang
//! through a long replay instead of being refused.
//!
//! Contract for later startup work: no refuse-to-start check may run after the
//! drain has begun delivering.
//!
//! [`probe_socket`]: crate::socket::probe_socket

use std::io;

use crate::config::Config;

/// Runs every refuse-to-start check that can run before recovery. An `Err`
/// names the conflicting resource; `main` logs it and exits 1.
pub(crate) fn preflight(config: &Config) -> io::Result<()> {
    #[cfg(unix)]
    check_socket_path(&config.socket_path)?;
    test_bind_metrics(config)?;
    #[cfg(all(unix, feature = "tls"))]
    check_tcp_listener(config)?;
    Ok(())
}

/// Steps 1 and 2: the socket path's format, then what is at it.
#[cfg(unix)]
fn check_socket_path(path: &std::path::Path) -> io::Result<()> {
    use crate::socket::{self, SocketProbe};
    use std::os::unix::fs::{FileTypeExt, MetadataExt};

    socket::validate_socket_path(path)?;
    check_socket_path_len(path)?;

    // `bind_hardened` refuses any non-socket entry (a regular file, a
    // directory, a symlink) rather than unlink it. Refuse it here too, so the
    // refusal comes before recovery instead of after delivery has begun.
    match std::fs::symlink_metadata(path) {
        Ok(meta) if !meta.file_type().is_socket() => {
            return Err(socket::non_socket_refusal(path, meta.mode() & 0o170_000));
        }
        // A socket goes to the probe. A missing entry (or a missing parent,
        // which systemd's RuntimeDirectory can cause) is the probe's `Absent`,
        // and an unreadable parent is the probe's `Unknown`.
        _ => {}
    }

    match socket::probe_socket(path) {
        SocketProbe::Absent | SocketProbe::Stale => Ok(()),
        SocketProbe::Live => Err(socket::live_socket_refusal(path)),
        SocketProbe::Unknown(errno) => Err(socket::unknown_socket_refusal(path, errno)),
    }
}

/// A path that does not fit `sockaddr_un.sun_path` (with its terminating NUL)
/// cannot be bound; the late bind would fail with "path must be shorter than
/// SUN_LEN" after recovery. Refuse it here instead.
#[cfg(unix)]
fn check_socket_path_len(path: &std::path::Path) -> io::Result<()> {
    use std::os::unix::ffi::OsStrExt;
    // Safety: sockaddr_un is plain old data; all-zero is a valid value.
    let addr: libc::sockaddr_un = unsafe { std::mem::zeroed() };
    let max = addr.sun_path.len() - 1;
    let len = path.as_os_str().as_bytes().len();
    if len > max {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            format!(
                "socket_path '{}' is {len} bytes, longer than the {max} bytes a Unix \
                 socket path can have on this platform. Choose a shorter socket_path.",
                path.display()
            ),
        ));
    }
    Ok(())
}

/// Step 3: the metrics listener's address must be free. Bound and dropped.
fn test_bind_metrics(config: &Config) -> io::Result<()> {
    std::net::TcpListener::bind((config.metrics_bind, config.metrics_port))
        .map(drop)
        .map_err(|e| metrics_bind_error(config, &e))
}

/// The refusal for a metrics address that cannot be bound. Shared by the
/// preflight test-bind and the real bind in `main`, so both say the same.
pub(crate) fn metrics_bind_error(config: &Config, e: &io::Error) -> io::Error {
    io::Error::new(
        e.kind(),
        format!(
            "failed to bind metrics endpoint to {}:{} ({e}) — another process \
             (often another weir-server instance) is using that port. Change it \
             with --metrics-port / WEIR_METRICS_PORT (or --metrics-bind / \
             WEIR_METRICS_BIND).",
            config.metrics_bind, config.metrics_port
        ),
    )
}

/// Step 4: with `tcp_bind` set, the TLS material must load and the TCP address
/// must be free. The loaded config and the listener are both dropped.
#[cfg(all(unix, feature = "tls"))]
fn check_tcp_listener(config: &Config) -> io::Result<()> {
    let Some(bind_addr) = config.tcp_bind else {
        return Ok(());
    };
    let (cert, key, ca) = tls_paths(config)?;
    crate::socket::tls::ReloadableServerConfig::load(cert, key, ca)
        .map(drop)
        .map_err(|e| tls_load_error(&e))?;
    std::net::TcpListener::bind(bind_addr)
        .map(drop)
        .map_err(|e| tcp_bind_error(bind_addr, &e))
}

/// The three TLS paths, which config validation guarantees whenever
/// `tcp_bind` is set. An error rather than a panic, should that ever change.
#[cfg(all(unix, feature = "tls"))]
pub(crate) fn tls_paths(
    config: &Config,
) -> io::Result<(std::path::PathBuf, std::path::PathBuf, std::path::PathBuf)> {
    match (
        &config.tls_cert_path,
        &config.tls_key_path,
        &config.tls_client_ca_path,
    ) {
        (Some(cert), Some(key), Some(ca)) => Ok((cert.clone(), key.clone(), ca.clone())),
        _ => Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "tcp_bind is set but tls_cert_path, tls_key_path and tls_client_ca_path \
             are not all set",
        )),
    }
}

/// The refusal for TLS material that does not load. Shared with `main`.
#[cfg(all(unix, feature = "tls"))]
pub(crate) fn tls_load_error(e: &crate::socket::tls::TlsConfigError) -> io::Error {
    io::Error::other(format!("failed to load TLS config: {e}"))
}

/// The refusal for a `tcp_bind` address that cannot be bound. Shared with
/// `main`.
#[cfg(all(unix, feature = "tls"))]
pub(crate) fn tcp_bind_error(bind_addr: std::net::SocketAddr, e: &io::Error) -> io::Error {
    io::Error::new(
        e.kind(),
        format!(
            "failed to bind the TCP+mTLS listener to tcp_bind {bind_addr} ({e}) — another \
             process (often another weir-server instance) is using that address. Change \
             it with --tcp-bind / WEIR_TCP_BIND."
        ),
    )
}

#[cfg(all(test, unix))]
mod tests {
    use super::*;
    use crate::config::PartialConfig;
    use std::path::{Path, PathBuf};

    /// A free loopback port: bind `:0`, read it, release it.
    fn free_port() -> u16 {
        std::net::TcpListener::bind("127.0.0.1:0")
            .unwrap()
            .local_addr()
            .unwrap()
            .port()
    }

    /// A scratch dir with a `wab` subdir, and a validated `Config` whose
    /// socket lives in the dir and whose metrics port is free. `edit` adjusts
    /// the CLI layer before validation.
    fn config_in(label: &str, edit: impl FnOnce(&mut PartialConfig)) -> (Config, PathBuf) {
        let dir = crate::testutil::scratch_dir(&format!("preflight_{label}"));
        let wab = dir.join("wab");
        crate::testutil::mkdir_p(&wab);
        let mut cli = PartialConfig {
            wab_dir: Some(wab),
            socket_path: Some(dir.join("weir.sock")),
            metrics_port: Some(free_port()),
            ..PartialConfig::empty()
        };
        edit(&mut cli);
        let config = Config::from_layers(cli, PartialConfig::empty(), PartialConfig::empty())
            .expect("valid test config");
        (config, dir)
    }

    fn cleanup(dir: &Path) {
        std::fs::remove_dir_all(dir).ok();
    }

    #[test]
    fn preflight_passes_on_clean_paths() {
        let (config, dir) = config_in("clean", |_| {});
        preflight(&config).expect("nothing conflicts, so preflight must pass");
        assert!(
            !config.socket_path.exists(),
            "preflight creates nothing at socket_path"
        );
        cleanup(&dir);
    }

    /// Preflight test-binds and RELEASES: holding a bound but unserved metrics
    /// listener through a long replay would make scrapes hang. The real bind
    /// later in `main` must find the address free.
    #[test]
    fn preflight_releases_what_it_test_binds() {
        let (config, dir) = config_in("releases", |_| {});
        preflight(&config).unwrap();
        std::net::TcpListener::bind((config.metrics_bind, config.metrics_port))
            .expect("the metrics address must be free again after preflight");
        cleanup(&dir);
    }

    #[test]
    fn preflight_refuses_live_socket() {
        let (config, dir) = config_in("live_socket", |_| {});
        let _live = std::os::unix::net::UnixListener::bind(&config.socket_path).unwrap();

        let err = preflight(&config).expect_err("a live socket must refuse");
        let msg = err.to_string();
        assert!(msg.contains("in use by a running process"), "{msg}");
        assert!(
            msg.contains(&*config.socket_path.to_string_lossy()),
            "the refusal must name the socket path: {msg}"
        );
        assert!(
            std::os::unix::net::UnixStream::connect(&config.socket_path).is_ok(),
            "the live socket must be left alone"
        );
        cleanup(&dir);
    }

    /// A stale socket (nobody listening, e.g. a SIGKILLed daemon's) passes, and
    /// preflight never unlinks it: removal belongs to `bind_hardened`, after
    /// recovery.
    #[test]
    fn preflight_passes_a_stale_socket_and_leaves_it() {
        let (config, dir) = config_in("stale_socket", |_| {});
        drop(std::os::unix::net::UnixListener::bind(&config.socket_path).unwrap());
        assert!(config.socket_path.exists());

        preflight(&config).expect("a stale socket is not a conflict");
        assert!(
            config.socket_path.exists(),
            "preflight must never unlink anything"
        );
        cleanup(&dir);
    }

    /// `bind_hardened` refuses a non-socket entry at `socket_path` rather than
    /// unlink it, so preflight must refuse it first, before recovery.
    #[test]
    fn preflight_refuses_a_non_socket_at_socket_path() {
        let (config, dir) = config_in("non_socket", |_| {});
        std::fs::write(&config.socket_path, b"not a socket").unwrap();

        let err = preflight(&config).expect_err("a regular file must refuse");
        assert!(
            err.to_string().contains("non-socket entry already exists"),
            "{err}"
        );
        assert_eq!(std::fs::read(&config.socket_path).unwrap(), b"not a socket");
        cleanup(&dir);
    }

    /// A socket path the kernel cannot bind is refused before recovery, not
    /// at the late bind. The longest bindable length passes.
    #[test]
    fn preflight_refuses_a_socket_path_too_long_to_bind() {
        // Safety: sockaddr_un is plain old data; all-zero is a valid value.
        let max = unsafe { std::mem::zeroed::<libc::sockaddr_un>() }
            .sun_path
            .len()
            - 1;
        let base = crate::testutil::scratch_dir("preflight_long");
        let prefix = base.join("s");
        let fill = |len: usize| {
            let mut p = prefix.as_os_str().to_owned();
            p.push("x".repeat(len - prefix.as_os_str().len()));
            PathBuf::from(p)
        };

        let (config, dir) = config_in("too_long", |cli| cli.socket_path = Some(fill(max + 1)));
        let err = preflight(&config).expect_err("an unbindable path must refuse");
        assert!(err.to_string().contains("longer than"), "{err}");
        cleanup(&dir);

        let (config, dir) = config_in("max_len", |cli| cli.socket_path = Some(fill(max)));
        preflight(&config).expect("the longest bindable path must pass");
        let _l = std::os::unix::net::UnixListener::bind(&config.socket_path)
            .expect("and it really is bindable");
        cleanup(&dir);
        cleanup(&base);
    }

    /// Review Focus 3: systemd's `RuntimeDirectory` may be gone at start.
    /// Preflight lets it through to the bind's existing parent-missing error.
    #[test]
    fn preflight_passes_when_the_socket_parent_is_missing() {
        let (config, dir) = config_in("no_parent", |cli| {
            cli.socket_path = Some(
                crate::testutil::scratch_dir("preflight_no_parent_root")
                    .join("gone")
                    .join("weir.sock"),
            );
        });
        preflight(&config).expect("a missing parent is the bind's error, not preflight's");
        cleanup(&dir);
        cleanup(config.socket_path.parent().unwrap().parent().unwrap());
    }

    #[test]
    fn preflight_refuses_taken_metrics_port() {
        let taken = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let port = taken.local_addr().unwrap().port();
        let (config, dir) = config_in("metrics_taken", |cli| {
            cli.metrics_port = Some(port);
        });

        let err = preflight(&config).expect_err("a taken metrics port must refuse");
        let msg = err.to_string();
        assert!(msg.contains("failed to bind metrics endpoint"), "{msg}");
        assert!(
            msg.contains(&format!("127.0.0.1:{port}")),
            "the refusal must name the address: {msg}"
        );
        assert_eq!(err.kind(), io::ErrorKind::AddrInUse);
        cleanup(&dir);
    }

    /// The checks run in the spec's order, so with both a live socket and a
    /// taken metrics port the socket is what gets reported.
    #[test]
    fn socket_is_checked_before_metrics() {
        let taken = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let port = taken.local_addr().unwrap().port();
        let (config, dir) = config_in("order", |cli| {
            cli.metrics_port = Some(port);
        });
        let _live = std::os::unix::net::UnixListener::bind(&config.socket_path).unwrap();

        let err = preflight(&config).unwrap_err();
        assert!(
            err.to_string().contains("in use by a running process"),
            "{err}"
        );
        cleanup(&dir);
    }

    #[cfg(feature = "tls")]
    mod tls {
        use super::*;

        /// Writes a CA, a leaf signed by it, and the leaf's key into `dir`;
        /// returns (cert, key, ca) paths.
        fn write_tls_material(dir: &Path) -> (PathBuf, PathBuf, PathBuf) {
            let mut ca_params = rcgen::CertificateParams::new(vec![]).unwrap();
            ca_params.is_ca = rcgen::IsCa::Ca(rcgen::BasicConstraints::Unconstrained);
            let ca_key = rcgen::KeyPair::generate().unwrap();
            let ca_cert = ca_params.self_signed(&ca_key).unwrap();
            let leaf_params = rcgen::CertificateParams::new(vec!["weir-test".to_string()]).unwrap();
            let leaf_key = rcgen::KeyPair::generate().unwrap();
            let leaf = leaf_params.signed_by(&leaf_key, &ca_cert, &ca_key).unwrap();

            let (cert, key, ca) = (
                dir.join("cert.pem"),
                dir.join("key.pem"),
                dir.join("ca.pem"),
            );
            std::fs::write(&cert, leaf.pem()).unwrap();
            std::fs::write(&key, leaf_key.serialize_pem()).unwrap();
            std::fs::write(&ca, ca_cert.pem()).unwrap();
            (cert, key, ca)
        }

        fn tls_config(label: &str, tcp_port: u16, bad_material: bool) -> (Config, PathBuf) {
            let tls_dir = crate::testutil::scratch_dir(&format!("preflight_tlsmat_{label}"));
            let (cert, key, ca) = write_tls_material(&tls_dir);
            if bad_material {
                std::fs::write(&cert, b"-----BEGIN NOTHING-----\ngarbage\n").unwrap();
            }
            let (config, dir) = config_in(label, |cli| {
                cli.tcp_bind = Some(format!("127.0.0.1:{tcp_port}"));
                cli.tls_cert_path = Some(cert);
                cli.tls_key_path = Some(key);
                cli.tls_client_ca_path = Some(ca);
            });
            (config, dir)
        }

        /// Removes the TLS material's dir, found through the config.
        fn cleanup_tls(config: &Config) {
            if let Some(cert) = &config.tls_cert_path {
                cleanup(cert.parent().unwrap());
            }
        }

        #[test]
        fn preflight_passes_with_valid_tls_and_a_free_tcp_port() {
            let (config, dir) = tls_config("tls_ok", free_port(), false);
            preflight(&config).expect("valid TLS material and a free port must pass");
            std::net::TcpListener::bind(config.tcp_bind.unwrap())
                .expect("the tcp_bind address must be free again after preflight");
            cleanup(&dir);
            cleanup_tls(&config);
        }

        #[test]
        fn preflight_refuses_bad_tls_material() {
            let (config, dir) = tls_config("tls_bad", free_port(), true);
            let err = preflight(&config).expect_err("unloadable TLS material must refuse");
            let msg = err.to_string();
            assert!(msg.contains("failed to load TLS config"), "{msg}");
            assert!(
                msg.contains("cert.pem"),
                "the refusal must name the offending file: {msg}"
            );
            cleanup(&dir);
            cleanup_tls(&config);
        }

        #[test]
        fn preflight_refuses_taken_tcp_port() {
            let taken = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
            let port = taken.local_addr().unwrap().port();
            let (config, dir) = tls_config("tcp_taken", port, false);

            let err = preflight(&config).expect_err("a taken tcp_bind port must refuse");
            let msg = err.to_string();
            assert!(msg.contains("tcp_bind"), "{msg}");
            assert!(msg.contains(&format!("127.0.0.1:{port}")), "{msg}");
            assert_eq!(err.kind(), io::ErrorKind::AddrInUse);
            cleanup(&dir);
            cleanup_tls(&config);
        }
    }
}
