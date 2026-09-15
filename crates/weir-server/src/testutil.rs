//! Shared test-only helpers.
//!
//! Compiled under `cfg(test)` and the `dst` feature only; nothing here ships
//! in a default daemon build.

/// A scratch directory whose mode does not depend on the process umask.
///
/// Every test in this crate that needs a directory must get it from here, and
/// the reason is a real cascade rather than tidiness.
///
/// `bind_hardened` tightens the umask to `0o177` **process-wide** for the
/// duration of its `bind(2)` — it has to, because that is the only portable way
/// to make the socket inode `0o600` from creation. `umask_test_lock` serialises
/// the socket tests against each other, but a lock cannot serialise the rest of
/// the crate's tests against them. A test that calls `create_dir_all` inside
/// that window gets a directory at mode `0o600`: no execute bit, so every file
/// created under it fails `EACCES`, and the failure lands nowhere near its
/// cause. One `cargo test --workspace` run on this machine produced 89 such
/// failures; the next produced one.
///
/// It also used to *persist*. These paths are pid-scoped, `create_dir_all` is a
/// no-op on an existing directory, and a failed run leaves the `0o600`
/// directory behind — so a later run reusing the pid failed at the same line
/// for a reason that was no longer present. The `chmod` runs on every call, not
/// only at creation, which is what heals a directory inherited in that state.
///
/// It does NOT remove an existing directory first. Several labels here are
/// shared by more than one test, and those tests ran concurrently against one
/// directory long before this helper existed; removing on entry turned that
/// benign sharing into tests deleting each other's files mid-run (50 failures
/// on the first try). Same path, same sharing, correct mode.
///
/// The mode is applied by `chmod` **after** creation, not by `DirBuilder::mode`:
/// `mkdir(2)` masks its mode argument through the umask unconditionally, so the
/// `DirBuilder` version still lands at `0o600` inside the window. `chmod(2)`
/// takes no mask, which makes it the only umask-independent way to get the mode
/// asked for. `umask_immune_dir_ignores_a_hostile_process_umask` in
/// `socket::tests` holds that distinction down.
#[cfg(unix)]
pub(crate) fn scratch_dir(label: &str) -> std::path::PathBuf {
    use std::os::unix::fs::PermissionsExt;
    let dir = std::env::temp_dir().join(format!("weir_{label}_{}", std::process::id()));
    std::fs::create_dir_all(&dir).expect("create scratch dir");
    std::fs::set_permissions(&dir, std::fs::Permissions::from_mode(0o700))
        .expect("chmod scratch dir");
    dir
}

/// Windows has no umask, so there is nothing to be immune to.
#[cfg(not(unix))]
pub(crate) fn scratch_dir(label: &str) -> std::path::PathBuf {
    let dir = std::env::temp_dir().join(format!("weir_{label}_{}", std::process::id()));
    std::fs::create_dir_all(&dir).expect("create scratch dir");
    dir
}

/// `create_dir_all` + an explicit `chmod`, for a directory a test creates
/// *inside* a [`scratch_dir`].
///
/// [`scratch_dir`] fixes the mode of the directory it returns, which is not
/// enough: a shard directory created under it a moment later goes through
/// `mkdir(2)` with the ambient umask, and inside `bind_hardened`'s window that
/// is `0o177` — so the parent is fine and the child is `0o600`, and everything
/// written into the child fails `EACCES`. Every nested directory a test creates
/// needs the same treatment as the root, which is why this exists rather than
/// bare `fs::create_dir_all(...).unwrap()`.
#[cfg(test)] // only test modules create nested directories; `dst` needs just `scratch_dir`
pub(crate) fn mkdir_p(path: impl AsRef<std::path::Path>) {
    let path = path.as_ref();
    std::fs::create_dir_all(path).unwrap_or_else(|e| panic!("create {}: {e}", path.display()));
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        std::fs::set_permissions(path, std::fs::Permissions::from_mode(0o700))
            .unwrap_or_else(|e| panic!("chmod {}: {e}", path.display()));
    }
}
