use clap::Parser;
use std::net::SocketAddr;
use std::path::PathBuf;
use weir_console::ops::OpsConfig;

#[derive(Parser)]
#[command(
    name = "weir-console",
    about = "Inspect and operate a weir wab directory."
)]
struct Args {
    /// The weir wab directory to inspect (read-only Explorer + dead-letter target).
    #[arg(long)]
    wab_dir: PathBuf,
    /// Address to bind the console (localhost only by default).
    #[arg(long, default_value = "127.0.0.1:8787")]
    bind: SocketAddr,
    /// Daemon /metrics address for the Ops status header.
    #[arg(long, default_value = "127.0.0.1:9185")]
    metrics_addr: String,
    /// Daemon Unix socket used by `dl requeue` to re-push records.
    #[arg(long, default_value = "/run/weir/weir.sock")]
    socket: PathBuf,
    /// Path to the `weir-ctl` binary. Default: next to this exe, then PATH.
    #[arg(long)]
    weir_ctl: Option<PathBuf>,
    /// Disable all Ops mutations (requeue/drop + their previews).
    #[arg(long)]
    read_only: bool,
}

/// Resolve the weir-ctl binary: explicit flag, else a sibling of our own exe, else PATH.
fn resolve_weir_ctl(explicit: Option<PathBuf>) -> PathBuf {
    if let Some(p) = explicit {
        return p;
    }
    if let Ok(exe) = std::env::current_exe()
        && let Some(dir) = exe.parent()
    {
        let cand = dir.join("weir-ctl");
        if cand.is_file() {
            return cand;
        }
    }
    PathBuf::from("weir-ctl")
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args = Args::parse();
    if !args.wab_dir.is_dir() {
        eprintln!(
            "weir-console: --wab-dir {:?} is not a directory",
            args.wab_dir
        );
        std::process::exit(2);
    }

    let weir_ctl = resolve_weir_ctl(args.weir_ctl);
    // Probe once so a misconfigured weir-ctl is a clear warning at startup, not a
    // surprise on the first Ops action. The Explorer works without it.
    match std::process::Command::new(&weir_ctl)
        .arg("--version")
        .output()
    {
        Ok(o) if o.status.success() => {}
        _ => eprintln!(
            "weir-console: warning — could not run weir-ctl at {:?}; Ops actions will error \
             until you pass --weir-ctl <path> or put weir-ctl on PATH",
            weir_ctl
        ),
    }

    let ops = OpsConfig {
        weir_ctl,
        wab_dir: args.wab_dir.clone(),
        metrics_addr: args.metrics_addr,
        socket: args.socket,
        read_only: args.read_only,
    };
    let static_dir = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("static");
    let app = weir_console::server::router_with_ops(args.wab_dir.clone(), static_dir, ops);

    let listener = tokio::net::TcpListener::bind(args.bind).await?;
    println!(
        "weir-console for {:?} at http://{}{}",
        args.wab_dir,
        args.bind,
        if args.read_only { " (read-only)" } else { "" }
    );
    axum::serve(listener, app).await?;
    Ok(())
}
