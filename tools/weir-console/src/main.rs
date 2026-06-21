use std::net::SocketAddr;
use std::path::PathBuf;
use clap::Parser;

#[derive(Parser)]
#[command(name = "weir-console", about = "Inspect a weir wab directory (WAB Explorer).")]
struct Args {
    /// The weir wab directory to inspect (read-only).
    #[arg(long)]
    wab_dir: PathBuf,
    /// Address to bind.
    #[arg(long, default_value = "127.0.0.1:8787")]
    bind: SocketAddr,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args = Args::parse();
    if !args.wab_dir.is_dir() {
        eprintln!("weir-console: --wab-dir {:?} is not a directory", args.wab_dir);
        std::process::exit(2);
    }
    let app = weir_console::server::router(args.wab_dir.clone());
    let listener = tokio::net::TcpListener::bind(args.bind).await?;
    println!("weir-console: WAB Explorer for {:?} at http://{}", args.wab_dir, args.bind);
    axum::serve(listener, app).await?;
    Ok(())
}
