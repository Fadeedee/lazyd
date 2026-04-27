mod control;
mod error;
mod fanotify;
mod instance;
mod remote;

use std::path::PathBuf;

use clap::Parser;
use tracing::info;

use crate::control::ControlPlane;
use crate::error::Result;
use crate::fanotify::FanotifyBackend;
use crate::instance::InstanceRegistry;

#[derive(Debug, Parser)]
#[command(version, about = "Lazy EROFS range-read daemon")]
struct Cli {
    #[arg(long, env = "LAZYD_SOCKET", default_value = "/run/lazyd/lazyd.sock")]
    socket: PathBuf,

    #[arg(long, env = "LAZYD_DISABLE_FANOTIFY")]
    disable_fanotify: bool,
}

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .init();

    let cli = Cli::parse();

    let fanotify = if cli.disable_fanotify {
        None
    } else {
        match FanotifyBackend::new() {
            Ok(backend) => Some(backend),
            Err(err) => {
                tracing::warn!(%err, "fanotify unavailable; control plane remains usable");
                None
            }
        }
    };

    let registry = InstanceRegistry::new(fanotify.clone());
    if let Some(backend) = fanotify {
        backend.spawn_event_loop(registry.clone());
    }

    let control = ControlPlane::new(registry);
    info!(socket = %cli.socket.display(), "starting lazyd");
    control.serve(cli.socket).await
}
