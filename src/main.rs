use mux_proxy::app;
use std::process::ExitCode;

#[tokio::main]
async fn main() -> ExitCode {
    if let Err(err) = app::run().await {
        eprintln!("fatal: {err:#}");
        return ExitCode::from(1);
    }
    ExitCode::SUCCESS
}
