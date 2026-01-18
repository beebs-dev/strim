use anyhow::Result;
use clap::Parser;

mod app;
mod args;

#[tokio::main]
async fn main() -> Result<()> {
    synapse_common::init();
    let args = args::RunArgs::parse();
    app::run(args).await?;
    Ok(())
}
