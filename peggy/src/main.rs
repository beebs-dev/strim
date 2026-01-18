use anyhow::Result;
use clap::Parser;

mod app;
mod args;

#[tokio::main]
async fn main() -> Result<()> {
    strim_common::init();
    let args = args::RunArgs::parse();
    app::run(args).await?;
    Ok(())
}
