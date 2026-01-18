use anyhow::Result;
use clap::Parser;

mod app;
mod args;

#[tokio::main]
async fn main() -> Result<()> {
    strim_common::init();
    app::run(args::RunArgs::parse()).await
}
