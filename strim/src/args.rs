use clap::{Parser, Subcommand};

#[derive(Parser, Debug, Clone)]
#[command(author, version, about, long_about = None)]
pub struct Cli {
    #[command(subcommand)]
    pub command: Commands,
}

#[derive(Debug, Clone, Subcommand)]
pub enum Commands {
    /// Run the search service HTTP server
    Server(ServerArgs),
}

#[derive(Debug, Clone, clap::Args)]
pub struct ServerArgs {
    #[arg(long, env = "NODE_ID", required = true)]
    pub node_id: String,

    #[arg(long, env = "POD_IP", required = true)]
    pub pod_ip: String,

    #[arg(long, env = "POD_NAME", required = true)]
    pub pod_name: String,

    #[arg(long, env = "POD_UID", required = true)]
    pub pod_uid: String,

    #[arg(long, env = "NAMESPACE", required = true)]
    pub namespace: String,

    #[arg(long, env = "PORT", required = true)]
    pub port: u16,

    #[clap(flatten)]
    pub target: Option<Target>,
}

#[derive(Debug, Clone, clap::Args)]
pub struct Target {
    #[arg(long, env = "TARGET_BUCKET", required = true)]
    pub bucket: String,

    #[arg(long, env = "TARGET_ENDPOINT", required = true)]
    pub endpoint: String,

    #[arg(long, env = "TARGET_REGION", required = true)]
    pub region: String,

    #[arg(long, env = "TARGET_SECRET", required = true)]
    pub secret: String,

    #[arg(long, env = "TARGET_KEY_PREFIX", required = true)]
    pub key_prefix: String,
}
