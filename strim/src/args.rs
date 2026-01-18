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
    pub target: Option<TargetArgs>,
}

#[derive(Debug, Clone, clap::Args)]
pub struct TargetArgs {
    #[arg(
        long,
        env = "TARGET_BUCKET", 
        requires_all = [
            "endpoint",
            "region",
            "secret",
            "key_prefix"
        ]
    )]
    pub bucket: Option<String>,

    #[arg(long, env = "TARGET_ENDPOINT")]
    pub endpoint: Option<String>,

    #[arg(long, env = "TARGET_REGION")]
    pub region: Option<String>,

    #[arg(long, env = "TARGET_SECRET")]
    pub secret: Option<String>,

    #[arg(long, env = "TARGET_KEY_PREFIX")]
    pub key_prefix: Option<String>,
}

pub struct Target {
    pub bucket: String,
    pub endpoint: String,
    pub region: String,
    pub secret: String,
    pub key_prefix: String,
}

impl TryFrom<TargetArgs> for Target {
    type Error = anyhow::Error;

    fn try_from(args: TargetArgs) -> Result<Self, Self::Error> {
        Ok(Target {
            bucket: args
                .bucket
                .ok_or_else(|| anyhow::anyhow!("Target bucket is required"))?,
            endpoint: args
                .endpoint
                .ok_or_else(|| anyhow::anyhow!("Target endpoint is required"))?,
            region: args
                .region
                .ok_or_else(|| anyhow::anyhow!("Target region is required"))?,
            secret: args
                .secret
                .ok_or_else(|| anyhow::anyhow!("Target secret is required"))?,
            key_prefix: args
                .key_prefix
                .ok_or_else(|| anyhow::anyhow!("Target key_prefix is required"))?,
        })
    }
}
