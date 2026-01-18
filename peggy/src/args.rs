use clap::Parser;

#[derive(Parser, Debug, Clone)]
#[command(author, version, about, long_about = None)]
pub(crate) struct RunArgs {
    #[arg(long, env = "NODE_ID", required = true)]
    pub node_id: String,

    #[arg(long, env = "HLS_DIR", required = true)]
    pub hls_dir: String,

    #[arg(long, env = "AWS_ACCESS_KEY_ID", required = true)]
    pub aws_access_key_id: String,

    #[arg(long, env = "AWS_SECRET_ACCESS_KEY", required = true)]
    pub aws_secret_access_key: String,

    #[arg(long, env = "S3_BUCKET", required = true)]
    pub s3_bucket: String,

    #[arg(long, env = "S3_REGION", required = true)]
    pub s3_region: String,

    #[arg(long, env = "S3_ENDPOINT")]
    pub s3_endpoint: Option<String>,

    #[arg(long, env = "S3_KEY_PREFIX", required = true)]
    pub s3_key_prefix: String,
}
