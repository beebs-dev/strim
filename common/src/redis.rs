use deadpool_redis::{Config as RedisPoolConfig, Pool};
use owo_colors::OwoColorize;
use redis::AsyncCommands;

pub async fn init_redis(args: &crate::args::RedisArgs) -> Pool {
    println!(
        "{}{}",
        "🔌 Connecting to Redis • url=".green(),
        args.url_redacted().green().dimmed(),
    );
    let pool = RedisPoolConfig::from_url(args.url())
        .create_pool(Some(deadpool_redis::Runtime::Tokio1))
        .expect("Failed to create Redis pool");
    pool.get()
        .await
        .expect("Failed to connect to Redis")
        .ping::<String>()
        .await
        .expect("Failed to ping Redis");
    pool
}
