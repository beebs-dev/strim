use anyhow::{Context, Result};
use aws_config::BehaviorVersion;
use aws_sdk_s3::{
    Client as S3Client,
    config::{Builder as S3Builder, Credentials as S3Credentials, Region},
    primitives::ByteStream,
    types::ObjectCannedAcl,
};
use notify::{
    Event, EventKind, RecommendedWatcher, RecursiveMode, Watcher,
    event::{CreateKind, DataChange, ModifyKind, RenameMode},
};
use owo_colors::{OwoColorize, Rgb};
use std::{ops::Deref, os::unix::fs::MetadataExt, path::PathBuf, sync::Arc, time::Duration};
use tokio::{select, sync::mpsc};
use tokio_util::sync::CancellationToken;

const FG1_COLOR: (u8, u8, u8) = (163, 83, 207);
const FG2_COLOR: (u8, u8, u8) = (90, 70, 130);
const FG1: Rgb = Rgb(FG1_COLOR.0, FG1_COLOR.1, FG1_COLOR.2);
const FG2: Rgb = Rgb(FG2_COLOR.0, FG2_COLOR.1, FG2_COLOR.2);

struct AppInner {
    client: S3Client,
    bucket: String,
    key_prefix: String,
    hls_dir: PathBuf,
}

#[derive(Clone)]
struct App {
    inner: Arc<AppInner>,
}

impl Deref for App {
    type Target = AppInner;

    fn deref(&self) -> &Self::Target {
        &self.inner
    }
}

impl App {
    pub fn new(client: S3Client, bucket: String, key_prefix: String, hls_dir: PathBuf) -> Self {
        Self {
            inner: Arc::new(AppInner {
                client,
                bucket,
                key_prefix,
                hls_dir,
            }),
        }
    }

    pub async fn handle_segment(&self, path: &PathBuf) -> Result<()> {
        // TODO: send to whisper
        println!("TODO: send segment to whisper: {:?}", path);
        self.upload_to_s3(path, true).await
    }

    pub async fn upload_to_s3(&self, path: &PathBuf, remove: bool) -> Result<()> {
        let meta = match tokio::fs::metadata(&path).await {
            Ok(m) => m,
            Err(_) => return Ok(()),
        };
        let relative_path = path
            .strip_prefix(&self.hls_dir)
            .context("Failed to get relative path for upload")?;
        let mimetype = match path.extension().and_then(|s| s.to_str()) {
            Some("m3u8") => "application/vnd.apple.mpegurl",
            Some("ts") => "video/mp2t",
            Some("vtt") => "text/vtt",
            _ => {
                eprintln!(
                    "{}",
                    format!(
                        "⚠️  Unknown file extension for file {:?}, defaulting to application/octet-stream",
                        path
                    )
                    .yellow(),
                );
                "application/octet-stream"
            }
        };
        let key = format!("{}{}", self.key_prefix, relative_path.to_string_lossy(),);
        let mut put = self.client.put_object();
        if path.extension().is_some_and(|s| s.to_str() == Some("m3u8")) {
            // Disable caching for playlists
            put = put.cache_control("no-cache");
        }
        let now = std::time::SystemTime::now();
        put.cache_control("no-cache")
            .bucket(&self.bucket)
            .set_acl(Some(ObjectCannedAcl::PublicRead))
            .set_content_type(Some(mimetype.into()))
            .key(key.clone())
            .body(ByteStream::from_path(path.clone()).await?)
            .send()
            .await
            .with_context(|| format!("Failed to upload file {:?} to S3", path))?;
        let size_mb = meta.size() as f64 / (1024.0 * 1024.0);
        let elapsed = now.elapsed().unwrap();
        let rate = size_mb / elapsed.as_secs_f64();
        let elapsed = humantime::format_duration(Duration::from_millis(elapsed.as_millis() as u64));
        println!(
            "{}{}{}{}{}{}{}{}{}{}{}{}",
            "📤 Uploaded ".color(FG1),
            path.to_str().unwrap().color(FG2),
            " to s3://".color(FG1),
            self.bucket.color(FG2),
            "/".color(FG1),
            key.color(FG2),
            " • size=".color(FG1),
            format!("{:.2} MiB", size_mb).color(FG2),
            " • elapsed=".color(FG1),
            elapsed.to_string().color(FG2),
            " • avg_rate=".color(FG1),
            format!("{:.2} MiB/s", rate,).color(FG2),
        );
        if remove {
            tokio::fs::remove_file(path.clone())
                .await
                .context("Failed to remove file after upload")?;
            println!(
                "{}{}{}{}",
                "♻️ Garbage collected local file • path=".color(FG1),
                path.to_str().unwrap().color(FG2),
                " • size=".color(FG1),
                format!("{:.2} MiB", size_mb).color(FG2),
            );
        }
        Ok(())
    }
}

pub async fn run(args: crate::args::RunArgs) -> Result<()> {
    seer_common::metrics::maybe_spawn_metrics_server(args.node_id.clone());
    let cancel = CancellationToken::new();
    let cancel_clone = cancel.clone();
    tokio::spawn(async move {
        synapse_common::shutdown::shutdown_signal().await;
        cancel_clone.cancel();
    });
    let s3_creds = S3Credentials::new(
        args.aws_access_key_id.clone(),
        args.aws_secret_access_key.clone(),
        None,
        None,
        "cli",
    );
    let region = args.s3_region.as_str();
    let mut builder = S3Builder::new()
        .credentials_provider(s3_creds)
        .region(Region::new(region.to_string()));
    let endpoint = args.s3_endpoint.as_deref();
    let behavior_version = BehaviorVersion::latest();
    if let Some(endpoint) = endpoint {
        builder = builder.endpoint_url(endpoint.to_string());
    }
    let config = builder
        .force_path_style(true) //.behavior_version_latest()
        .behavior_version(behavior_version)
        .build();
    let client = S3Client::from_conf(config);
    let hls_dir = PathBuf::from(args.hls_dir);
    let app = App::new(
        client,
        args.s3_bucket.clone(),
        args.s3_key_prefix.clone(),
        hls_dir.clone(),
    );
    println!("{}", "🚀 Starting peggy".green());
    let (tx, mut rx) = mpsc::channel::<PathBuf>(1024);
    let watch_dir = hls_dir.clone();
    let watcher_tx = tx.clone();
    let cancel_clone = cancel.clone();
    tokio::task::spawn(async move {
        let res: Result<()> = async {
            let mut watcher: RecommendedWatcher = RecommendedWatcher::new(
                move |res: Result<Event, notify::Error>| {
                    match res {
                        Ok(event) => {
                            // Filter interesting event kinds
                            match event.kind {
                                EventKind::Create(CreateKind::File)
                                | EventKind::Modify(ModifyKind::Name(RenameMode::To))
                                | EventKind::Modify(ModifyKind::Data(DataChange::Content)) => {
                                    for path in event.paths {
                                        let _ = watcher_tx.try_send(path);
                                    }
                                }
                                _ => {}
                            }
                        }
                        Err(err) => {
                            eprintln!("watch error: {err}");
                        }
                    }
                },
                Default::default(),
            )?;
            watcher.watch(&watch_dir, RecursiveMode::Recursive)?;
            // Keep this thread alive; watcher is driven by the callback
            loop {
                let sleep = tokio::time::sleep(Duration::from_secs(3600));
                tokio::pin!(sleep);
                tokio::select! {
                    _ = &mut sleep => {
                        // continue loop
                    }
                    _ = cancel_clone.cancelled() => {
                        println!("{}", "🛑 Stopping file watcher".red());
                        return Ok(());
                    }
                }
            }
        }
        .await;
        if let Err(e) = res {
            eprintln!("watcher task error: {e:?}");
        }
    });
    loop {
        select! {
            _ = cancel.cancelled() => {
                println!("{}", "🛑 Shutting down peggy".red());
                break;
            }
            value = rx.recv() => {
                match value {
                    Some(path) => {
                        if path.is_dir() {
                            // Ignore directories
                            continue;
                        }
                        if path.file_name().is_none_or(|s| s.to_string_lossy().starts_with('.')) {
                            // Ignore hidden files
                            continue;
                        }
                        if let Some(ext) = path.clone().extension().and_then(|s| s.to_str()) {
                            match ext {
                                "m3u8" => {
                                    if let Err(e) = app.upload_to_s3(&path, false).await {
                                        log_error(e.context(format!("Failed to upload playlist {:?}", path)));
                                    }
                                }
                                "ts" => {
                                    if let Err(e) = app.handle_segment(&path).await {
                                        log_error(e.context(format!("Failed to handle segment {:?}", path)));
                                    }
                                }
                                "vtt" => {
                                    if let Err(e) = app.upload_to_s3(&path, true).await {
                                        log_error(e.context(format!("Failed to upload text track {:?}", path)));
                                    }
                                }
                                _ => {}
                            }
                        }
                    }
                    None => break,
                }
            }
        }
    }
    Ok(())
}

fn log_error(err: anyhow::Error) {
    eprintln!("{} {}", "❌ Error:".red(), err);
    for cause in err.chain().skip(1) {
        eprintln!("    {} {}", "↳".red(), cause);
    }
}
