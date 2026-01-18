#![allow(dead_code)]

mod args;
mod colors;
mod connection;
mod server;

use crate::{
    args::{Target, TargetArgs},
    colors::{FG1, FG2},
};
use anyhow::{Context, Result, bail};
use clap::Parser;
use connection::{Connection, ConnectionError, ReadResult};
use kube::Client;
use mio::net::{TcpListener, TcpStream};
use mio::*;
use owo_colors::OwoColorize;
use server::{Server, ServerResult};
use slab::Slab;
use std::net::SocketAddr;
use std::str::FromStr;
use std::time::SystemTime;
use std::{collections::HashSet, time::Duration};
use strim_common::shutdown::shutdown_signal;

const SERVER: Token = Token(usize::MAX - 1);

type ClosedTokens = HashSet<usize>;
enum EventResult {
    None,
    ReadResult(Box<ReadResult>),
    DisconnectConnection,
}

#[derive(Debug)]
struct PullOptions {
    host: String,
    app: String,
    stream: String,
    target: String,
}

#[derive(Debug)]
pub struct PushOptions {
    host: String,
    app: String,
    source_stream: String,
    target_stream: String,
}

#[derive(Debug)]
struct AppOptions {
    log_io: bool,
    push: Option<PushOptions>,
}

#[tokio::main]
async fn main() -> Result<()> {
    strim_common::init();
    let cli = args::Cli::parse();
    match cli.command {
        args::Commands::Server(args) => run_server(args).await,
    }
}

async fn run_server(args: args::ServerArgs) -> Result<()> {
    strim_common::metrics::maybe_spawn_metrics_server();
    let app_options = get_app_options(&args);

    // Create a kubernetes client using the default configuration.
    // In-cluster, the kubeconfig will be set by the service account.
    let client: Client = Client::try_default()
        .await
        .expect("Expected a valid KUBECONFIG environment variable.");

    let address = format!("0.0.0.0:{}", args.port).parse().unwrap();
    let listener = TcpListener::bind(&address).unwrap();
    let mut poll = Poll::new().unwrap();

    println!(
        "{}",
        "🟢 strim server listening for RTMP connections".green()
    );
    poll.register(&listener, SERVER, Ready::readable(), PollOpt::edge())
        .unwrap();

    let mut server = Server::new(
        client,
        args.pod_ip,
        args.pod_name,
        args.pod_uid,
        args.namespace,
        args.port,
        &app_options.push,
        args.target
            .map(|target: TargetArgs| -> Result<Option<Target>> {
                if target.bucket.as_ref().is_some_and(|b| !b.is_empty()) {
                    Ok(Some(
                        target
                            .try_into()
                            .context("Failed to parse target configuration")?,
                    ))
                } else {
                    Ok(None)
                }
            })
            .transpose()
            .context("Failed to parse target configuration")?
            .flatten(),
    );
    let mut connection_count = 1;
    let mut connections = Slab::new();

    let cancel = tokio_util::sync::CancellationToken::new();
    tokio::spawn({
        let cancel = cancel.clone();
        async move {
            shutdown_signal().await;
            println!("{}", "🛑 Shutdown signal received".red());
            cancel.cancel();
        }
    });

    // if let Some(ref pull) = app_options.pull {
    //     println!(
    //         "Starting pull client for rtmp://{}/{}/{}",
    //         pull.host, pull.app, pull.stream
    //     );

    //     let mut pull_host = pull.host.clone();
    //     if !pull_host.contains(":") {
    //         pull_host = format!("{}:{}", pull_host, args.port);
    //     }

    //     let addr = SocketAddr::from_str(&pull_host).unwrap();
    //     let stream = TcpStream::connect(&addr).unwrap();
    //     let connection = Connection::new(stream, connection_count, app_options.log_io, false);
    //     let token = connections.insert(connection);
    //     connection_count += 1;

    //     println!("Pull client started with connection id {}", token);
    //     connections[token].token = Some(Token(token));
    //     connections[token].register(&mut poll).unwrap();
    //     server.register_pull_client(
    //         token,
    //         pull.app.clone(),
    //         pull.stream.clone(),
    //         pull.target.clone(),
    //     );
    // }
    strim_common::signal_ready();

    let mut events = Events::with_capacity(1024);
    let mut outer_started_at = SystemTime::now();
    let mut inner_started_at;
    let mut _total_ns = 0;
    let mut _poll_count = 0_u32;

    loop {
        if cancel.is_cancelled() {
            bail!("Context cancelled");
        }
        poll.poll(&mut events, Some(Duration::from_millis(100)))
            .context("Failed to poll for RTMP events")?;
        inner_started_at = SystemTime::now();
        _poll_count += 1;
        for event in events.iter() {
            if cancel.is_cancelled() {
                bail!("Context cancelled");
            }
            let mut connections_to_close = ClosedTokens::new();
            match event.token() {
                SERVER => {
                    let (socket, _) = listener.accept().unwrap();
                    println!(
                        "{}{}{}{}",
                        "🔌 Accepted new connection • peer_addr=".color(FG1),
                        socket.peer_addr()?.to_string().color(FG2),
                        " • local_addr=".color(FG1),
                        socket.local_addr()?.to_string().color(FG2),
                    );
                    let connection =
                        Connection::new(socket, connection_count, app_options.log_io, true);
                    let token = connections.insert(connection);
                    connection_count += 1;
                    println!(
                        "{}{}",
                        "🔗 New connection • id=".color(FG1),
                        token.to_string().color(FG2),
                    );
                    connections[token].token = Some(Token(token));
                    connections[token].register(&mut poll).unwrap();
                }

                Token(token) => {
                    match process_event(&event.readiness(), &mut connections, token, &mut poll) {
                        EventResult::None => (),
                        EventResult::ReadResult(result) => {
                            match *result {
                                ReadResult::HandshakingInProgress => (),
                                ReadResult::NoBytesReceived => (),
                                ReadResult::BytesReceived { buffer, byte_count } => {
                                    connections_to_close = handle_read_bytes(
                                        &buffer[..byte_count],
                                        token,
                                        &mut server,
                                        &mut connections,
                                        &mut poll,
                                        &app_options,
                                        &mut connection_count,
                                    );
                                }

                                ReadResult::HandshakeCompleted { buffer, byte_count } => {
                                    // Server will understand that the first call to
                                    // handle_read_bytes signifies that handshaking is completed
                                    connections_to_close = handle_read_bytes(
                                        &buffer[..byte_count],
                                        token,
                                        &mut server,
                                        &mut connections,
                                        &mut poll,
                                        &app_options,
                                        &mut connection_count,
                                    );
                                }
                            }
                        }

                        EventResult::DisconnectConnection => {
                            connections_to_close.insert(token);
                        }
                    }
                }
            }

            for token in connections_to_close {
                println!(
                    "{}{}",
                    "⚠️ Closing connection • id=".yellow(),
                    token.to_string().yellow().dimmed()
                );
                connections.remove(token);
                server.notify_connection_closed(token);
            }
        }

        let inner_elapsed = inner_started_at.elapsed().unwrap();
        let outer_elapsed = outer_started_at.elapsed().unwrap();
        _total_ns += inner_elapsed.subsec_nanos();

        if outer_elapsed.as_secs() >= 10 {
            //let seconds_since_start = outer_started_at.elapsed().unwrap().as_secs();
            //let seconds_doing_work =
            //    (total_ns as f64) / (1000 as f64) / (1000 as f64) / (1000 as f64);
            //let percentage_doing_work =
            //    (seconds_doing_work / seconds_since_start as f64) * 100 as f64;
            //println!(
            //    "{}{}{}{}{}{}{}{}{}",
            //    "Spent ".color(FG1),
            //    (total_ns / 1000 / 1000).to_string().color(FG2),
            //    " ms (".color(FG1),
            //    (percentage_doing_work as u32).to_string().color(FG2),
            //    "% of time) doing work over ",
            //    seconds_since_start.to_string().color(FG2),
            //    " seconds (avg ".color(FG1),
            //    ((total_ns / poll_count) / 1000).to_string().color(FG2),
            //    " microseconds per iteration)".color(FG1),
            //);
            // Reset so each notification is per that interval
            _total_ns = 0;
            _poll_count = 0;
            outer_started_at = SystemTime::now();
        }
    }
}

fn get_app_options(_args: &args::ServerArgs) -> AppOptions {
    AppOptions {
        log_io: true,
        //pull: Some(PullOptions {
        //    host: format!("0.0.0.0:{}", args.port),
        //    app: "live".to_string(),
        //    stream: "stream".to_string(),
        //    target: "target_stream".to_string(),
        //}),
        push: Some(PushOptions {
            host: "localhost".to_string(),
            app: "live".to_string(),
            source_stream: "stream".to_string(),
            target_stream: "pushed_stream".to_string(),
        }),
    }
}

fn process_event(
    event: &Ready,
    connections: &mut Slab<Connection>,
    token: usize,
    poll: &mut Poll,
) -> EventResult {
    let connection = match connections.get_mut(token) {
        Some(connection) => connection,
        None => return EventResult::None,
    };

    if event.is_writable() {
        match connection.writable(poll) {
            Ok(_) => (),
            Err(error) => {
                println!("Error occurred while writing: {:?}", error);
                return EventResult::DisconnectConnection;
            }
        }
    }

    if event.is_readable() {
        match connection.readable(poll) {
            Ok(result) => return EventResult::ReadResult(Box::new(result)),
            Err(ConnectionError::SocketClosed) => return EventResult::DisconnectConnection,
            Err(x) => {
                println!("Error occurred: {:?}", x);
                return EventResult::DisconnectConnection;
            }
        }
    }

    EventResult::None
}

fn handle_read_bytes(
    bytes: &[u8],
    from_token: usize,
    server: &mut Server,
    connections: &mut Slab<Connection>,
    poll: &mut Poll,
    app_options: &AppOptions,
    connection_count: &mut usize,
) -> ClosedTokens {
    let mut closed_tokens = ClosedTokens::new();

    let mut server_results = match server.bytes_received(from_token, bytes) {
        Ok(results) => results,
        Err(error) => {
            println!("Input caused the following server error: {}", error);
            closed_tokens.insert(from_token);
            return closed_tokens;
        }
    };

    for result in server_results.drain(..) {
        match result {
            ServerResult::OutboundPacket {
                target_connection_id,
                packet,
            } => {
                if let Some(connection) = connections.get_mut(target_connection_id) {
                    connection.enqueue_packet(poll, packet).unwrap();
                }
            }

            ServerResult::DisconnectConnection { connection_id } => {
                closed_tokens.insert(connection_id);
            }

            ServerResult::StartPushing => {
                if let Some(ref push) = app_options.push {
                    println!(
                        "Starting push to rtmp://{}/{}/{}",
                        push.host, push.app, push.target_stream
                    );

                    let mut push_host = push.host.clone();
                    if !push_host.contains(":") {
                        push_host += ":1935";
                    }

                    let addr = SocketAddr::from_str(&push_host).unwrap();
                    let stream = TcpStream::connect(&addr).unwrap();
                    let connection =
                        Connection::new(stream, *connection_count, app_options.log_io, false);
                    let token = connections.insert(connection);
                    *connection_count += 1;

                    println!("Push client started with connection id {}", token);
                    connections[token].token = Some(Token(token));
                    connections[token].register(poll).unwrap();
                    server.register_push_client(token);
                }
            }
        }
    }

    closed_tokens
}
