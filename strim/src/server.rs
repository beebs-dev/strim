use super::{
    PushOptions,
    args::Target,
    colors::{FG1, FG2},
};
//use base64::{Engine as _, engine::general_purpose::URL_SAFE_NO_PAD};
use bytes::Bytes;
use k8s_openapi::apimachinery::pkg::apis::meta::v1::{ObjectMeta, OwnerReference};
use kube::{Api, Client};
use owo_colors::OwoColorize;
use rml_rtmp::chunk_io::Packet;
use rml_rtmp::sessions::{
    ClientSession, ClientSessionConfig, ClientSessionEvent, ClientSessionResult,
};
use rml_rtmp::sessions::{PublishRequestType, StreamMetadata};
use rml_rtmp::sessions::{
    ServerSession, ServerSessionConfig, ServerSessionEvent, ServerSessionResult,
};
use rml_rtmp::time::RtmpTimestamp;
use serde::Deserialize;
use sha2::Digest;
use slab::Slab;
use std::collections::{HashMap, HashSet};
use std::rc::Rc;
use strim_types::{Strim, StrimSource, StrimSpec, StrimTarget};

#[derive(Deserialize, Clone, Debug)]
struct StreamKeyPayload {
    pub stream_key: String,
    pub stable_id: String,
}

fn stream_hash(pod_ip: &str, stream_key: &str, entropy: usize) -> String {
    let mut hash = sha2::Sha256::new();
    hash.update(stream_key.as_bytes());
    hash.update(pod_ip.as_bytes());
    hash.update(entropy.to_le_bytes());
    format!("{:x}", hash.finalize())[..8].to_string()
}

fn pod_name(pod_ip: &str, stream_key: &str, entropy: usize) -> (String, String) {
    let hash = stream_hash(pod_ip, stream_key, entropy);
    (format!("ffmpeg-{}", hash), hash)
}

enum ReceivedDataType {
    Audio,
    Video,
}

enum InboundClientAction {
    Waiting,
    Publishing(String), // Publishing to a stream key
    Watching { stream_key: String, stream_id: u32 },
}

struct InboundClient {
    session: ServerSession,
    current_action: InboundClientAction,
    connection_id: usize,
    has_received_video_keyframe: bool,
}

impl InboundClient {
    fn get_active_stream_id(&self) -> Option<u32> {
        match self.current_action {
            InboundClientAction::Waiting => None,
            InboundClientAction::Publishing(_) => None,
            InboundClientAction::Watching {
                stream_key: _,
                stream_id,
            } => Some(stream_id),
        }
    }
}

#[derive(PartialEq, Clone, Debug)]
enum PullState {
    Handshaking,
    Connecting,
    Connected,
    Pulling,
}

struct PullClient {
    session: Option<ClientSession>,
    connection_id: usize,
    pull_app: String,
    pull_stream: String,
    pull_target_stream: String,
    state: PullState,
}

#[derive(PartialEq, Clone, Debug)]
enum PushState {
    Inactive,
    WaitingForConnection,
    Handshaking,
    Connecting,
    Connected,
    Pushing,
}

struct PushClient {
    session: Option<ClientSession>,
    connection_id: Option<usize>,
    push_app: String,
    push_source_stream: String,
    push_target_stream: String,
    state: PushState,
}

struct MediaChannel {
    publishing_client_id: Option<usize>,
    watching_client_ids: HashSet<usize>,
    metadata: Option<Rc<StreamMetadata>>,
    video_sequence_header: Option<Bytes>,
    audio_sequence_header: Option<Bytes>,
}

#[derive(Debug)]
pub enum ServerResult {
    DisconnectConnection {
        connection_id: usize,
    },
    OutboundPacket {
        target_connection_id: usize,
        packet: Packet,
    },
    StartPushing,
}

pub struct ResourceReference {
    pub name: String,
    pub namespace: String,
}

pub struct Server {
    client: Client,
    pod_ip: String,
    pod_name: String,
    pod_uid: String,
    port: u16,
    namespace: String,
    clients: Slab<InboundClient>,
    connection_to_client_map: HashMap<usize, usize>,
    connection_gc: HashMap<usize, ResourceReference>,
    channels: HashMap<String, MediaChannel>,
    pull_client: Option<PullClient>,
    push_client: Option<PushClient>,
    target: Option<Target>,
}

impl Server {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        client: Client,
        pod_ip: String,
        pod_name: String,
        pod_uid: String,
        namespace: String,
        port: u16,
        push_options: &Option<PushOptions>,
        target: Option<Target>,
    ) -> Server {
        let push_client = push_options.as_ref().map(|options| PushClient {
            push_app: options.app.clone(),
            push_source_stream: options.source_stream.clone(),
            push_target_stream: options.target_stream.clone(),
            connection_id: None,
            session: None,
            state: PushState::Inactive,
        });

        Server {
            client,
            pod_ip,
            pod_name,
            pod_uid,
            namespace,
            port,
            clients: Slab::with_capacity(1024),
            connection_to_client_map: HashMap::with_capacity(1024),
            channels: HashMap::new(),
            pull_client: None,
            push_client,
            connection_gc: HashMap::new(),
            target,
        }
    }

    #[allow(dead_code)]
    pub fn register_pull_client(
        &mut self,
        connection_id: usize,
        app: String,
        stream: String,
        target_stream: String,
    ) {
        // Pre-create the target channel.
        self.channels
            .entry(target_stream.clone())
            .or_insert(MediaChannel {
                publishing_client_id: Some(connection_id),
                watching_client_ids: HashSet::new(),
                metadata: None,
                video_sequence_header: None,
                audio_sequence_header: None,
            });

        self.pull_client = Some(PullClient {
            session: None,
            pull_app: app,
            pull_stream: stream,
            pull_target_stream: target_stream,
            state: PullState::Handshaking,
            connection_id,
        });
    }

    pub fn register_push_client(&mut self, connection_id: usize) {
        if let Some(ref mut client) = self.push_client {
            client.connection_id = Some(connection_id);
            client.state = PushState::Handshaking;
        }
    }

    pub fn bytes_received(
        &mut self,
        connection_id: usize,
        bytes: &[u8],
    ) -> Result<Vec<ServerResult>, String> {
        let mut server_results = Vec::new();

        let push_client_connection_id = self.push_client.as_ref().and_then(|c| c.connection_id);

        let pull_client_connection_id = self.pull_client.as_ref().map(|c| c.connection_id);

        if pull_client_connection_id
            .as_ref()
            .is_some_and(|id| *id == connection_id)
        {
            // These bytes were received by the current pull client

            let mut initial_session_results = Vec::new();
            if let Some(ref mut pull_client) = self.pull_client
                && pull_client.session.is_none()
            {
                let (session, session_results) =
                    ClientSession::new(ClientSessionConfig::new()).unwrap();
                pull_client.session = Some(session);

                for result in session_results {
                    initial_session_results.push(result);
                }
            }

            let session_results = match self
                .pull_client
                .as_mut()
                .unwrap()
                .session
                .as_mut()
                .unwrap()
                .handle_input(bytes)
            {
                Ok(results) => results,
                Err(error) => return Err(error.to_string()),
            };

            if !initial_session_results.is_empty() {
                self.handle_push_session_results(initial_session_results, &mut server_results);
            }

            self.handle_pull_session_results(session_results, &mut server_results);
        } else if push_client_connection_id
            .as_ref()
            .is_some_and(|id| *id == connection_id)
        {
            // These bytes were received by the current push client
            let mut initial_session_results = Vec::new();

            let session_results = if let Some(ref mut push_client) = self.push_client {
                if push_client.session.is_none() {
                    let (session, session_results) =
                        ClientSession::new(ClientSessionConfig::new()).unwrap();
                    push_client.session = Some(session);

                    for result in session_results {
                        initial_session_results.push(result);
                    }
                }

                match push_client.session.as_mut().unwrap().handle_input(bytes) {
                    Ok(results) => results,
                    Err(error) => return Err(error.to_string()),
                }
            } else {
                Vec::new()
            };

            if !initial_session_results.is_empty() {
                self.handle_push_session_results(initial_session_results, &mut server_results);
            }

            self.handle_push_session_results(session_results, &mut server_results);
        } else {
            // Since the pull client did not send these bytes, map it to an inbound client
            if !self.connection_to_client_map.contains_key(&connection_id) {
                let config = ServerSessionConfig::new();
                let (session, initial_session_results) = match ServerSession::new(config) {
                    Ok(results) => results,
                    Err(error) => return Err(error.to_string()),
                };

                self.handle_server_session_results(
                    connection_id,
                    initial_session_results,
                    &mut server_results,
                );
                let client = InboundClient {
                    session,
                    connection_id,
                    current_action: InboundClientAction::Waiting,
                    has_received_video_keyframe: false,
                };

                let client_id = self.clients.insert(client);
                self.connection_to_client_map
                    .insert(connection_id, client_id);
            }

            let client_results;
            {
                let client_id = self.connection_to_client_map.get(&connection_id).unwrap();
                let client = self.clients.get_mut(*client_id).unwrap();
                client_results = match client.session.handle_input(bytes) {
                    Ok(results) => results,
                    Err(error) => return Err(error.to_string()),
                };
            }

            self.handle_server_session_results(connection_id, client_results, &mut server_results);
        }

        Ok(server_results)
    }

    pub fn notify_connection_closed(&mut self, connection_id: usize) {
        if let Some(r) = self.connection_gc.remove(&connection_id) {
            let strim_api: Api<Strim> = Api::namespaced(self.client.clone(), &r.namespace);
            tokio::spawn(async move {
                match strim_api.delete(&r.name, &Default::default()).await {
                    Ok(_) => {
                        println!(
                            "{}{}{}{}",
                            "🗑️ Successfully deleted Strim resource • namespace=".color(FG1),
                            r.namespace.color(FG2),
                            " • name=".color(FG1),
                            r.name.color(FG2),
                        );
                    }
                    Err(kube::Error::Api(ae)) if ae.code == 404 => {
                        println!(
                            "{}{}{}{}",
                            "💨 Strim resource not found when attempting to delete • namespace="
                                .yellow(),
                            r.namespace.yellow().dimmed(),
                            " • name=".yellow(),
                            r.name.yellow().dimmed(),
                        );
                    }
                    Err(e) => {
                        println!(
                            "{}{}{}{}{}{}",
                            "❌ Failed to delete Strim resource • namespace=".red(),
                            r.namespace.red().dimmed(),
                            " • name=".red(),
                            r.name.red().dimmed(),
                            " • error=".red(),
                            format!("{:?}", e).red().dimmed(),
                        );
                    }
                }
            });
        }
        if self
            .pull_client
            .as_ref()
            .is_some_and(|c| c.connection_id == connection_id)
        {
            self.pull_client = None;
        } else {
            match self.connection_to_client_map.remove(&connection_id) {
                None => (),
                Some(client_id) => {
                    let client = self.clients.remove(client_id);
                    match client.current_action {
                        InboundClientAction::Publishing(stream_key) => {
                            self.publishing_ended(stream_key)
                        }
                        InboundClientAction::Watching {
                            stream_key,
                            stream_id: _,
                        } => self.play_ended(client_id, stream_key),
                        InboundClientAction::Waiting => (),
                    }
                }
            }
        }
    }

    fn handle_server_session_results(
        &mut self,
        executed_connection_id: usize,
        session_results: Vec<ServerSessionResult>,
        server_results: &mut Vec<ServerResult>,
    ) {
        for result in session_results {
            match result {
                ServerSessionResult::OutboundResponse(packet) => {
                    server_results.push(ServerResult::OutboundPacket {
                        target_connection_id: executed_connection_id,
                        packet,
                    })
                }

                ServerSessionResult::RaisedEvent(event) => {
                    self.handle_raised_event(executed_connection_id, event, server_results)
                }

                x => eprintln!("{}", format!("Server result received: {:?}", x).yellow()),
            }
        }
    }

    fn handle_raised_event(
        &mut self,
        executed_connection_id: usize,
        event: ServerSessionEvent,
        server_results: &mut Vec<ServerResult>,
    ) {
        match event {
            ServerSessionEvent::ConnectionRequested {
                request_id,
                app_name,
            } => {
                self.handle_connection_requested(
                    executed_connection_id,
                    request_id,
                    app_name,
                    server_results,
                );
            }

            ServerSessionEvent::PublishStreamRequested {
                request_id,
                app_name,
                stream_key,
                mode: _,
            } => {
                self.handle_publish_requested(
                    executed_connection_id,
                    request_id,
                    app_name,
                    stream_key,
                    server_results,
                );
            }

            ServerSessionEvent::PublishStreamFinished {
                app_name: _,
                stream_key,
            } => {
                let _ = stream_key;
            }

            ServerSessionEvent::PlayStreamRequested {
                request_id,
                app_name,
                stream_key,
                start_at: _,
                duration: _,
                reset: _,
                stream_id,
            } => {
                self.handle_play_requested(
                    executed_connection_id,
                    request_id,
                    app_name,
                    stream_key,
                    stream_id,
                    server_results,
                );
            }

            ServerSessionEvent::StreamMetadataChanged {
                app_name,
                stream_key,
                metadata,
            } => {
                self.handle_metadata_received(app_name, stream_key, metadata, server_results);
            }

            ServerSessionEvent::VideoDataReceived {
                app_name: _,
                stream_key,
                data,
                timestamp,
            } => {
                self.handle_audio_video_data_received(
                    stream_key,
                    timestamp,
                    data,
                    ReceivedDataType::Video,
                    server_results,
                );
            }

            ServerSessionEvent::AudioDataReceived {
                app_name: _,
                stream_key,
                data,
                timestamp,
            } => {
                self.handle_audio_video_data_received(
                    stream_key,
                    timestamp,
                    data,
                    ReceivedDataType::Audio,
                    server_results,
                );
            }
            _ => {
                //eprintln!(
                //    "{}{}{}{}",
                //    "⚡ Ignoring unhandled event • connection_id={}".color(FG1),
                //    executed_connection_id.color(FG2),
                //    " • event=".color(FG1),
                //    format!("{:?}", event).color(FG2),
                //);
            }
        }
    }

    fn handle_connection_requested(
        &mut self,
        requested_connection_id: usize,
        request_id: u32,
        app_name: String,
        server_results: &mut Vec<ServerResult>,
    ) {
        eprintln!(
            "{}{}{}{}",
            "⚡ Connection to app requested • connection_id=".color(FG1),
            requested_connection_id.color(FG2),
            " • app_name=".color(FG1),
            app_name.color(FG2),
        );
        let accept_result;
        {
            let client_id = self
                .connection_to_client_map
                .get(&requested_connection_id)
                .unwrap();
            let client = self.clients.get_mut(*client_id).unwrap();
            accept_result = client.session.accept_request(request_id);
        }

        match accept_result {
            Err(error) => {
                eprintln!(
                    "{}",
                    format!(
                        "❌ Error occurred accepting connection request: {:?}",
                        error
                    )
                    .red()
                );
                server_results.push(ServerResult::DisconnectConnection {
                    connection_id: requested_connection_id,
                })
            }

            Ok(results) => {
                self.handle_server_session_results(
                    requested_connection_id,
                    results,
                    server_results,
                );
            }
        }
    }

    fn handle_publish_requested(
        &mut self,
        requested_connection_id: usize,
        request_id: u32,
        app_name: String,
        stream_key: String,
        server_results: &mut Vec<ServerResult>,
    ) {
        //let Ok(stream_key) = URL_SAFE_NO_PAD.decode(stream_key.as_bytes()) else {
        //    eprintln!(
        //        "{}{}{}{}",
        //        "❌ Publish request stream key base64 decode failed • connection_id=".red(),
        //        requested_connection_id.red().dimmed(),
        //        " • stream_key=".red(),
        //        stream_key.red().dimmed(),
        //    );
        //    return;
        //};
        //let Ok(StreamKeyPayload {
        //    stream_key,
        //    stable_id,
        //}) = serde_json::from_slice(&stream_key)
        //else {
        //    eprintln!(
        //        "{}{}{}{}",
        //        "❌ Publish request stream key JSON decode failed • connection_id=".red(),
        //        requested_connection_id.red().dimmed(),
        //        " • stream_key=".red(),
        //        String::from_utf8_lossy(&stream_key).red().dimmed(),
        //    );
        //    return;
        //};
        eprintln!(
            "{}{}{}{}",
            "📢 Publish requested • app_name=".color(FG1),
            app_name.color(FG2),
            " • stream_key=".color(FG1),
            stream_key.color(FG2),
        );
        println!(
            "{}",
            format!(
                "TODO: announce publish request for stream key '{}' over redis, requested_connection_id={}, request_id={}",
                stream_key,
                requested_connection_id,
                request_id
            )
            .magenta()
        );
        match self.channels.get(&stream_key) {
            None => (),
            Some(channel) => match channel.publishing_client_id {
                None => (),
                Some(_) => {
                    eprintln!(
                        "{}",
                        format!(
                            "Stream key '{}' is already being published to, rejecting publish request",
                            stream_key
                        )
                        .red()
                    );
                    server_results.push(ServerResult::DisconnectConnection {
                        connection_id: requested_connection_id,
                    });
                    return;
                }
            },
        }

        let accept_result;
        {
            let client_id = self
                .connection_to_client_map
                .get(&requested_connection_id)
                .unwrap();
            let client = self.clients.get_mut(*client_id).unwrap();
            client.current_action = InboundClientAction::Publishing(stream_key.clone());

            let channel = self
                .channels
                .entry(stream_key.clone())
                .or_insert(MediaChannel {
                    publishing_client_id: None,
                    watching_client_ids: HashSet::new(),
                    metadata: None,
                    video_sequence_header: None,
                    audio_sequence_header: None,
                });

            channel.publishing_client_id = Some(*client_id);
            accept_result = client.session.accept_request(request_id);
        }

        match accept_result {
            Err(error) => {
                eprintln!(
                    "{}",
                    format!("Error occurred accepting publish request: {:?}", error).red()
                );
                server_results.push(ServerResult::DisconnectConnection {
                    connection_id: requested_connection_id,
                })
            }

            Ok(results) => {
                if let Some(ref mut client) = self.push_client
                    && client.state == PushState::Inactive
                {
                    if app_name == client.push_app && stream_key == client.push_source_stream {
                        eprintln!(
                            "{}",
                            "✔️ Publishing on the push source stream key!".color(FG1)
                        );
                        client.state = PushState::WaitingForConnection;
                        server_results.push(ServerResult::StartPushing)
                    } else {
                        eprintln!(
                            "{}",
                            "⚠️ Not publishing on the push source stream key!".yellow()
                        );
                    }
                }

                self.handle_server_session_results(
                    requested_connection_id,
                    results,
                    server_results,
                );
            }
        }
        let random_usize = rand::random::<u64>() as usize;
        let (name, _hash) = pod_name(&self.pod_ip, &stream_key, random_usize);
        let target = match self.target {
            Some(ref target) => target,
            None => return, // no s3 upload``
        };
        self.connection_gc.insert(
            requested_connection_id,
            ResourceReference {
                name: name.clone(),
                namespace: self.namespace.clone(),
            },
        );
        let strim_resource = Strim {
            metadata: ObjectMeta {
                name: Some(name),
                namespace: Some(self.namespace.clone()),
                annotations: None,
                labels: None,
                owner_references: Some(vec![OwnerReference {
                    api_version: "v1".to_string(),
                    kind: "Pod".to_string(),
                    name: self.pod_name.clone(),
                    uid: self.pod_uid.clone(),
                    controller: Some(true),
                    block_owner_deletion: None,
                }]),
                ..Default::default()
            },
            spec: StrimSpec {
                source: StrimSource {
                    internal_url: format!(
                        "rtmp://{}:{}/live/{}",
                        self.pod_ip, self.port, stream_key
                    ),
                },
                target: StrimTarget {
                    bucket: target.bucket.clone(),
                    endpoint: target.endpoint.clone(),
                    region: target.region.clone(),
                    secret: target.secret.clone(),
                    key_prefix: format!("{}/", stream_key),
                },
                transcribe: true,
            },
            ..Default::default()
        };
        let strim_api: Api<Strim> = Api::namespaced(self.client.clone(), &self.namespace);
        tokio::spawn(async move {
            match strim_api.create(&Default::default(), &strim_resource).await {
                Ok(_) => {
                    println!(
                        "{}{}{}{}",
                        "✔️ Successfully created Strim resource • namespace=".color(FG1),
                        strim_resource
                            .metadata
                            .namespace
                            .as_ref()
                            .unwrap_or(&"<unknown>".to_string())
                            .color(FG2),
                        " • name=".color(FG1),
                        strim_resource
                            .metadata
                            .name
                            .as_ref()
                            .unwrap_or(&"<unknown>".to_string())
                            .color(FG2)
                    );
                }
                Err(e) => {
                    eprintln!(
                        "{}{}{}{}{}{}",
                        "❌ Failed to create Strim resource • namespace=".red(),
                        strim_resource
                            .metadata
                            .namespace
                            .as_ref()
                            .unwrap_or(&"<unknown>".to_string())
                            .red()
                            .dimmed(),
                        " • name=".red(),
                        strim_resource
                            .metadata
                            .name
                            .as_ref()
                            .unwrap_or(&"<unknown>".to_string())
                            .red()
                            .dimmed(),
                        " • error=".red(),
                        format!("{:?}", e).red().dimmed(),
                    );
                }
            }
        });
    }

    fn handle_play_requested(
        &mut self,
        requested_connection_id: usize,
        request_id: u32,
        app_name: String,
        stream_key: String,
        stream_id: u32,
        server_results: &mut Vec<ServerResult>,
    ) {
        println!(
            "{}{}{}{}{}{}{}{}",
            "📥 Play requested • app_name=".color(FG1),
            app_name.color(FG2),
            " • stream_key=".color(FG1),
            stream_key.color(FG2),
            " • requested_connection_id=".color(FG1),
            requested_connection_id.color(FG2),
            " • request_id=".color(FG1),
            request_id.color(FG2),
        );
        let accept_result;
        {
            let client_id = self
                .connection_to_client_map
                .get(&requested_connection_id)
                .unwrap();
            let client = self.clients.get_mut(*client_id).unwrap();
            client.current_action = InboundClientAction::Watching {
                stream_key: stream_key.clone(),
                stream_id,
            };

            let channel = self
                .channels
                .entry(stream_key.clone())
                .or_insert(MediaChannel {
                    publishing_client_id: None,
                    watching_client_ids: HashSet::new(),
                    metadata: None,
                    video_sequence_header: None,
                    audio_sequence_header: None,
                });

            channel.watching_client_ids.insert(*client_id);
            accept_result = match client.session.accept_request(request_id) {
                Err(error) => Err(error),
                Ok(mut results) => {
                    // If the channel already has existing metadata, send that to the new client
                    // so they have up to date info
                    if let Some(metadata) = channel.metadata.as_ref() {
                        let packet = match client.session.send_metadata(stream_id, metadata) {
                            Ok(packet) => packet,
                            Err(error) => {
                                eprintln!(
                                        "{}",
                                        format!(
                                            "❌ Error occurred sending existing metadata to new client: {:?}",
                                            error
                                        ).red(),
                                    );
                                server_results.push(ServerResult::DisconnectConnection {
                                    connection_id: requested_connection_id,
                                });

                                return;
                            }
                        };

                        results.push(ServerSessionResult::OutboundResponse(packet));
                    }

                    // If the channel already has sequence headers, send them
                    match channel.video_sequence_header {
                        None => (),
                        Some(ref data) => {
                            let packet = match client.session.send_video_data(
                                stream_id,
                                data.clone(),
                                RtmpTimestamp::new(0),
                                false,
                            ) {
                                Ok(packet) => packet,
                                Err(error) => {
                                    eprintln!(
                                        "{}",
                                        format!(
                                            "❌ Error occurred sending video header to new client: {:?}",
                                            error
                                        ).red(),
                                    );
                                    server_results.push(ServerResult::DisconnectConnection {
                                        connection_id: requested_connection_id,
                                    });

                                    return;
                                }
                            };

                            results.push(ServerSessionResult::OutboundResponse(packet));
                        }
                    }

                    match channel.audio_sequence_header {
                        None => (),
                        Some(ref data) => {
                            let packet = match client.session.send_audio_data(
                                stream_id,
                                data.clone(),
                                RtmpTimestamp::new(0),
                                false,
                            ) {
                                Ok(packet) => packet,
                                Err(error) => {
                                    eprintln!(
                                        "{}",
                                        format!(
                                            "❌ Error occurred sending audio header to new client: {:?}",
                                            error
                                        ).red(),
                                    );
                                    server_results.push(ServerResult::DisconnectConnection {
                                        connection_id: requested_connection_id,
                                    });

                                    return;
                                }
                            };

                            results.push(ServerSessionResult::OutboundResponse(packet));
                        }
                    }

                    Ok(results)
                }
            }
        }

        match accept_result {
            Err(error) => {
                eprintln!(
                    "{}",
                    format!("❌ Error occurred accepting playback request: {:?}", error).red(),
                );
                server_results.push(ServerResult::DisconnectConnection {
                    connection_id: requested_connection_id,
                });
            }

            Ok(results) => {
                self.handle_server_session_results(
                    requested_connection_id,
                    results,
                    server_results,
                );
            }
        }
    }

    fn handle_metadata_received(
        &mut self,
        app_name: String,
        stream_key: String,
        metadata: StreamMetadata,
        server_results: &mut Vec<ServerResult>,
    ) {
        println!(
            "{}{}{}{}",
            "🆕 Metadata received • app_name=".color(FG1),
            app_name.color(FG2),
            " • stream_key=".color(FG1),
            stream_key.color(FG2),
        );
        let channel = match self.channels.get_mut(&stream_key) {
            Some(channel) => channel,
            None => return,
        };

        let metadata = Rc::new(metadata);
        channel.metadata = Some(metadata.clone());

        // Send the metadata to all current watchers
        for client_id in &channel.watching_client_ids {
            let client = match self.clients.get_mut(*client_id) {
                Some(client) => client,
                None => continue,
            };

            let active_stream_id = match client.get_active_stream_id() {
                Some(stream_id) => stream_id,
                None => continue,
            };

            match client.session.send_metadata(active_stream_id, &metadata) {
                Ok(packet) => server_results.push(ServerResult::OutboundPacket {
                    target_connection_id: client.connection_id,
                    packet,
                }),

                Err(error) => {
                    eprintln!(
                        "{}",
                        format!(
                            "❌ Error sending metadata to client on connection id {}: {:?}",
                            client.connection_id, error
                        )
                        .red(),
                    );
                    server_results.push(ServerResult::DisconnectConnection {
                        connection_id: client.connection_id,
                    });
                }
            }
        }
    }

    fn handle_audio_video_data_received(
        &mut self,
        stream_key: String,
        timestamp: RtmpTimestamp,
        data: Bytes,
        data_type: ReceivedDataType,
        server_results: &mut Vec<ServerResult>,
    ) {
        {
            let channel = match self.channels.get_mut(&stream_key) {
                Some(channel) => channel,
                None => return,
            };

            // If this is an audio or video sequence header we need to save it, so it can be
            // distributed to any late coming watchers
            match data_type {
                ReceivedDataType::Video => {
                    if is_video_sequence_header(data.clone()) {
                        channel.video_sequence_header = Some(data.clone());
                    }
                }

                ReceivedDataType::Audio => {
                    if is_audio_sequence_header(data.clone()) {
                        channel.audio_sequence_header = Some(data.clone());
                    }
                }
            }

            for client_id in &channel.watching_client_ids {
                let client = match self.clients.get_mut(*client_id) {
                    Some(client) => client,
                    None => continue,
                };

                let active_stream_id = match client.get_active_stream_id() {
                    Some(stream_id) => stream_id,
                    None => continue,
                };

                let should_send_to_client = match data_type {
                    ReceivedDataType::Video => {
                        client.has_received_video_keyframe
                            || (is_video_sequence_header(data.clone())
                                || is_video_keyframe(data.clone()))
                    }

                    ReceivedDataType::Audio => {
                        client.has_received_video_keyframe || is_audio_sequence_header(data.clone())
                    }
                };

                if !should_send_to_client {
                    continue;
                }

                let send_result = match data_type {
                    ReceivedDataType::Audio => client.session.send_audio_data(
                        active_stream_id,
                        data.clone(),
                        timestamp,
                        true,
                    ),
                    ReceivedDataType::Video => {
                        if is_video_keyframe(data.clone()) {
                            client.has_received_video_keyframe = true;
                        }

                        client.session.send_video_data(
                            active_stream_id,
                            data.clone(),
                            timestamp,
                            true,
                        )
                    }
                };

                match send_result {
                    Ok(packet) => server_results.push(ServerResult::OutboundPacket {
                        target_connection_id: client.connection_id,
                        packet,
                    }),

                    Err(error) => {
                        eprintln!(
                            "{}",
                            format!(
                                "❌ Error sending a/v data to client on connection id {}: {:?}",
                                client.connection_id, error
                            )
                            .red(),
                        );
                        server_results.push(ServerResult::DisconnectConnection {
                            connection_id: client.connection_id,
                        });
                    }
                }
            }
        }

        let mut push_results = Vec::new();
        {
            if let Some(ref mut client) = self.push_client
                && client.state == PushState::Pushing
            {
                let result = match data_type {
                    ReceivedDataType::Video => client.session.as_mut().unwrap().publish_video_data(
                        data.clone(),
                        timestamp,
                        true,
                    ),

                    ReceivedDataType::Audio => client.session.as_mut().unwrap().publish_audio_data(
                        data.clone(),
                        timestamp,
                        true,
                    ),
                };

                match result {
                    Ok(client_result) => push_results.push(client_result),
                    Err(error) => {
                        eprintln!(
                            "{}",
                            format!("❌ Error pushing a/v data: {:?}", error).red(),
                        );
                    }
                }
            }
        }

        if !push_results.is_empty() {
            self.handle_push_session_results(push_results, server_results);
        }
    }

    fn publishing_ended(&mut self, stream_key: String) {
        let channel = match self.channels.get_mut(&stream_key) {
            Some(channel) => channel,
            None => return,
        };

        channel.publishing_client_id = None;
        channel.metadata = None;
    }

    fn play_ended(&mut self, client_id: usize, stream_key: String) {
        let channel = match self.channels.get_mut(&stream_key) {
            Some(channel) => channel,
            None => return,
        };

        channel.watching_client_ids.remove(&client_id);
    }

    fn handle_pull_session_results(
        &mut self,
        session_results: Vec<ClientSessionResult>,
        server_results: &mut Vec<ServerResult>,
    ) {
        let mut new_results = Vec::new();
        let mut events = Vec::new();
        if let Some(ref mut client) = self.pull_client {
            for result in session_results {
                match result {
                    ClientSessionResult::OutboundResponse(packet) => {
                        server_results.push(ServerResult::OutboundPacket {
                            target_connection_id: client.connection_id,
                            packet,
                        });
                    }

                    ClientSessionResult::RaisedEvent(event) => {
                        events.push(event);
                    }

                    x => eprintln!("{}", format!("Client result received: {:?}", x).yellow()),
                }
            }

            if client.state == PullState::Handshaking {
                // Since this was called we know we are no longer handshaking, so we need to
                // initiate the connect to the RTMP app
                client.state = PullState::Connecting;

                let result = client
                    .session
                    .as_mut()
                    .unwrap()
                    .request_connection(client.pull_app.clone())
                    .unwrap();
                new_results.push(result);
            }
        }

        if !new_results.is_empty() {
            self.handle_pull_session_results(new_results, server_results);
        }

        for event in events {
            match event {
                ClientSessionEvent::ConnectionRequestAccepted => {
                    self.handle_pull_connection_accepted_event(server_results);
                }

                ClientSessionEvent::PlaybackRequestAccepted => {
                    self.handle_pull_playback_accepted_event(server_results);
                }

                ClientSessionEvent::VideoDataReceived { data, timestamp } => {
                    self.handle_pull_audio_video_data_received(
                        data,
                        ReceivedDataType::Video,
                        timestamp,
                        server_results,
                    );
                }

                ClientSessionEvent::AudioDataReceived { data, timestamp } => {
                    self.handle_pull_audio_video_data_received(
                        data,
                        ReceivedDataType::Audio,
                        timestamp,
                        server_results,
                    );
                }

                ClientSessionEvent::StreamMetadataReceived { metadata } => {
                    self.handle_pull_metadata_received(metadata, server_results);
                }

                x => eprintln!("{}", format!("❌ Unhandled event raised: {:?}", x).yellow()),
            }
        }
    }

    fn handle_pull_connection_accepted_event(&mut self, server_results: &mut Vec<ServerResult>) {
        let mut new_results = Vec::new();
        if let Some(ref mut client) = self.pull_client {
            eprintln!(
                "{}",
                format!("Pull accepted for app '{}'", client.pull_app).yellow()
            );
            client.state = PullState::Connected;

            let result = client
                .session
                .as_mut()
                .unwrap()
                .request_playback(client.pull_stream.clone())
                .unwrap();
            let mut results = vec![result];
            new_results.append(&mut results);
        }

        if !new_results.is_empty() {
            self.handle_pull_session_results(new_results, server_results);
        }
    }

    fn handle_pull_playback_accepted_event(&mut self, _server_results: &mut Vec<ServerResult>) {
        if let Some(ref mut client) = self.pull_client {
            println!(
                "{}",
                format!("Playback accepted for stream '{}'", client.pull_stream).green()
            );
            client.state = PullState::Pulling;
        }
    }

    fn handle_pull_audio_video_data_received(
        &mut self,
        data: Bytes,
        data_type: ReceivedDataType,
        timestamp: RtmpTimestamp,
        server_results: &mut Vec<ServerResult>,
    ) {
        let stream_key = match self.pull_client {
            Some(ref client) => client.pull_target_stream.clone(),
            None => return,
        };

        self.handle_audio_video_data_received(
            stream_key,
            timestamp,
            data,
            data_type,
            server_results,
        );
    }

    fn handle_pull_metadata_received(
        &mut self,
        metadata: StreamMetadata,
        server_results: &mut Vec<ServerResult>,
    ) {
        let (app_name, stream_key) = match self.pull_client {
            Some(ref client) => (client.pull_target_stream.clone(), client.pull_app.clone()),
            None => return,
        };

        self.handle_metadata_received(app_name, stream_key, metadata, server_results);
    }

    fn handle_push_session_results(
        &mut self,
        session_results: Vec<ClientSessionResult>,
        server_results: &mut Vec<ServerResult>,
    ) {
        let mut new_results = Vec::new();
        let mut events = Vec::new();
        if let Some(ref mut client) = self.push_client {
            for result in session_results {
                match result {
                    ClientSessionResult::OutboundResponse(packet) => {
                        server_results.push(ServerResult::OutboundPacket {
                            target_connection_id: client.connection_id.unwrap(),
                            packet,
                        });
                    }

                    ClientSessionResult::RaisedEvent(event) => {
                        events.push(event);
                    }

                    x => eprintln!(
                        "{}",
                        format!("Push client result received: {:?}", x).green()
                    ),
                }
            }

            if client.state == PushState::Handshaking {
                // Since we got here we know handshaking was successful, so we need
                // to initiate the connection process
                client.state = PushState::Connecting;

                let result = match client
                    .session
                    .as_mut()
                    .unwrap()
                    .request_connection(client.push_app.clone())
                {
                    Ok(result) => result,
                    Err(error) => {
                        eprintln!(
                            "{}",
                            format!(
                                "❌ Failed to request connection for push client: {:?}",
                                error
                            )
                            .red()
                        );
                        return;
                    }
                };

                new_results.push(result);
            }
        }

        if !new_results.is_empty() {
            self.handle_push_session_results(new_results, server_results);
        }

        for event in events {
            match event {
                ClientSessionEvent::ConnectionRequestAccepted => {
                    self.handle_push_connection_accepted_event(server_results);
                }

                ClientSessionEvent::PublishRequestAccepted => {
                    self.handle_push_publish_accepted_event(server_results);
                }

                x => eprintln!("{}", format!("Push event raised: {:?}", x).green()),
            }
        }
    }

    fn handle_push_connection_accepted_event(&mut self, server_results: &mut Vec<ServerResult>) {
        let mut new_results = Vec::new();
        if let Some(ref mut client) = self.push_client {
            eprintln!(
                "{}",
                format!("push accepted for app '{}'", client.push_app).green()
            );
            client.state = PushState::Connected;

            let result = client
                .session
                .as_mut()
                .unwrap()
                .request_publishing(client.push_target_stream.clone(), PublishRequestType::Live)
                .unwrap();

            let mut results = vec![result];
            new_results.append(&mut results);
        }

        if !new_results.is_empty() {
            self.handle_push_session_results(new_results, server_results);
        }
    }

    fn handle_push_publish_accepted_event(&mut self, server_results: &mut Vec<ServerResult>) {
        let mut new_results = Vec::new();
        if let Some(ref mut client) = self.push_client {
            println!(
                "{}{}",
                "✔️ Publish accepted for push • stream_key=".color(FG1),
                client.push_target_stream.color(FG2),
            );
            client.state = PushState::Pushing;

            // Send out any metadata or header information if we have any
            if let Some(channel) = self.channels.get(&client.push_source_stream) {
                if let Some(ref metadata) = channel.metadata {
                    let result = client
                        .session
                        .as_mut()
                        .unwrap()
                        .publish_metadata(metadata)
                        .unwrap();
                    new_results.push(result);
                }

                if let Some(ref bytes) = channel.video_sequence_header {
                    let result = client
                        .session
                        .as_mut()
                        .unwrap()
                        .publish_video_data(bytes.clone(), RtmpTimestamp::new(0), false)
                        .unwrap();

                    new_results.push(result);
                }

                if let Some(ref bytes) = channel.audio_sequence_header {
                    let result = client
                        .session
                        .as_mut()
                        .unwrap()
                        .publish_audio_data(bytes.clone(), RtmpTimestamp::new(0), false)
                        .unwrap();

                    new_results.push(result);
                }
            }
        }

        if !new_results.is_empty() {
            self.handle_push_session_results(new_results, server_results);
        }
    }
}

fn is_video_sequence_header(data: Bytes) -> bool {
    // This is assuming h264.
    data.len() >= 2 && data[0] == 0x17 && data[1] == 0x00
}

fn is_audio_sequence_header(data: Bytes) -> bool {
    // This is assuming aac
    data.len() >= 2 && data[0] == 0xaf && data[1] == 0x00
}

fn is_video_keyframe(data: Bytes) -> bool {
    // assumings h264
    data.len() >= 2 && data[0] == 0x17 && data[1] != 0x00 // 0x00 is the sequence header, don't count that for now
}
