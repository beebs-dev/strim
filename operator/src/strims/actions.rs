use crate::util::{Error, messages, patch::*};
use k8s_openapi::api::core::v1::{
    Container, EnvVar, EnvVarSource, ObjectFieldSelector, Pod, PodSpec, SecretKeySelector, Volume,
    VolumeMount,
};
use kube::{
    Api, Client,
    api::{ObjectMeta, Resource},
};
use strim_types::*;

/// Updates the `Strim`'s phase to Active.
pub async fn active(client: Client, instance: &Strim, peggy_pod_name: &str) -> Result<(), Error> {
    patch_status(client, instance, |status| {
        status.phase = StrimPhase::Active;
        status.message = Some(format!(
            "The peggy Pod '{}' is active and running.",
            peggy_pod_name
        ));
    })
    .await?;
    Ok(())
}

/// Updates the `Strim`'s phase to Terminating.
pub async fn terminating(client: Client, instance: &Strim) -> Result<(), Error> {
    patch_status(client, instance, |status| {
        status.phase = StrimPhase::Terminating;
        status.message = Some(messages::TERMINATING.to_owned());
    })
    .await?;
    Ok(())
}

pub async fn delete_pod(client: Client, instance: &Strim) -> Result<(), Error> {
    let pods: Api<Pod> =
        Api::namespaced(client.clone(), instance.meta().namespace.as_ref().unwrap());
    pods.delete(instance.meta().name.as_ref().unwrap(), &Default::default())
        .await?;
    Ok(())
}

fn starting_message(pod_name: &str) -> String {
    format!("The peggy Pod '{}' is starting.", pod_name)
}

pub async fn starting(client: Client, instance: &Strim, pod_name: &str) -> Result<(), Error> {
    patch_status(client, instance, |status| {
        status.phase = StrimPhase::Starting;
        status.message = Some(starting_message(pod_name));
    })
    .await?;
    Ok(())
}

fn ffmpeg_pod(instance: &Strim) -> Pod {
    // For simplicity, we create a pod spec with a single container
    // that runs ffmpeg to stream from the source to the destination
    const HLS_DIR: &str = "/hls";
    let image = String::from("thavlik/strim-peggy:latest");
    Pod {
        metadata: ObjectMeta {
            name: instance.meta().name.clone(),
            namespace: instance.meta().namespace.clone(),
            owner_references: Some(vec![instance.controller_owner_ref(&()).unwrap()]),
            ..Default::default()
        },
        spec: Some(PodSpec {
            volumes: Some(vec![Volume {
                name: "hls-storage".to_string(),
                empty_dir: Some(Default::default()),
                ..Default::default()
            }]),
            containers: vec![
                Container {
                    name: "ffmpeg".to_string(),
                    image: Some(image.clone()),
                    image_pull_policy: Some("Always".to_string()),
                    command: Some(vec!["/usr/local/bin/run-ffmpeg-hls.sh".to_string()]),
                    volume_mounts: Some(vec![VolumeMount {
                        name: "hls-storage".to_string(),
                        mount_path: HLS_DIR.to_string(),
                        ..Default::default()
                    }]),
                    env: Some(vec![
                        EnvVar {
                            name: "HLS_DIR".to_string(),
                            value: Some(HLS_DIR.to_string()),
                            ..Default::default()
                        },
                        EnvVar {
                            name: "RTMP_URL".to_string(),
                            value: Some(instance.spec.source.internal_url.clone()),
                            ..Default::default()
                        },
                    ]),
                    ..Default::default()
                },
                Container {
                    name: "peggy".to_string(),
                    image: Some(image.clone()),
                    image_pull_policy: Some("Always".to_string()),
                    volume_mounts: Some(vec![VolumeMount {
                        name: "hls-storage".to_string(),
                        mount_path: HLS_DIR.to_string(),
                        ..Default::default()
                    }]),
                    env: Some(vec![
                        EnvVar {
                            name: "NODE_ID".to_string(),
                            value_from: Some(EnvVarSource {
                                field_ref: Some(ObjectFieldSelector {
                                    field_path: "metadata.name".to_string(),
                                    ..Default::default()
                                }),
                                ..Default::default()
                            }),
                            ..Default::default()
                        },
                        EnvVar {
                            name: "HLS_DIR".to_string(),
                            value: Some("/hls".to_string()),
                            ..Default::default()
                        },
                        EnvVar {
                            name: "AWS_ACCESS_KEY_ID".to_string(),
                            value_from: Some(EnvVarSource {
                                secret_key_ref: Some(SecretKeySelector {
                                    key: "access_key_id".to_string(),
                                    name: instance.spec.target.secret.clone(),
                                    ..Default::default()
                                }),
                                ..Default::default()
                            }),
                            ..Default::default()
                        },
                        EnvVar {
                            name: "AWS_SECRET_ACCESS_KEY".to_string(),
                            value_from: Some(EnvVarSource {
                                secret_key_ref: Some(SecretKeySelector {
                                    key: "secret_access_key".to_string(),
                                    name: instance.spec.target.secret.clone(),
                                    ..Default::default()
                                }),
                                ..Default::default()
                            }),
                            ..Default::default()
                        },
                        EnvVar {
                            name: "S3_BUCKET".to_string(),
                            value: Some(instance.spec.target.bucket.clone()),
                            ..Default::default()
                        },
                        EnvVar {
                            name: "S3_REGION".to_string(),
                            value: Some(instance.spec.target.region.clone()),
                            ..Default::default()
                        },
                        EnvVar {
                            name: "S3_ENDPOINT".to_string(),
                            value: Some(instance.spec.target.endpoint.clone()),
                            ..Default::default()
                        },
                        EnvVar {
                            name: "S3_KEY_PREFIX".to_string(),
                            value: Some(instance.spec.target.key_prefix.clone()),
                            ..Default::default()
                        },
                    ]),
                    ..Default::default()
                },
            ],
            restart_policy: Some("Never".to_string()),
            ..Default::default()
        }),
        status: None,
    }
}

pub async fn create_pod(client: Client, instance: &Strim) -> Result<(), Error> {
    let pod = ffmpeg_pod(instance);
    patch_status(client.clone(), instance, |status| {
        status.phase = StrimPhase::Starting;
        status.message = Some(starting_message(pod.meta().name.as_ref().unwrap()));
    })
    .await?;
    let pods: Api<Pod> =
        Api::namespaced(client.clone(), instance.meta().namespace.as_ref().unwrap());
    pods.create(&Default::default(), &pod).await?;
    Ok(())
}

pub async fn error(client: Client, instance: &Strim, message: String) -> Result<(), Error> {
    patch_status(client.clone(), instance, |status| {
        status.phase = StrimPhase::Error;
        status.message = Some(message);
    })
    .await?;
    Ok(())
}
