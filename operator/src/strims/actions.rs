use crate::util::{self, Error, patch::*};
use k8s_openapi::api::core::v1::{
    Container, EnvVar, EnvVarSource, ObjectFieldSelector, Pod, PodSpec, SecretKeySelector, Volume,
    VolumeMount,
};
use kube::{
    Api, Client,
    api::{ObjectMeta, Resource},
    runtime::reflector::Lookup,
};
use strim_common::annotations;
use strim_types::*;

fn instance_name(instance: &Strim) -> Result<&str, Error> {
    instance
        .meta()
        .name
        .as_deref()
        .ok_or_else(|| Error::UserInput("Strim is missing metadata.name".to_string()))
}

fn instance_namespace(instance: &Strim) -> Result<&str, Error> {
    instance
        .meta()
        .namespace
        .as_deref()
        .ok_or_else(|| Error::UserInput("Strim is missing metadata.namespace".to_string()))
}

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

pub async fn delete_pod(client: Client, instance: &Strim, reason: String) -> Result<(), Error> {
    let pod_name = instance_name(instance)?;
    println!(
        "Deleting Pod '{}' for Strim '{}' • reason: {}",
        pod_name, pod_name, reason
    );
    patch_status(client.clone(), instance, |status| {
        status.phase = StrimPhase::Pending;
        status.message = Some(delete_message(&reason));
    })
    .await?;
    let pods: Api<Pod> = Api::namespaced(client.clone(), instance_namespace(instance)?);
    match pods.delete(pod_name, &Default::default()).await {
        Ok(_) => {}
        Err(kube::Error::Api(ae)) if ae.code == 404 => {}
        Err(e) => return Err(Error::from(e)),
    }
    Ok(())
}

fn delete_message(reason: &str) -> String {
    format!("The peggy Pod is being deleted. Reason: {}", reason)
}

pub async fn starting(client: Client, instance: &Strim, reason: String) -> Result<(), Error> {
    patch_status(client, instance, |status| {
        status.phase = StrimPhase::Starting;
        status.message = Some(reason);
    })
    .await?;
    Ok(())
}

pub async fn pending(client: Client, instance: &Strim, reason: String) -> Result<(), Error> {
    patch_status(client, instance, |status| {
        status.phase = StrimPhase::Pending;
        status.message = Some(reason);
    })
    .await?;
    Ok(())
}

pub async fn terminating(client: Client, instance: &Strim, reason: String) -> Result<(), Error> {
    patch_status(client, instance, |status| {
        status.phase = StrimPhase::Terminating;
        status.message = Some(reason);
    })
    .await?;
    Ok(())
}

pub fn pod_resource(instance: &Strim) -> Result<Pod, Error> {
    // For simplicity, we create a pod spec with a single container
    // that runs ffmpeg to stream from the source to the destination
    const HLS_DIR: &str = "/hls";
    let image = String::from("thavlik/strim-peggy:latest");
    let name = instance_name(instance)?.to_string();
    let namespace = instance_namespace(instance)?.to_string();
    Ok(Pod {
        metadata: ObjectMeta {
            name: Some(name),
            namespace: Some(namespace),
            owner_references: Some(vec![instance.controller_owner_ref(&()).unwrap()]),
            annotations: Some({
                let mut annotations = std::collections::BTreeMap::new();
                annotations.insert(
                    annotations::SPEC_HASH.to_string(),
                    util::hash_spec(&instance.spec),
                );
                annotations.insert(
                    annotations::CREATED_BY.to_string(),
                    "strim-operator".to_string(),
                );
                annotations
            }),
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
                    env: Some({
                        let mut env = vec![
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
                        ];
                        if let Some(delete_after) =
                            instance.spec.target.delete_old_segments_after.as_deref()
                        {
                            env.push(EnvVar {
                                name: "DELETE_OLD_SEGMENTS_AFTER".to_string(),
                                value: Some(delete_after.to_string()),
                                ..Default::default()
                            });
                        }
                        env
                    }),
                    ..Default::default()
                },
            ],
            restart_policy: Some("Never".to_string()),
            // resources: Some(ResourceRequirements {
            //     requests: Some({
            //         let mut m = std::collections::BTreeMap::new();
            //         m.insert("cpu".to_string(), Quantity("500m".to_string()));
            //         m.insert("memory".to_string(), Quantity("64Mi".to_string()));
            //         m
            //     }),
            //     limits: Some({
            //         let mut m = std::collections::BTreeMap::new();
            //         //m.insert("cpu".to_string(), Quantity("2000m".to_string()));
            //         m.insert("memory".to_string(), Quantity("512Mi".to_string()));
            //         m
            //     }),
            //     ..Default::default()
            // }),
            ..Default::default()
        }),
        status: None,
    })
}

pub async fn create_pod(client: Client, instance: &Strim) -> Result<(), Error> {
    let pod = pod_resource(instance)?;
    let pod_name = instance_name(instance)?;
    patch_status(client.clone(), instance, |status| {
        status.phase = StrimPhase::Starting;
        status.message = Some(format!("Creating peggy Pod '{}'", pod_name));
    })
    .await?;
    let pods: Api<Pod> = Api::namespaced(client.clone(), instance_namespace(instance)?);
    match pods.create(&Default::default(), &pod).await {
        Ok(_) => Ok(()),
        Err(e) => match e {
            kube::Error::Api(ae) if ae.code == 409 => Ok(()),
            _ => return Err(Error::from(e)),
        },
    }
}

pub async fn error(client: Client, instance: &Strim, message: String) -> Result<(), Error> {
    patch_status(client.clone(), instance, |status| {
        status.phase = StrimPhase::Error;
        status.message = Some(message);
    })
    .await?;
    Ok(())
}
