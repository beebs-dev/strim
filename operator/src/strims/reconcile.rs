use chrono::Utc;
use futures::stream::StreamExt;
use k8s_openapi::api::core::v1::Pod;
use kube::{
    Api, Resource, ResourceExt,
    client::Client,
    runtime::{Controller, controller::Action},
};
use kube_leader_election::{LeaseLock, LeaseLockParams};
use owo_colors::OwoColorize;
use std::sync::Arc;
use strim_types::*;
use tokio::time::Duration;
use tokio_util::sync::CancellationToken;

use super::actions;
use crate::{
    strims::actions::HASH_ANNOTATION_NAME,
    util::{
        self, Error, PROBE_INTERVAL,
        colors::{FG1, FG2},
    },
};

#[cfg(feature = "metrics")]
use crate::util::metrics::ControllerMetrics;

/// Entrypoint for the `Strim` controller.
pub async fn run(client: Client) -> Result<(), Error> {
    println!("{}", "Starting Strim controller...".green());

    // Preparation of resources used by the `kube_runtime::Controller`
    let context: Arc<ContextData> = Arc::new(ContextData::new(client.clone()));

    // The controller comes from the `kube_runtime` crate and manages the reconciliation process.
    // It requires the following information:
    // - `kube::Api<T>` this controller "owns". In this case, `T = Strim`, as this controller owns the `Strim` resource,
    // - `kube::api::ListParams` to select the `Strim` resources with. Can be used for Strim filtering `Strim` resources before reconciliation,
    // - `reconcile` function with reconciliation logic to be called each time a resource of `Strim` kind is created/updated/deleted,
    // - `on_error` function to call whenever reconciliation fails.
    //Controller::new(crd_api, Default::default())
    //    .owns(Api::<Pod>::all(client), Default::default())
    //    .run(reconcile, on_error, context)
    //    .for_each(|_reconciliation_result| async move {})
    //    .await;
    //Ok(())

    // Namespace where the Lease object lives.
    // Commonly: the controller's namespace. If you deploy in one namespace, hardcode it.
    // If you want it dynamic, inject NAMESPACE via the Downward API.
    let lease_namespace = std::env::var("NAMESPACE").unwrap_or_else(|_| "default".to_string());
    // Unique identity per replica (Downward API POD_NAME is ideal).
    // Fallback to hostname if not present.
    let holder_id = std::env::var("POD_NAME")
        .or_else(|_| std::env::var("HOSTNAME"))
        .unwrap_or_else(|_| format!("strim-controller-{}", uuid::Uuid::new_v4()));
    // The shared lock name across all replicas
    let lease_name = "strim-controller-lock".to_string();
    // TTL: how long leadership is considered valid without renewal.
    // Renew should happen well before TTL expires.
    let lease_ttl = Duration::from_secs(15);
    let renew_every = Duration::from_secs(5);
    let leadership = LeaseLock::new(
        client.clone(),
        &lease_namespace,
        LeaseLockParams {
            holder_id,
            lease_name,
            lease_ttl,
        },
    );

    let shutdown = CancellationToken::new();
    let shutdown_signal = shutdown.clone();
    tokio::spawn(async move {
        strim_common::shutdown::shutdown_signal().await;
        shutdown_signal.cancel();
    });
    strim_common::signal_ready();
    println!("{}", "🌱 Starting Strim controller...".green());
    // We run indefinitely; only the leader runs the controller.
    // On leadership loss, we abort the controller and go back to standby.
    let mut controller_task: Option<tokio::task::JoinHandle<()>> = None;
    let mut tick = tokio::time::interval(renew_every);
    loop {
        tokio::select! {
            _ = shutdown.cancelled() => {
                if let Some(task) = controller_task.take() {
                    task.abort();
                    task.await.ok();
                }
                break Ok(())
            },
            _ = tick.tick() => {}
        }
        let lease = match leadership.try_acquire_or_renew().await {
            Ok(l) => l,
            Err(e) => {
                // If we can't talk to the apiserver / update Lease, assume we are not safe to lead.
                eprintln!("leader election renew/acquire failed: {e}");
                if let Some(task) = controller_task.take() {
                    task.abort();
                    eprintln!("aborted controller due to leader election error");
                }
                continue;
            }
        };
        if lease.acquired_lease {
            // We are leader; ensure controller is running
            if controller_task.is_none() {
                println!("acquired leadership; starting controller");
                let client_for_controller = client.clone();
                let context_for_controller = context.clone();
                let crd_api_for_controller: Api<Strim> = Api::all(client_for_controller.clone());
                controller_task = Some(tokio::spawn(async move {
                    println!("{}", "🚀 Strim controller started.".green());
                    Controller::new(crd_api_for_controller, Default::default())
                        .owns(Api::<Pod>::all(client_for_controller), Default::default())
                        .run(reconcile, on_error, context_for_controller)
                        .for_each(|_res| async move {})
                        .await;
                }));
            }
        } else if let Some(task) = controller_task.take() {
            // We are NOT leader; ensure controller is stopped
            eprintln!("lost leadership; stopping controller");
            task.abort();
        }
    }
}

/// Context injected with each `reconcile` and `on_error` method invocation.
struct ContextData {
    /// Kubernetes client to make Kubernetes API requests with. Required for K8S resource management.
    client: Client,

    #[cfg(feature = "metrics")]
    metrics: ControllerMetrics,
}

impl ContextData {
    /// Constructs a new instance of ContextData.
    ///
    /// # Arguments:
    /// - `client`: A Kubernetes client to make Kubernetes REST API requests with. Resources
    ///   will be created and deleted with this client.
    pub fn new(client: Client) -> Self {
        #[cfg(feature = "metrics")]
        {
            ContextData {
                client,
                metrics: ControllerMetrics::new("consumers"),
            }
        }
        #[cfg(not(feature = "metrics"))]
        {
            ContextData { client }
        }
    }
}

/// Action to be taken upon an `Strim` resource during reconciliation
#[derive(Debug, PartialEq)]
enum StrimAction {
    /// Create all subresources required by the [`Strim`].
    CreatePod,

    DeletePod,

    Starting {
        pod_name: String,
    },

    /// Signals that the [`Strim`] is fully reconciled.
    Active {
        pod_name: String,
    },

    /// An error occurred during reconciliation.
    Error(String),

    /// The [`Strim`] resource is in desired state and requires no actions to be taken.
    NoOp,

    Requeue(Duration),
}

impl StrimAction {
    fn to_str(&self) -> &str {
        match self {
            StrimAction::CreatePod => "CreatePod",
            StrimAction::DeletePod => "DeletePod",
            StrimAction::Starting { .. } => "Starting",
            StrimAction::Active { .. } => "Active",
            StrimAction::NoOp => "NoOp",
            StrimAction::Error(_) => "Error",
            StrimAction::Requeue(_) => "Requeue",
        }
    }
}

/// Reconciliation function for the `Strim` resource.
async fn reconcile(instance: Arc<Strim>, context: Arc<ContextData>) -> Result<Action, Error> {
    // The `Client` is shared -> a clone from the reference is obtained
    let client: Client = context.client.clone();

    // The resource of `Strim` kind is required to have a namespace set. However, it is not guaranteed
    // the resource will have a `namespace` set. Therefore, the `namespace` field on object's metadata
    // is optional and Rust forces the programmer to check for it's existence first.
    let namespace: String = match instance.namespace() {
        None => {
            // If there is no namespace to deploy to defined, reconciliation ends with an error immediately.
            return Err(Error::UserInput(
                "Expected Strim resource to be namespaced. Can't deploy to an unknown namespace."
                    .to_owned(),
            ));
        }
        // If namespace is known, proceed. In a more advanced version of the operator, perhaps
        // the namespace could be checked for existence first.
        Some(namespace) => namespace,
    };

    // Name of the Strim resource is used to name the subresources as well.
    let name = instance.name_any();

    // Increment total number of reconciles for the Strim resource.
    #[cfg(feature = "metrics")]
    context
        .metrics
        .reconcile_counter
        .with_label_values(&[&name, &namespace])
        .inc();

    // Benchmark the read phase of reconciliation.
    #[cfg(feature = "metrics")]
    let start = std::time::Instant::now();

    // Read phase of reconciliation determines goal during the write phase.
    let action = determine_action(client.clone(), &name, &namespace, &instance).await?;

    if action != StrimAction::NoOp {
        println!(
            "🔧 {}{}{}{}{}",
            namespace.color(FG2),
            "/".color(FG1),
            name.color(FG2),
            " ACTION: ".color(FG1),
            format!("{:?}", action).color(FG2),
        );
    }

    // Report the read phase performance.
    #[cfg(feature = "metrics")]
    context
        .metrics
        .read_histogram
        .with_label_values(&[&name, &namespace, action.to_str()])
        .observe(start.elapsed().as_secs_f64());

    // Increment the counter for the action.
    #[cfg(feature = "metrics")]
    context
        .metrics
        .action_counter
        .with_label_values(&[&name, &namespace, action.to_str()])
        .inc();

    // Benchmark the write phase of reconciliation.
    #[cfg(feature = "metrics")]
    let timer = match action {
        // Don't measure performance for NoOp actions.
        StrimAction::NoOp => None,
        // Start a performance timer for the write phase.
        _ => Some(
            context
                .metrics
                .write_histogram
                .with_label_values(&[&name, &namespace, action.to_str()])
                .start_timer(),
        ),
    };

    // Performs action as decided by the `determine_action` function.
    // This is the write phase of reconciliation.
    let result = match action {
        StrimAction::Requeue(duration) => Action::requeue(duration),
        StrimAction::Starting { pod_name } => {
            // Update the phase to Starting.
            actions::starting(client, &instance, &pod_name).await?;

            Action::await_change()
        }
        StrimAction::DeletePod => {
            actions::delete_pod(client.clone(), &instance).await?;

            Action::await_change()
        }
        // StrimAction::Delete => {
        //     // Show that the reservation is being terminated.
        //     actions::terminating(client.clone(), &instance).await?;

        //     // Remove the finalizer from the Strim resource.
        //     //finalizer::delete::<Strim>(client.clone(), &name, &namespace).await?;

        //     // Child resources will be deleted by kubernetes.
        //     Action::await_change()
        // }
        StrimAction::CreatePod => {
            // Add a finalizer so the resource can be properly garbage collected.
            //let instance = finalizer::add(client.clone(), &name, &namespace).await?;
            // Note: finalizer is not required since we do not have custom logic on deletion of child resources.
            actions::create_pod(client.clone(), &instance).await?;

            Action::await_change()
        }
        StrimAction::Error(message) => {
            actions::error(client.clone(), &instance, message).await?;

            Action::await_change()
        }
        StrimAction::Active { pod_name } => {
            // Update the phase to Active, meaning the reservation is in use.
            actions::active(client, &instance, &pod_name).await?;

            // Resource is fully reconciled.
            Action::requeue(PROBE_INTERVAL)
        }
        // The resource is already in desired state, do nothing and re-check after 10 seconds
        StrimAction::NoOp => Action::requeue(PROBE_INTERVAL),
    };

    #[cfg(feature = "metrics")]
    if let Some(timer) = timer {
        timer.observe_duration();
    }

    Ok(result)
}

/// Resources arrives into reconciliation queue in a certain state. This function looks at
/// the state of given `Strim` resource and decides which actions needs to be performed.
/// The finite set of possible actions is represented by the `StrimAction` enum.
///
/// # Arguments
/// - `instance`: A reference to `Strim` being reconciled to decide next action upon.
async fn determine_action(
    client: Client,
    _name: &str,
    namespace: &str,
    instance: &Strim,
) -> Result<StrimAction, Error> {
    // Don't do anything while being deleted.
    if instance.metadata.deletion_timestamp.is_some() {
        return Ok(StrimAction::Requeue(Duration::from_millis(500)));
    }

    // Does the ffmpeg pod exist?
    let pod = match get_pod(
        client.clone(),
        namespace,
        instance.meta().name.as_ref().unwrap(),
    )
    .await?
    {
        Some(pod) => pod,
        None => return Ok(StrimAction::CreatePod),
    };

    // Don't do anything while the pod is being deleted.
    if pod.metadata.deletion_timestamp.is_some() {
        return Ok(StrimAction::Requeue(Duration::from_millis(500)));
    }

    // Check the hash
    let desired_hash = util::hash_spec(&instance.spec);
    if !pod
        .metadata
        .annotations
        .as_ref()
        .is_some_and(|a| a.get(HASH_ANNOTATION_NAME) == Some(&desired_hash))
    {
        println!("Spec hash mismatch, recreating Pod");
        return Ok(StrimAction::DeletePod);
    }

    match pod.status.as_ref().and_then(|s| s.phase.as_deref()) {
        Some("Pending") | Some("ContainerCreating") => {
            if instance
                .status
                .as_ref()
                .is_some_and(|s| s.phase == StrimPhase::Starting)
            {
                return Ok(StrimAction::NoOp);
            }
            return Ok(StrimAction::Starting {
                pod_name: pod.meta().name.clone().unwrap(),
            });
        }
        Some("Running") => {}
        Some("Succeeded") | Some("Failed") => {
            return Ok(StrimAction::DeletePod);
        }
        _ => {
            return Ok(StrimAction::Error("Pod is in unknown state.".to_owned()));
        }
    }

    if let Some(ref status) = pod.status
        && let Some(ref container_statuses) = status.container_statuses
    {
        for container_status in container_statuses {
            if let Some(state) = &container_status.state
                && let Some(ref terminated) = state.terminated
            {
                println!(
                    "Pod's container terminated with exit code {} and reason: {}",
                    terminated.exit_code,
                    terminated
                        .reason
                        .as_ref()
                        .unwrap_or(&"No reason provided".to_string())
                );
                // Recreate the pod
                return Ok(StrimAction::DeletePod);
            }
        }
    }

    // Keep the Active status up-to-date.
    determine_status_action(instance)
}

async fn get_pod(client: Client, namespace: &str, name: &str) -> Result<Option<Pod>, Error> {
    let api: Api<Pod> = Api::namespaced(client, namespace);
    match api.get(name).await {
        Ok(pod) => Ok(Some(pod)),
        Err(kube::Error::Api(ae)) if ae.code == 404 => Ok(None),
        Err(e) => Err(e.into()),
    }
}

/// Determines the action given that the only thing left to do
/// is periodically keeping the Active phase up-to-date.
fn determine_status_action(instance: &Strim) -> Result<StrimAction, Error> {
    let (phase, age) = get_strim_phase(instance)?;
    if phase != StrimPhase::Active || age > PROBE_INTERVAL {
        Ok(StrimAction::Active {
            pod_name: instance.meta().name.clone().unwrap(),
        })
    } else {
        Ok(StrimAction::NoOp)
    }
}

/// Returns the phase of the Strim.
pub fn get_strim_phase(instance: &Strim) -> Result<(StrimPhase, Duration), Error> {
    let status = instance
        .status
        .as_ref()
        .ok_or_else(|| Error::UserInput("No status".to_string()))?;
    let phase = status.phase;
    let last_updated: chrono::DateTime<Utc> = status
        .last_updated
        .as_ref()
        .ok_or_else(|| Error::UserInput("No lastUpdated".to_string()))?
        .parse()?;
    let age: chrono::Duration = Utc::now() - last_updated;
    Ok((phase, age.to_std()?))
}

/// Actions to be taken when a reconciliation fails - for whatever reason.
/// Prints out the error to `stderr` and requeues the resource for another reconciliation after
/// five seconds.
///
/// # Arguments
/// - `instance`: The erroneous resource.
/// - `error`: A reference to the `kube::Error` that occurred during reconciliation.
/// - `_context`: Unused argument. Context Data "injected" automatically by kube-rs.
fn on_error(instance: Arc<Strim>, error: &Error, _context: Arc<ContextData>) -> Action {
    eprintln!(
        "{}",
        format!("Reconciliation error: {:?} {:?}", error, instance).red()
    );
    Action::requeue(Duration::from_secs(5))
}
