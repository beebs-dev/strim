use kube::CustomResource;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use std::{fmt, str::FromStr};

#[derive(Deserialize, Serialize, Clone, Debug, Default, PartialEq, JsonSchema)]
pub struct StrimSource {
    pub internal_url: String,
}

#[derive(Deserialize, Serialize, Clone, Debug, Default, PartialEq, JsonSchema)]
pub struct StrimTarget {
    pub bucket: String,

    pub endpoint: String,

    pub region: String,

    pub secret: String,

    #[serde(rename = "keyPrefix")]
    pub key_prefix: String,

    #[serde(rename = "deleteOldSegmentsAfter")]
    pub delete_old_segments_after: Option<String>,
}

#[derive(CustomResource, Serialize, Deserialize, Default, Debug, PartialEq, Clone, JsonSchema)]
#[kube(
    group = "strim.beebs.dev",
    version = "v1",
    kind = "Strim",
    plural = "strims",
    derive = "PartialEq",
    status = "StrimStatus",
    namespaced
)]
#[kube(derive = "Default")]
#[kube(
    printcolumn = "{\"jsonPath\": \".status.phase\", \"name\": \"PHASE\", \"type\": \"string\" }"
)]
#[kube(
    printcolumn = "{\"jsonPath\": \".status.lastUpdated\", \"name\": \"AGE\", \"type\": \"date\" }"
)]
pub struct StrimSpec {
    pub source: StrimSource,

    pub target: StrimTarget,

    #[serde(default)]
    pub transcribe: bool,
}

/// Status object for the [`Strim`] resource.
#[derive(Deserialize, Serialize, Clone, Debug, PartialEq, Default, JsonSchema)]
pub struct StrimStatus {
    /// A short description of the [`Strim`] resource's current state.
    pub phase: StrimPhase,

    /// A human-readable message indicating details about why the
    /// [`Strim`] is in this phase.
    pub message: Option<String>,

    /// Timestamp of when the [`StrimStatus`] object was last updated.
    #[serde(rename = "lastUpdated")]
    pub last_updated: Option<String>,
}

/// A short description of the [`Strim`] resource's current state.
#[derive(Deserialize, Serialize, Clone, Copy, Debug, PartialEq, JsonSchema, Default)]
pub enum StrimPhase {
    /// The [`Strim`] resource first appeared to the controller.
    #[default]
    Pending,

    Starting,

    Error,

    /// The [`Strim`] is consuming the VPN credentials on a reserved slot.
    Active,

    /// Deletion of the [`Strim`] is pending garbage collection.
    Terminating,
}

impl FromStr for StrimPhase {
    type Err = ();

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "Pending" => Ok(StrimPhase::Pending),
            "Starting" => Ok(StrimPhase::Starting),
            "Active" => Ok(StrimPhase::Active),
            "Terminating" => Ok(StrimPhase::Terminating),
            "Error" => Ok(StrimPhase::Error),
            _ => Err(()),
        }
    }
}

impl fmt::Display for StrimPhase {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            StrimPhase::Pending => write!(f, "Pending"),
            StrimPhase::Starting => write!(f, "Starting"),
            StrimPhase::Active => write!(f, "Active"),
            StrimPhase::Terminating => write!(f, "Terminating"),
            StrimPhase::Error => write!(f, "Error"),
        }
    }
}
