use std::collections::BTreeMap;
use std::fmt;
use std::fs;
use std::path::Path;

use anyhow::{Context, Result};
use chrono::{DateTime, Utc};
use serde::de::{self, Deserializer, Visitor};
use serde::{Deserialize, Serialize, Serializer};

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct Manifest {
    pub schema_version: u32,
    pub networks: BTreeMap<NetworkId, ManifestEntry>,
}

impl Manifest {
    pub const CURRENT_SCHEMA_VERSION: u32 = 1;

    pub fn new() -> Self {
        Self {
            schema_version: Self::CURRENT_SCHEMA_VERSION,
            networks: BTreeMap::new(),
        }
    }
}

impl Default for Manifest {
    fn default() -> Self {
        Self::new()
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct ManifestEntry {
    pub dump_url: String,
    pub dump_timestamp: String,
    #[serde(default = "ManifestEntry::default_seed_generation")]
    pub seed_generation: u32,
}

impl ManifestEntry {
    pub const DEFAULT_SEED_GENERATION: u32 = 1;

    pub fn default_seed_generation() -> u32 {
        Self::DEFAULT_SEED_GENERATION
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct NetworkId(pub u64);

impl From<u64> for NetworkId {
    fn from(value: u64) -> Self {
        NetworkId(value)
    }
}

impl From<NetworkId> for u64 {
    fn from(value: NetworkId) -> Self {
        value.0
    }
}

impl Serialize for NetworkId {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_u64(self.0)
    }
}

impl<'de> Deserialize<'de> for NetworkId {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        struct NetworkIdVisitor;

        impl<'de> Visitor<'de> for NetworkIdVisitor {
            type Value = NetworkId;

            fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
                formatter.write_str("a positive integer network identifier")
            }

            fn visit_u64<E>(self, value: u64) -> Result<Self::Value, E>
            where
                E: de::Error,
            {
                Ok(NetworkId(value))
            }

            fn visit_i64<E>(self, value: i64) -> Result<Self::Value, E>
            where
                E: de::Error,
            {
                if value < 0 {
                    return Err(E::custom("network id must be non-negative"));
                }
                Ok(NetworkId(value as u64))
            }

            fn visit_str<E>(self, value: &str) -> Result<Self::Value, E>
            where
                E: de::Error,
            {
                value
                    .parse::<u64>()
                    .map(NetworkId)
                    .map_err(|_| E::custom("network id must be a u64"))
            }
        }

        deserializer.deserialize_any(NetworkIdVisitor)
    }
}

pub fn update_manifest(
    manifest_path: &Path,
    network_id: u64,
    dump_url: &str,
    timestamp: DateTime<Utc>,
) -> Result<()> {
    let mut manifest = load_manifest(manifest_path)?;
    if manifest.schema_version != Manifest::CURRENT_SCHEMA_VERSION {
        anyhow::bail!(
            "unsupported manifest schema version {}; expected {}",
            manifest.schema_version,
            Manifest::CURRENT_SCHEMA_VERSION
        );
    }

    let network_id = NetworkId::from(network_id);
    let seed_generation = manifest
        .networks
        .get(&network_id)
        .map(|entry| entry.seed_generation)
        .unwrap_or(ManifestEntry::DEFAULT_SEED_GENERATION);

    let entry = ManifestEntry {
        dump_url: dump_url.to_string(),
        dump_timestamp: timestamp.to_rfc3339(),
        seed_generation,
    };
    manifest.networks.insert(network_id, entry);

    let mut serialized =
        serde_yaml::to_string(&manifest).context("failed to serialize manifest to YAML")?;
    if let Some(stripped) = serialized.strip_prefix("---\n") {
        serialized = stripped.to_string();
    } else if let Some(stripped) = serialized.strip_prefix("---\r\n") {
        serialized = stripped.to_string();
    }
    fs::write(manifest_path, serialized)
        .with_context(|| format!("failed to write manifest to {}", manifest_path.display()))?;
    Ok(())
}

fn load_manifest(manifest_path: &Path) -> Result<Manifest> {
    if !manifest_path.exists() {
        return Ok(Manifest::new());
    }

    let contents = fs::read_to_string(manifest_path)
        .with_context(|| format!("failed to read manifest from {}", manifest_path.display()))?;

    let manifest: Manifest = serde_yaml::from_str(&contents)
        .with_context(|| format!("failed to parse manifest {}", manifest_path.display()))?;
    Ok(manifest)
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    #[test]
    fn update_manifest_creates_file_when_missing() {
        let dir = tempdir().unwrap();
        let manifest_path = dir.path().join("manifest.yaml");

        update_manifest(
            &manifest_path,
            42161,
            "https://example.com/42161.sql.gz",
            Utc::now(),
        )
        .unwrap();

        assert!(manifest_path.exists());
        let parsed: Manifest =
            serde_yaml::from_str(&fs::read_to_string(&manifest_path).unwrap()).unwrap();
        assert_eq!(parsed.schema_version, Manifest::CURRENT_SCHEMA_VERSION);
        let entry = parsed.networks.get(&NetworkId::from(42161)).unwrap();
        assert_eq!(entry.seed_generation, ManifestEntry::DEFAULT_SEED_GENERATION);
    }

    #[test]
    fn update_manifest_preserves_other_networks() {
        let dir = tempdir().unwrap();
        let manifest_path = dir.path().join("manifest.yaml");

        let mut manifest = Manifest::new();

        manifest.networks.insert(
            NetworkId::from(1u64),
            ManifestEntry {
                dump_url: "https://example.com/old.sql.gz".to_string(),
                dump_timestamp: "2024-01-01T00:00:00Z".to_string(),
                seed_generation: 3,
            },
        );
        fs::write(&manifest_path, serde_yaml::to_string(&manifest).unwrap()).unwrap();

        update_manifest(
            &manifest_path,
            42161,
            "https://example.com/new.sql.gz",
            Utc::now(),
        )
        .unwrap();

        let parsed: Manifest =
            serde_yaml::from_str(&fs::read_to_string(&manifest_path).unwrap()).unwrap();
        let old_entry = parsed.networks.get(&NetworkId::from(1u64)).unwrap();
        assert_eq!(old_entry.seed_generation, 3);
        let new_entry = parsed.networks.get(&NetworkId::from(42161u64)).unwrap();
        assert_eq!(
            new_entry.seed_generation,
            ManifestEntry::DEFAULT_SEED_GENERATION
        );
    }

    #[test]
    fn update_manifest_errors_on_schema_mismatch() {
        let dir = tempdir().unwrap();
        let manifest_path = dir.path().join("manifest.yaml");

        let mut manifest = Manifest::new();
        manifest.schema_version = 999;
        fs::write(&manifest_path, serde_yaml::to_string(&manifest).unwrap()).unwrap();

        let err = update_manifest(
            &manifest_path,
            1,
            "https://example.com/1.sql.gz",
            Utc::now(),
        )
        .unwrap_err();

        assert!(err
            .to_string()
            .contains("unsupported manifest schema version"));
    }
}
