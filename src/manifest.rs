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
    pub const CURRENT_SCHEMA_VERSION: u32 = 2;

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

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SchemaVersionBump {
    pub previous: u32,
    pub next: u32,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SeedGenerationBump {
    pub network_id: NetworkId,
    pub previous: u32,
    pub next: u32,
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

    write_manifest(manifest_path, &manifest)
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

fn write_manifest(manifest_path: &Path, manifest: &Manifest) -> Result<()> {
    let mut serialized =
        serde_yaml::to_string(manifest).context("failed to serialize manifest to YAML")?;
    if let Some(stripped) = serialized.strip_prefix("---\n") {
        serialized = stripped.to_string();
    } else if let Some(stripped) = serialized.strip_prefix("---\r\n") {
        serialized = stripped.to_string();
    }
    fs::write(manifest_path, serialized)
        .with_context(|| format!("failed to write manifest to {}", manifest_path.display()))?;
    Ok(())
}

pub fn bump_schema_version(manifest_path: &Path, source_path: &Path) -> Result<SchemaVersionBump> {
    let mut manifest = load_manifest(manifest_path)?;
    let previous = manifest.schema_version;
    let next = previous + 1;
    manifest.schema_version = next;
    write_manifest(manifest_path, &manifest)?;

    update_schema_version_constant(source_path, previous, next)?;
    Ok(SchemaVersionBump { previous, next })
}

pub fn bump_seed_generation(
    manifest_path: &Path,
    network_id: NetworkId,
) -> Result<SeedGenerationBump> {
    let mut manifest = load_manifest(manifest_path)?;
    let entry = manifest
        .networks
        .get_mut(&network_id)
        .with_context(|| format!("network id {} not found in manifest", u64::from(network_id)))?;

    let previous = entry.seed_generation;
    let next = previous + 1;
    entry.seed_generation = next;

    write_manifest(manifest_path, &manifest)?;
    Ok(SeedGenerationBump {
        network_id,
        previous,
        next,
    })
}

fn update_schema_version_constant(
    source_path: &Path,
    expected_current: u32,
    next: u32,
) -> Result<()> {
    let contents = fs::read_to_string(source_path)
        .with_context(|| format!("failed to read manifest source {}", source_path.display()))?;
    let marker = "pub const CURRENT_SCHEMA_VERSION: u32 = ";
    let start = contents.find(marker).with_context(|| {
        format!(
            "CURRENT_SCHEMA_VERSION constant not found in {}",
            source_path.display()
        )
    })?;
    let value_start = start + marker.len();
    let remainder = &contents[value_start..];
    let semicolon_offset = remainder
        .find(';')
        .context("CURRENT_SCHEMA_VERSION constant missing ';'")?;
    let value_str = remainder[..semicolon_offset].trim();
    let current: u32 = value_str.parse().with_context(|| {
        format!(
            "failed to parse CURRENT_SCHEMA_VERSION value '{}'",
            value_str
        )
    })?;
    if current != expected_current {
        anyhow::bail!(
            "CURRENT_SCHEMA_VERSION constant ({}) does not match manifest schema_version ({})",
            current,
            expected_current
        );
    }

    let mut updated = String::with_capacity(contents.len());
    updated.push_str(&contents[..value_start]);
    updated.push_str(&next.to_string());
    updated.push_str(&remainder[semicolon_offset..]);
    fs::write(source_path, updated).with_context(|| {
        format!(
            "failed to write updated manifest source {}",
            source_path.display()
        )
    })?;
    Ok(())
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
        assert_eq!(
            entry.seed_generation,
            ManifestEntry::DEFAULT_SEED_GENERATION
        );
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

    #[test]
    fn bump_schema_version_updates_manifest_and_constant() {
        let dir = tempdir().unwrap();
        let manifest_path = dir.path().join("manifest.yaml");
        let source_path = dir.path().join("manifest.rs");

        let mut manifest = Manifest::new();
        manifest.schema_version = Manifest::CURRENT_SCHEMA_VERSION;
        write_manifest(&manifest_path, &manifest).unwrap();
        fs::write(
            &source_path,
            format!(
                "pub const CURRENT_SCHEMA_VERSION: u32 = {};\n",
                Manifest::CURRENT_SCHEMA_VERSION
            ),
        )
        .unwrap();

        let bump = bump_schema_version(&manifest_path, &source_path).unwrap();
        assert_eq!(bump.previous, Manifest::CURRENT_SCHEMA_VERSION);
        assert_eq!(bump.next, Manifest::CURRENT_SCHEMA_VERSION + 1);

        let updated_manifest = load_manifest(&manifest_path).unwrap();
        assert_eq!(updated_manifest.schema_version, bump.next);

        let updated_source = fs::read_to_string(&source_path).unwrap();
        assert!(updated_source.contains(&format!(
            "pub const CURRENT_SCHEMA_VERSION: u32 = {};",
            bump.next
        )));
    }

    #[test]
    fn bump_seed_generation_increments_existing_entry() {
        let dir = tempdir().unwrap();
        let manifest_path = dir.path().join("manifest.yaml");

        let network = NetworkId::from(10u64);
        let mut manifest = Manifest::new();
        manifest.networks.insert(
            network,
            ManifestEntry {
                dump_url: "https://example.com/10.sql.gz".to_string(),
                dump_timestamp: "2024-01-01T00:00:00Z".to_string(),
                seed_generation: 5,
            },
        );
        write_manifest(&manifest_path, &manifest).unwrap();

        let bump = bump_seed_generation(&manifest_path, network).unwrap();
        assert_eq!(bump.network_id, network);
        assert_eq!(bump.previous, 5);
        assert_eq!(bump.next, 6);

        let contents = fs::read_to_string(&manifest_path).unwrap();
        let parsed: Manifest = serde_yaml::from_str(&contents).unwrap();
        assert_eq!(
            parsed
                .networks
                .get(&network)
                .expect("entry exists")
                .seed_generation,
            6
        );
    }

    #[test]
    fn bump_seed_generation_errors_for_missing_network() {
        let dir = tempdir().unwrap();
        let manifest_path = dir.path().join("manifest.yaml");
        write_manifest(&manifest_path, &Manifest::new()).unwrap();

        let err = bump_seed_generation(&manifest_path, NetworkId::from(999u64)).unwrap_err();
        assert!(err
            .to_string()
            .contains("network id 999 not found in manifest"));
    }
}
