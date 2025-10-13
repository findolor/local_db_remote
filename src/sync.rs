use std::collections::{BTreeSet, HashMap};
use std::fs;
use std::path::{Path, PathBuf};

use anyhow::{Context, Result};
use chrono::Utc;

use crate::archive::{download_cli_archive, extract_cli_binary};
use crate::cli::{run_cli_sync, RunCliSyncOptions};
use crate::constants::{API_TOKEN_ENV_VARS, CLI_ARCHIVE_NAME, RELEASE_DOWNLOAD_URL_TEMPLATE};
use crate::database::{finalize_database, plan_sync, prepare_database};
use crate::http::{DefaultHttpClient, HttpClient};
use crate::logging::log_plan;
use crate::manifest::{update_manifest, Manifest};

#[derive(Clone, Debug)]
pub struct SyncConfig {
    pub db_dir: PathBuf,
    pub cli_dir: PathBuf,
    pub chain_ids: Vec<u64>,
}

impl Default for SyncConfig {
    fn default() -> Self {
        Self {
            db_dir: PathBuf::from("data"),
            cli_dir: PathBuf::from("bin"),
            chain_ids: vec![],
        }
    }
}

pub struct SyncRuntime {
    pub env: HashMap<String, String>,
    pub cwd: PathBuf,
    pub http: Box<dyn HttpClient>,
}

impl Default for SyncRuntime {
    fn default() -> Self {
        let env = std::env::vars().collect();
        let cwd = std::env::current_dir().expect("failed to read current directory");
        let http = Box::new(DefaultHttpClient::default()) as Box<dyn HttpClient>;

        Self { env, cwd, http }
    }
}

impl SyncRuntime {
    #[allow(dead_code)]
    pub fn with_http(mut self, http: Box<dyn HttpClient>) -> Self {
        self.http = http;
        self
    }
}

pub fn run_sync() -> Result<()> {
    run_sync_with(SyncRuntime::default(), SyncConfig::default())
}

pub fn run_sync_with(runtime: SyncRuntime, config: SyncConfig) -> Result<()> {
    let start_time = Utc::now();
    println!("Sync started at {}", start_time.to_rfc3339());

    let commit_hash = runtime
        .env
        .get("COMMIT_HASH")
        .map(|value| value.trim().to_string())
        .filter(|value| !value.is_empty())
        .ok_or_else(|| {
            anyhow::anyhow!("COMMIT_HASH must be set to a valid rain.orderbook commit hash")
        })?;
    println!("Using commit hash {commit_hash}");

    let archive_path = runtime.cwd.join(CLI_ARCHIVE_NAME);
    download_cli_archive(runtime.http.as_ref(), &commit_hash, &archive_path)?;

    let cli_dir = resolve_path(&runtime.cwd, &config.cli_dir);
    let cli_binary = extract_cli_binary(&archive_path, &cli_dir)?;

    let _ = fs::remove_file(&archive_path);

    let api_token = resolve_api_token(&runtime.env)?;
    println!("Using API token sourced from environment.");

    let db_dir = resolve_path(&runtime.cwd, &config.db_dir);
    fs::create_dir_all(&db_dir)
        .with_context(|| format!("failed to create database directory {}", db_dir.display()))?;

    let manifest_path = db_dir.join("manifest.yaml");
    let manifest = download_manifest_to_dir(runtime.http.as_ref(), &manifest_path)
        .with_context(|| format!("failed to download manifest to {}", manifest_path.display()))?;
    download_dumps_for_manifest(runtime.http.as_ref(), &manifest, &db_dir)
        .with_context(|| format!("failed to hydrate dumps into {}", db_dir.display()))?;

    let mut chain_ids: BTreeSet<u64> = manifest
        .networks
        .keys()
        .map(|network| u64::from(*network))
        .collect();
    for chain_id in &config.chain_ids {
        chain_ids.insert(*chain_id);
    }
    for chain_id in chain_ids {
        sync_single_chain(
            chain_id,
            &cli_binary,
            &api_token,
            &commit_hash,
            &db_dir,
            &manifest_path,
        )?;
    }

    let completion_time = Utc::now();
    let duration = completion_time - start_time;
    let elapsed_seconds = duration.num_milliseconds() as f64 / 1000.0;
    println!(
        "All syncs completed at {} (duration: {:.1}s)",
        completion_time.to_rfc3339(),
        elapsed_seconds
    );

    Ok(())
}

fn sync_single_chain(
    chain_id: u64,
    cli_binary: &Path,
    api_token: &str,
    commit_hash: &str,
    db_dir: &Path,
    manifest_path: &Path,
) -> Result<()> {
    println!("Starting sync for chain {chain_id}");
    let chain_start = Utc::now();

    let file_stem = chain_id.to_string();
    let (db_path, dump_path) = prepare_database(&file_stem, db_dir)?;
    let result = (|| -> Result<()> {
        let plan = plan_sync(&db_path, &dump_path)?;
        let plan_label = format!("chain {}", chain_id);
        log_plan(&plan_label, &plan);

        run_cli_sync(&RunCliSyncOptions {
            cli_binary: cli_binary.display().to_string(),
            db_path: db_path.display().to_string(),
            chain_id,
            api_token: Some(api_token.to_string()),
            repo_commit: commit_hash.to_string(),
            start_block: plan.next_start_block,
            end_block: None,
        })?;

        finalize_database(&file_stem, &db_path, &dump_path)?;
        Ok(())
    })();

    if let Err(error) = &result {
        eprintln!("Sync failed for chain {}: {error:?}", chain_id);
    }

    if db_path.exists() {
        let _ = fs::remove_file(&db_path);
    }

    result?;

    let completion_time = Utc::now();
    let dump_file_name = dump_path
        .file_name()
        .and_then(|name| name.to_str())
        .ok_or_else(|| anyhow::anyhow!("dump path is missing a valid filename"))?;
    let download_url = RELEASE_DOWNLOAD_URL_TEMPLATE.replace("{file}", dump_file_name);
    update_manifest(manifest_path, chain_id, &download_url, completion_time)?;
    println!(
        "Updated manifest entry for chain {} at {}",
        chain_id,
        manifest_path.display()
    );

    let duration = completion_time - chain_start;
    let elapsed_seconds = duration.num_milliseconds() as f64 / 1000.0;
    println!(
        "Chain {} completed at {} (duration: {:.1}s)",
        chain_id,
        completion_time.to_rfc3339(),
        elapsed_seconds
    );

    Ok(())
}

fn download_manifest_to_dir(http: &dyn HttpClient, manifest_path: &Path) -> Result<Manifest> {
    if let Some(parent) = manifest_path.parent() {
        fs::create_dir_all(parent)
            .with_context(|| format!("failed to create manifest directory {}", parent.display()))?;
    }

    let url = RELEASE_DOWNLOAD_URL_TEMPLATE.replace("{file}", "manifest.yaml");
    println!("Fetching manifest from {url}");

    match http.fetch_text(&url) {
        Ok(contents) => {
            let manifest: Manifest = serde_yaml::from_str(&contents)
                .with_context(|| format!("failed to parse manifest downloaded from {url}"))?;
            let normalized = normalize_yaml(&contents);
            fs::write(manifest_path, &normalized).with_context(|| {
                format!("failed to write manifest to {}", manifest_path.display())
            })?;
            Ok(manifest)
        }
        Err(error) => {
            println!("No manifest available at {url}; starting with empty manifest ({error})");
            let manifest = Manifest::new();
            let serialized = normalize_yaml(
                &serde_yaml::to_string(&manifest)
                    .context("failed to serialize manifest snapshot")?,
            );
            fs::write(manifest_path, &serialized).with_context(|| {
                format!("failed to write manifest to {}", manifest_path.display())
            })?;
            Ok(manifest)
        }
    }
}

fn download_dumps_for_manifest(
    http: &dyn HttpClient,
    manifest: &Manifest,
    db_dir: &Path,
) -> Result<()> {
    if manifest.networks.is_empty() {
        println!("Manifest has no networks; skipping dump hydration.");
        return Ok(());
    }

    fs::create_dir_all(db_dir)
        .with_context(|| format!("failed to create database directory {}", db_dir.display()))?;

    for network_id in manifest.networks.keys() {
        let chain_id = u64::from(*network_id);
        let file_name = format!("{chain_id}.sql.gz");
        let url = RELEASE_DOWNLOAD_URL_TEMPLATE.replace("{file}", &file_name);
        let destination = db_dir.join(&file_name);
        println!("Downloading dump for chain {chain_id} from {url}");
        let bytes = http.fetch_binary(&url).with_context(|| {
            format!(
                "failed to download dump for chain {} from {}",
                chain_id, url
            )
        })?;
        fs::write(&destination, &bytes).with_context(|| {
            format!(
                "failed to write dump for chain {} to {}",
                chain_id,
                destination.display()
            )
        })?;
    }

    Ok(())
}

fn normalize_yaml(contents: &str) -> String {
    if let Some(stripped) = contents.strip_prefix("---\n") {
        stripped.to_string()
    } else if let Some(stripped) = contents.strip_prefix("---\r\n") {
        stripped.to_string()
    } else {
        contents.to_string()
    }
}

fn resolve_api_token(env: &HashMap<String, String>) -> Result<String> {
    for key in API_TOKEN_ENV_VARS {
        if let Some(value) = env.get(*key) {
            let trimmed = value.trim();
            if !trimmed.is_empty() {
                return Ok(trimmed.to_string());
            }
        }
    }
    anyhow::bail!(
        "Missing API token. Set one of: {}.",
        API_TOKEN_ENV_VARS.join(", ")
    )
}

fn resolve_path(base: &Path, configured: &Path) -> PathBuf {
    if configured.is_absolute() {
        configured.to_path_buf()
    } else {
        base.join(configured)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::manifest::{ManifestEntry, NetworkId};
    use std::collections::HashMap as StdHashMap;
    use std::sync::Mutex;
    use tempfile::tempdir;

    #[test]
    fn resolve_api_token_uses_first_non_empty_value() {
        let mut env = HashMap::new();
        env.insert(API_TOKEN_ENV_VARS[0].to_string(), " token ".to_string());

        let token = resolve_api_token(&env).unwrap();
        assert_eq!(token, "token");
    }

    #[test]
    fn resolve_api_token_errors_when_missing() {
        let env = HashMap::new();
        let err = resolve_api_token(&env).unwrap_err();
        assert!(err.to_string().contains("Missing API token"));
    }

    #[test]
    fn resolve_path_returns_absolute_as_is() {
        let base = Path::new("/tmp");
        let configured = Path::new("/var/data");
        let resolved = resolve_path(base, configured);
        assert_eq!(resolved, configured);
    }

    #[test]
    fn resolve_path_joins_relative_with_base() {
        let base = Path::new("/tmp");
        let configured = Path::new("data/db");
        let resolved = resolve_path(base, configured);
        assert_eq!(resolved, base.join(configured));
    }

    #[test]
    fn normalize_yaml_strips_document_marker() {
        let input = "---\nschema_version: 1\n";
        let output = normalize_yaml(input);
        assert_eq!(output, "schema_version: 1\n");
    }

    #[test]
    fn download_manifest_to_dir_initializes_when_missing() {
        struct FailingHttpClient;

        impl HttpClient for FailingHttpClient {
            fn fetch_text(&self, _url: &str) -> Result<String> {
                Err(anyhow::anyhow!("404"))
            }

            fn fetch_binary(&self, _url: &str) -> Result<Vec<u8>> {
                Err(anyhow::anyhow!("unexpected binary request"))
            }
        }

        let temp = tempdir().unwrap();
        let path = temp.path().join("manifest.yaml");
        let manifest = download_manifest_to_dir(&FailingHttpClient, &path).unwrap();
        assert_eq!(manifest, Manifest::new());
        let persisted = std::fs::read_to_string(&path).unwrap();
        assert!(persisted.contains("schema_version"));
        assert!(!persisted.starts_with("---"));
    }

    #[test]
    fn download_dumps_for_manifest_writes_expected_files() {
        struct RecordingHttpClient {
            responses: Mutex<StdHashMap<String, Vec<u8>>>,
        }

        impl RecordingHttpClient {
            fn new(responses: StdHashMap<String, Vec<u8>>) -> Self {
                Self {
                    responses: Mutex::new(responses),
                }
            }
        }

        impl HttpClient for RecordingHttpClient {
            fn fetch_text(&self, url: &str) -> Result<String> {
                Err(anyhow::anyhow!("unexpected text request for {url}"))
            }

            fn fetch_binary(&self, url: &str) -> Result<Vec<u8>> {
                let mut guard = self.responses.lock().unwrap();
                guard
                    .remove(url)
                    .map(Ok)
                    .unwrap_or_else(|| Err(anyhow::anyhow!("unexpected url {url}")))
            }
        }

        let temp = tempdir().unwrap();
        let file_name = "10.sql.gz";
        let url = RELEASE_DOWNLOAD_URL_TEMPLATE.replace("{file}", file_name);
        let mut responses = StdHashMap::new();
        responses.insert(url.clone(), b"dump-bytes".to_vec());
        let client = RecordingHttpClient::new(responses);

        let mut manifest = Manifest::new();
        manifest.networks.insert(
            NetworkId::from(10u64),
            ManifestEntry {
                dump_url: url.clone(),
                dump_timestamp: Utc::now().to_rfc3339(),
                seed_generation: ManifestEntry::DEFAULT_SEED_GENERATION,
            },
        );

        download_dumps_for_manifest(&client, &manifest, temp.path()).unwrap();

        let dump_path = temp.path().join(file_name);
        let bytes = std::fs::read(&dump_path).unwrap();
        assert_eq!(bytes, b"dump-bytes".to_vec());
    }
}
