use std::collections::HashMap;
use std::path::{Path, PathBuf};

use anyhow::{Context, Result};
use chrono::{DateTime, Utc};

use crate::archive::{download_cli_archive, extract_cli_binary};
use crate::cli::{run_cli_sync, RunCliSyncOptions};
use crate::database::{finalize_database, plan_sync, prepare_database, SyncPlan};
use crate::http::{DefaultHttpClient, HttpClient};
use crate::manifest::{update_manifest, Manifest};

pub trait CliRunner: Send + Sync {
    fn run(&self, options: &RunCliSyncOptions) -> Result<()>;
}

pub trait ArchiveService: Send + Sync {
    fn download_archive(
        &self,
        http: &dyn HttpClient,
        url: &str,
        destination: &Path,
    ) -> Result<PathBuf>;

    fn extract_binary(&self, archive_path: &Path, output_dir: &Path) -> Result<PathBuf>;
}

pub trait DatabaseManager: Send + Sync {
    fn prepare_database(&self, db_stem: &str, db_dir: &Path) -> Result<(PathBuf, PathBuf)>;
    fn plan_sync(&self, db_path: &Path, dump_path: &Path) -> Result<SyncPlan>;
    fn finalize_database(&self, db_stem: &str, db_path: &Path, dump_path: &Path) -> Result<()>;
}

pub trait ManifestService: Send + Sync {
    fn download_manifest(&self, http: &dyn HttpClient, manifest_path: &Path) -> Result<Manifest>;

    fn download_dumps(
        &self,
        http: &dyn HttpClient,
        manifest: &Manifest,
        db_dir: &Path,
    ) -> Result<()>;

    fn update_manifest(
        &self,
        manifest_path: &Path,
        chain_id: u64,
        download_url: &str,
        timestamp: DateTime<Utc>,
    ) -> Result<()>;
}

pub trait TimeProvider: Send + Sync {
    fn now(&self) -> DateTime<Utc>;
}

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
    pub cli_runner: Box<dyn CliRunner>,
    pub archive: Box<dyn ArchiveService>,
    pub database: Box<dyn DatabaseManager>,
    pub manifest: Box<dyn ManifestService>,
    pub time: Box<dyn TimeProvider>,
}

impl Default for SyncRuntime {
    fn default() -> Self {
        let env = std::env::vars().collect();
        let cwd = std::env::current_dir().expect("failed to read current directory");
        let http = Box::new(DefaultHttpClient::default()) as Box<dyn HttpClient>;
        let cli_runner = Box::new(DefaultCliRunner) as Box<dyn CliRunner>;
        let archive = Box::new(DefaultArchiveService) as Box<dyn ArchiveService>;
        let database = Box::new(DefaultDatabaseManager) as Box<dyn DatabaseManager>;
        let manifest = Box::new(DefaultManifestService) as Box<dyn ManifestService>;
        let time = Box::new(SystemTimeProvider) as Box<dyn TimeProvider>;

        Self {
            env,
            cwd,
            http,
            cli_runner,
            archive,
            database,
            manifest,
            time,
        }
    }
}

impl SyncRuntime {
    #[allow(dead_code)]
    pub fn with_http(mut self, http: Box<dyn HttpClient>) -> Self {
        self.http = http;
        self
    }
}

#[derive(Default)]
struct DefaultCliRunner;

impl CliRunner for DefaultCliRunner {
    fn run(&self, options: &RunCliSyncOptions) -> Result<()> {
        run_cli_sync(options)
    }
}

#[derive(Default)]
struct DefaultArchiveService;

impl ArchiveService for DefaultArchiveService {
    fn download_archive(
        &self,
        http: &dyn HttpClient,
        url: &str,
        destination: &Path,
    ) -> Result<PathBuf> {
        download_cli_archive(http, url, destination)
    }

    fn extract_binary(&self, archive_path: &Path, output_dir: &Path) -> Result<PathBuf> {
        extract_cli_binary(archive_path, output_dir)
    }
}

#[derive(Default)]
struct DefaultDatabaseManager;

impl DatabaseManager for DefaultDatabaseManager {
    fn prepare_database(&self, db_stem: &str, db_dir: &Path) -> Result<(PathBuf, PathBuf)> {
        prepare_database(db_stem, db_dir)
    }

    fn plan_sync(&self, db_path: &Path, dump_path: &Path) -> Result<SyncPlan> {
        plan_sync(db_path, dump_path)
    }

    fn finalize_database(&self, db_stem: &str, db_path: &Path, dump_path: &Path) -> Result<()> {
        finalize_database(db_stem, db_path, dump_path)
    }
}

#[derive(Default)]
struct DefaultManifestService;

impl ManifestService for DefaultManifestService {
    fn download_manifest(&self, http: &dyn HttpClient, manifest_path: &Path) -> Result<Manifest> {
        if let Some(parent) = manifest_path.parent() {
            std::fs::create_dir_all(parent).with_context(|| {
                format!("failed to create manifest directory {}", parent.display())
            })?;
        }

        let url =
            crate::constants::RELEASE_DOWNLOAD_URL_TEMPLATE.replace("{file}", "manifest.yaml");
        println!("Fetching manifest from {url}");

        match http.fetch_text(&url) {
            Ok(contents) => {
                let manifest: Manifest = serde_yaml::from_str(&contents)
                    .with_context(|| format!("failed to parse manifest downloaded from {url}"))?;
                let normalized = normalize_yaml(&contents);
                std::fs::write(manifest_path, &normalized).with_context(|| {
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
                std::fs::write(manifest_path, &serialized).with_context(|| {
                    format!("failed to write manifest to {}", manifest_path.display())
                })?;
                Ok(manifest)
            }
        }
    }

    fn download_dumps(
        &self,
        http: &dyn HttpClient,
        manifest: &Manifest,
        db_dir: &Path,
    ) -> Result<()> {
        if manifest.networks.is_empty() {
            println!("Manifest has no networks; skipping dump hydration.");
            return Ok(());
        }

        std::fs::create_dir_all(db_dir)
            .with_context(|| format!("failed to create database directory {}", db_dir.display()))?;

        for network_id in manifest.networks.keys() {
            let chain_id = u64::from(*network_id);
            let file_name = format!("{chain_id}.sql.gz");
            let url = crate::constants::RELEASE_DOWNLOAD_URL_TEMPLATE.replace("{file}", &file_name);
            let destination = db_dir.join(&file_name);
            println!("Downloading dump for chain {chain_id} from {url}");
            let bytes = http.fetch_binary(&url).with_context(|| {
                format!(
                    "failed to download dump for chain {} from {}",
                    chain_id, url
                )
            })?;
            std::fs::write(&destination, &bytes).with_context(|| {
                format!(
                    "failed to write dump for chain {} to {}",
                    chain_id,
                    destination.display()
                )
            })?;
        }

        Ok(())
    }

    fn update_manifest(
        &self,
        manifest_path: &Path,
        chain_id: u64,
        download_url: &str,
        timestamp: DateTime<Utc>,
    ) -> Result<()> {
        update_manifest(manifest_path, chain_id, download_url, timestamp)
    }
}

#[derive(Default)]
struct SystemTimeProvider;

impl TimeProvider for SystemTimeProvider {
    fn now(&self) -> DateTime<Utc> {
        Utc::now()
    }
}

pub(crate) fn normalize_yaml(contents: &str) -> String {
    if let Some(stripped) = contents.strip_prefix("---\n") {
        stripped.to_string()
    } else if let Some(stripped) = contents.strip_prefix("---\r\n") {
        stripped.to_string()
    } else {
        contents.to_string()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::http::HttpClient;
    use crate::manifest::{ManifestEntry, NetworkId};
    use std::sync::Mutex;
    use tempfile::tempdir;

    struct TextHttpClient {
        body: String,
        requests: Mutex<Vec<String>>,
    }

    impl TextHttpClient {
        fn new(body: &str) -> Self {
            Self {
                body: body.to_string(),
                requests: Mutex::new(Vec::new()),
            }
        }

        fn requests(&self) -> Vec<String> {
            self.requests.lock().unwrap().clone()
        }
    }

    impl HttpClient for TextHttpClient {
        fn fetch_text(&self, url: &str) -> Result<String> {
            self.requests.lock().unwrap().push(url.to_string());
            Ok(self.body.clone())
        }

        fn fetch_binary(&self, _url: &str) -> Result<Vec<u8>> {
            anyhow::bail!("unexpected binary request")
        }
    }

    struct FailingTextHttpClient {
        requests: Mutex<Vec<String>>,
        message: String,
    }

    impl FailingTextHttpClient {
        fn new(message: &str) -> Self {
            Self {
                requests: Mutex::new(Vec::new()),
                message: message.to_string(),
            }
        }

        fn requests(&self) -> Vec<String> {
            self.requests.lock().unwrap().clone()
        }
    }

    impl HttpClient for FailingTextHttpClient {
        fn fetch_text(&self, url: &str) -> Result<String> {
            self.requests.lock().unwrap().push(url.to_string());
            anyhow::bail!(self.message.clone())
        }

        fn fetch_binary(&self, _url: &str) -> Result<Vec<u8>> {
            anyhow::bail!("unexpected binary request")
        }
    }

    struct BinaryHttpClient {
        payload: Vec<u8>,
        requests: Mutex<Vec<String>>,
    }

    impl BinaryHttpClient {
        fn new(payload: &[u8]) -> Self {
            Self {
                payload: payload.to_vec(),
                requests: Mutex::new(Vec::new()),
            }
        }

        fn requests(&self) -> Vec<String> {
            self.requests.lock().unwrap().clone()
        }
    }

    impl HttpClient for BinaryHttpClient {
        fn fetch_text(&self, _url: &str) -> Result<String> {
            anyhow::bail!("unexpected text request")
        }

        fn fetch_binary(&self, url: &str) -> Result<Vec<u8>> {
            self.requests.lock().unwrap().push(url.to_string());
            Ok(self.payload.clone())
        }
    }

    #[test]
    fn download_manifest_writes_normalized_contents() {
        let temp = tempdir().unwrap();
        let manifest_path = temp.path().join("manifest.yaml");
        let http = TextHttpClient::new(
            r#"---
schema_version: 1
networks: {}
"#,
        );
        let service = DefaultManifestService;

        let manifest = service
            .download_manifest(&http, &manifest_path)
            .expect("manifest should load");

        assert_eq!(manifest.schema_version, 1);
        assert!(manifest_path.exists());
        let stored = std::fs::read_to_string(&manifest_path).unwrap();
        assert!(
            !stored.starts_with("---"),
            "document marker should be stripped: {stored}"
        );
        assert_eq!(
            http.requests(),
            vec![crate::constants::RELEASE_DOWNLOAD_URL_TEMPLATE.replace("{file}", "manifest.yaml")]
        );
    }

    #[test]
    fn download_manifest_falls_back_to_empty_manifest_on_failure() {
        let temp = tempdir().unwrap();
        let manifest_path = temp.path().join("manifest.yaml");
        let http = FailingTextHttpClient::new("network error");
        let service = DefaultManifestService;

        let manifest = service
            .download_manifest(&http, &manifest_path)
            .expect("fallback manifest should be created");

        assert_eq!(manifest.networks.len(), 0);
        assert!(manifest_path.exists());
        let stored = std::fs::read_to_string(&manifest_path).unwrap();
        assert!(
            stored.contains("schema_version"),
            "manifest contents unexpected: {stored}"
        );
        assert_eq!(
            http.requests(),
            vec![crate::constants::RELEASE_DOWNLOAD_URL_TEMPLATE.replace("{file}", "manifest.yaml")]
        );
    }

    #[test]
    fn download_dumps_writes_dump_files_per_network_entry() {
        let temp = tempdir().unwrap();
        let db_dir = temp.path();
        let manifest = Manifest {
            schema_version: Manifest::CURRENT_SCHEMA_VERSION,
            networks: vec![(
                NetworkId::from(123u64),
                ManifestEntry {
                    dump_url: "https://example.com/123.sql.gz".to_string(),
                    dump_timestamp: "2024-01-01T00:00:00Z".to_string(),
                    seed_generation: ManifestEntry::DEFAULT_SEED_GENERATION,
                },
            )]
            .into_iter()
            .collect(),
        };
        let http = BinaryHttpClient::new(b"dump-bytes");
        let service = DefaultManifestService;

        service
            .download_dumps(&http, &manifest, db_dir)
            .expect("dumps should download");

        let dump_path = db_dir.join("123.sql.gz");
        assert!(dump_path.exists());
        let bytes = std::fs::read(&dump_path).unwrap();
        assert_eq!(bytes, b"dump-bytes");
        assert_eq!(
            http.requests(),
            vec![crate::constants::RELEASE_DOWNLOAD_URL_TEMPLATE.replace("{file}", "123.sql.gz")]
        );
    }

    #[test]
    fn download_dumps_noops_when_manifest_empty() {
        let temp = tempdir().unwrap();
        let db_dir = temp.path();
        let manifest = Manifest::new();
        let http = BinaryHttpClient::new(b"unused");
        let service = DefaultManifestService;

        service
            .download_dumps(&http, &manifest, db_dir)
            .expect("empty manifest should skip downloads");

        assert!(std::fs::read_dir(db_dir).unwrap().next().is_none());
        assert!(http.requests().is_empty());
    }
}
