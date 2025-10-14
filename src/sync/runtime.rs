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
