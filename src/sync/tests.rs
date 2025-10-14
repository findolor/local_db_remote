use std::collections::HashMap;
use std::collections::VecDeque;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};

use anyhow::{anyhow, Result};
use chrono::TimeZone;
use tempfile::tempdir;

use super::orchestrator::run_sync_with;
use super::runtime::{
    normalize_yaml, ArchiveService, CliRunner, DatabaseManager, ManifestService, SyncConfig,
    SyncRuntime, TimeProvider,
};
use crate::cli::RunCliSyncOptions;
use crate::constants::{
    API_TOKEN_ENV_VARS, CLI_ARCHIVE_NAME, CLI_BINARY_URL_ENV_VAR, RELEASE_DOWNLOAD_URL_TEMPLATE,
    SETTINGS_YAML_ENV_VAR, SYNC_CHAIN_IDS_ENV_VAR,
};
use crate::database::SyncPlan;
use crate::http::HttpClient;
use crate::manifest::{Manifest, ManifestEntry, NetworkId};

#[derive(Clone, Default)]
struct MockCliRunner {
    inner: Arc<MockCliRunnerState>,
}

#[derive(Default)]
struct MockCliRunnerState {
    calls: Mutex<Vec<RunCliSyncOptions>>,
    fail_next: Mutex<Option<String>>,
}

impl MockCliRunner {
    fn calls(&self) -> Vec<RunCliSyncOptions> {
        self.inner.calls.lock().unwrap().clone()
    }

    fn fail_next_with(&self, message: &str) {
        *self.inner.fail_next.lock().unwrap() = Some(message.to_string());
    }
}

impl CliRunner for MockCliRunner {
    fn run(&self, options: &RunCliSyncOptions) -> Result<()> {
        self.inner.calls.lock().unwrap().push(options.clone());
        if let Some(message) = self.inner.fail_next.lock().unwrap().take() {
            anyhow::bail!(message);
        }
        Ok(())
    }
}

#[derive(Clone, Default)]
struct MockArchiveService {
    inner: Arc<MockArchiveState>,
}

#[derive(Default)]
struct MockArchiveState {
    download_calls: Mutex<Vec<(String, PathBuf)>>,
    extract_calls: Mutex<Vec<(PathBuf, PathBuf)>>,
}

impl MockArchiveService {
    fn download_calls(&self) -> Vec<(String, PathBuf)> {
        self.inner.download_calls.lock().unwrap().clone()
    }

    fn extract_calls(&self) -> Vec<(PathBuf, PathBuf)> {
        self.inner.extract_calls.lock().unwrap().clone()
    }
}

impl ArchiveService for MockArchiveService {
    fn download_archive(
        &self,
        _http: &dyn HttpClient,
        url: &str,
        destination: &Path,
    ) -> Result<PathBuf> {
        self.inner
            .download_calls
            .lock()
            .unwrap()
            .push((url.to_string(), destination.to_path_buf()));
        if let Some(parent) = destination.parent() {
            std::fs::create_dir_all(parent)?;
        }
        std::fs::write(destination, b"archive-bytes")?;
        Ok(destination.to_path_buf())
    }

    fn extract_binary(&self, archive_path: &Path, output_dir: &Path) -> Result<PathBuf> {
        self.inner
            .extract_calls
            .lock()
            .unwrap()
            .push((archive_path.to_path_buf(), output_dir.to_path_buf()));
        std::fs::create_dir_all(output_dir)?;
        let binary_path = output_dir.join("rain-orderbook-cli");
        std::fs::write(&binary_path, b"#!/bin/sh\necho mock\n")?;
        Ok(binary_path)
    }
}

#[derive(Clone)]
struct MockDatabaseManager {
    inner: Arc<MockDatabaseState>,
}

struct MockDatabaseState {
    prepare_calls: Mutex<Vec<(String, PathBuf)>>,
    plan_calls: Mutex<Vec<(PathBuf, PathBuf)>>,
    finalize_calls: Mutex<Vec<(String, PathBuf, PathBuf)>>,
    plan_template: Mutex<SyncPlan>,
}

impl Default for MockDatabaseState {
    fn default() -> Self {
        Self {
            prepare_calls: Default::default(),
            plan_calls: Default::default(),
            finalize_calls: Default::default(),
            plan_template: Mutex::new(SyncPlan {
                db_path: PathBuf::new(),
                dump_path: PathBuf::new(),
                last_synced_block: None,
                next_start_block: None,
            }),
        }
    }
}

impl MockDatabaseManager {
    fn new(plan: SyncPlan) -> Self {
        Self {
            inner: Arc::new(MockDatabaseState {
                plan_template: Mutex::new(plan),
                ..Default::default()
            }),
        }
    }

    fn prepare_calls(&self) -> Vec<(String, PathBuf)> {
        self.inner.prepare_calls.lock().unwrap().clone()
    }

    fn plan_calls(&self) -> Vec<(PathBuf, PathBuf)> {
        self.inner.plan_calls.lock().unwrap().clone()
    }

    fn finalize_calls(&self) -> Vec<(String, PathBuf, PathBuf)> {
        self.inner.finalize_calls.lock().unwrap().clone()
    }
}

impl DatabaseManager for MockDatabaseManager {
    fn prepare_database(&self, db_stem: &str, db_dir: &Path) -> Result<(PathBuf, PathBuf)> {
        self.inner
            .prepare_calls
            .lock()
            .unwrap()
            .push((db_stem.to_string(), db_dir.to_path_buf()));
        std::fs::create_dir_all(db_dir)?;
        let db_path = db_dir.join(format!("{db_stem}.db"));
        std::fs::write(&db_path, b"db-bytes")?;
        let dump_path = db_dir.join(format!("{db_stem}.sql.gz"));
        Ok((db_path, dump_path))
    }

    fn plan_sync(&self, db_path: &Path, dump_path: &Path) -> Result<SyncPlan> {
        self.inner
            .plan_calls
            .lock()
            .unwrap()
            .push((db_path.to_path_buf(), dump_path.to_path_buf()));
        let mut template = self.inner.plan_template.lock().unwrap().clone();
        template.db_path = db_path.to_path_buf();
        template.dump_path = dump_path.to_path_buf();
        Ok(template)
    }

    fn finalize_database(&self, db_stem: &str, db_path: &Path, dump_path: &Path) -> Result<()> {
        self.inner.finalize_calls.lock().unwrap().push((
            db_stem.to_string(),
            db_path.to_path_buf(),
            dump_path.to_path_buf(),
        ));
        if db_path.exists() {
            std::fs::remove_file(db_path)?;
        }
        std::fs::write(dump_path, b"compressed-bytes")?;
        Ok(())
    }
}

#[derive(Clone)]
struct MockManifestService {
    inner: Arc<MockManifestState>,
}

type ManifestUpdate = (PathBuf, u64, String, chrono::DateTime<chrono::Utc>);

struct MockManifestState {
    manifest: Manifest,
    download_calls: Mutex<Vec<PathBuf>>,
    download_dumps_calls: Mutex<Vec<PathBuf>>,
    updates: Mutex<Vec<ManifestUpdate>>,
}

impl MockManifestService {
    fn new(manifest: Manifest) -> Self {
        Self {
            inner: Arc::new(MockManifestState {
                manifest,
                download_calls: Default::default(),
                download_dumps_calls: Default::default(),
                updates: Default::default(),
            }),
        }
    }

    fn download_calls(&self) -> Vec<PathBuf> {
        self.inner.download_calls.lock().unwrap().clone()
    }

    fn download_dumps_calls(&self) -> Vec<PathBuf> {
        self.inner.download_dumps_calls.lock().unwrap().clone()
    }

    fn updates(&self) -> Vec<ManifestUpdate> {
        self.inner.updates.lock().unwrap().clone()
    }
}

impl ManifestService for MockManifestService {
    fn download_manifest(&self, _http: &dyn HttpClient, manifest_path: &Path) -> Result<Manifest> {
        self.inner
            .download_calls
            .lock()
            .unwrap()
            .push(manifest_path.to_path_buf());
        Ok(self.inner.manifest.clone())
    }

    fn download_dumps(
        &self,
        _http: &dyn HttpClient,
        _manifest: &Manifest,
        db_dir: &Path,
    ) -> Result<()> {
        self.inner
            .download_dumps_calls
            .lock()
            .unwrap()
            .push(db_dir.to_path_buf());
        Ok(())
    }

    fn update_manifest(
        &self,
        manifest_path: &Path,
        chain_id: u64,
        download_url: &str,
        timestamp: chrono::DateTime<chrono::Utc>,
    ) -> Result<()> {
        self.inner.updates.lock().unwrap().push((
            manifest_path.to_path_buf(),
            chain_id,
            download_url.to_string(),
            timestamp,
        ));
        Ok(())
    }
}

#[derive(Clone)]
struct MockTimeProvider {
    times: Arc<Mutex<VecDeque<chrono::DateTime<chrono::Utc>>>>,
}

impl MockTimeProvider {
    fn new(times: Vec<chrono::DateTime<chrono::Utc>>) -> Self {
        Self {
            times: Arc::new(Mutex::new(VecDeque::from(times))),
        }
    }

    fn remaining(&self) -> usize {
        self.times.lock().unwrap().len()
    }
}

impl TimeProvider for MockTimeProvider {
    fn now(&self) -> chrono::DateTime<chrono::Utc> {
        self.times
            .lock()
            .unwrap()
            .pop_front()
            .expect("no time values remaining")
    }
}

#[derive(Clone)]
struct StubHttpClient {
    inner: Arc<StubHttpState>,
}

struct StubHttpState {
    response: String,
    requests: Mutex<Vec<String>>,
}

impl StubHttpClient {
    fn new(response: &str) -> Self {
        Self {
            inner: Arc::new(StubHttpState {
                response: response.to_string(),
                requests: Default::default(),
            }),
        }
    }

    fn requests(&self) -> Vec<String> {
        self.inner.requests.lock().unwrap().clone()
    }
}

impl HttpClient for StubHttpClient {
    fn fetch_text(&self, url: &str) -> Result<String> {
        self.inner.requests.lock().unwrap().push(url.to_string());
        Ok(self.inner.response.clone())
    }

    fn fetch_binary(&self, _url: &str) -> Result<Vec<u8>> {
        Err(anyhow!("unexpected binary request"))
    }
}

fn base_env() -> HashMap<String, String> {
    let mut env = HashMap::new();
    env.insert(
        CLI_BINARY_URL_ENV_VAR.to_string(),
        "https://example.com/cli.tar.gz".to_string(),
    );
    env.insert(
        SETTINGS_YAML_ENV_VAR.to_string(),
        "https://example.com/settings.yaml".to_string(),
    );
    env.insert(API_TOKEN_ENV_VARS[0].to_string(), "token".to_string());
    env
}

fn make_time_provider(count: usize) -> MockTimeProvider {
    let times = (0..count)
        .map(|offset| {
            chrono::Utc
                .with_ymd_and_hms(2024, 1, 1, 0, 0, offset as u32)
                .unwrap()
        })
        .collect::<Vec<_>>();
    MockTimeProvider::new(times)
}

fn manifest_with_chain(chain_id: u64) -> Manifest {
    let mut manifest = Manifest::new();
    manifest.networks.insert(
        NetworkId::from(chain_id),
        ManifestEntry {
            dump_url: format!("https://example.com/{chain_id}.sql.gz"),
            dump_timestamp: "2024-01-01T00:00:00Z".to_string(),
            seed_generation: ManifestEntry::DEFAULT_SEED_GENERATION,
        },
    );
    manifest
}

#[test]
fn normalize_yaml_strips_document_marker() {
    let input = "---\nschema_version: 1\n";
    let output = normalize_yaml(input);
    assert_eq!(output, "schema_version: 1\n");
}

#[test]
fn run_sync_with_uses_injected_services() {
    let temp = tempdir().unwrap();
    let cwd = temp.path().to_path_buf();

    let chain_id = 42161u64;
    let manifest = manifest_with_chain(chain_id);

    let cli_runner = MockCliRunner::default();
    let archive = MockArchiveService::default();
    let plan = SyncPlan {
        db_path: PathBuf::new(),
        dump_path: PathBuf::new(),
        last_synced_block: Some(1),
        next_start_block: Some(2),
    };
    let database = MockDatabaseManager::new(plan);
    let manifest_service = MockManifestService::new(manifest);
    let time_provider = make_time_provider(4);
    let http_client = StubHttpClient::new("settings: true");
    let env = base_env();

    let runtime = SyncRuntime {
        env,
        cwd: cwd.clone(),
        http: Box::new(http_client.clone()),
        cli_runner: Box::new(cli_runner.clone()),
        archive: Box::new(archive.clone()),
        database: Box::new(database.clone()),
        manifest: Box::new(manifest_service.clone()),
        time: Box::new(time_provider.clone()),
    };

    run_sync_with(runtime, SyncConfig::default()).unwrap();

    let calls = cli_runner.calls();
    assert_eq!(calls.len(), 1);
    let call = &calls[0];
    assert_eq!(call.chain_id, chain_id);
    assert_eq!(call.start_block, Some(2));
    assert_eq!(call.settings_yaml, "settings: true");
    assert!(call.cli_binary.ends_with("bin/rain-orderbook-cli"));

    let prepare_calls = database.prepare_calls();
    assert_eq!(prepare_calls.len(), 1);
    assert_eq!(prepare_calls[0].0, chain_id.to_string());
    assert_eq!(prepare_calls[0].1, cwd.join("data"));

    let plan_calls = database.plan_calls();
    assert_eq!(plan_calls.len(), 1);
    assert_eq!(plan_calls[0].0, cwd.join(format!("data/{chain_id}.db")));

    let finalize_calls = database.finalize_calls();
    assert_eq!(finalize_calls.len(), 1);
    assert_eq!(finalize_calls[0].0, chain_id.to_string());
    assert_eq!(finalize_calls[0].1, cwd.join(format!("data/{chain_id}.db")));
    assert!(finalize_calls[0].2.exists());

    let manifest_downloads = manifest_service.download_calls();
    assert_eq!(manifest_downloads, vec![cwd.join("data/manifest.yaml")]);

    let dumps_calls = manifest_service.download_dumps_calls();
    assert_eq!(dumps_calls, vec![cwd.join("data")]);

    let updates = manifest_service.updates();
    assert_eq!(updates.len(), 1);
    let (path, updated_chain, url, timestamp) = &updates[0];
    assert_eq!(path, &cwd.join("data/manifest.yaml"));
    assert_eq!(*updated_chain, chain_id);
    let expected_url =
        RELEASE_DOWNLOAD_URL_TEMPLATE.replace("{file}", &format!("{chain_id}.sql.gz"));
    assert_eq!(url, &expected_url);
    assert_eq!(
        *timestamp,
        chrono::Utc.with_ymd_and_hms(2024, 1, 1, 0, 0, 2).unwrap()
    );

    let archive_downloads = archive.download_calls();
    assert_eq!(archive_downloads.len(), 1);
    assert_eq!(archive_downloads[0].0, "https://example.com/cli.tar.gz");
    assert_eq!(archive_downloads[0].1, cwd.join(CLI_ARCHIVE_NAME));

    let archive_extracts = archive.extract_calls();
    assert_eq!(archive_extracts.len(), 1);
    assert_eq!(archive_extracts[0].1, cwd.join("bin"));

    assert_eq!(
        http_client.requests(),
        vec!["https://example.com/settings.yaml".to_string()]
    );
    assert_eq!(time_provider.remaining(), 0);
}

#[test]
fn run_sync_with_fails_when_archive_download_fails() {
    struct FailingArchive;

    impl ArchiveService for FailingArchive {
        fn download_archive(
            &self,
            _http: &dyn HttpClient,
            _url: &str,
            _destination: &Path,
        ) -> Result<PathBuf> {
            anyhow::bail!("archive download failed");
        }

        fn extract_binary(&self, _archive_path: &Path, _output_dir: &Path) -> Result<PathBuf> {
            unreachable!("extract should not be called");
        }
    }

    let temp = tempdir().unwrap();
    let cwd = temp.path().to_path_buf();

    let cli_runner = MockCliRunner::default();
    let manifest_service = MockManifestService::new(Manifest::new());
    let database = MockDatabaseManager::new(SyncPlan {
        db_path: PathBuf::new(),
        dump_path: PathBuf::new(),
        last_synced_block: None,
        next_start_block: None,
    });
    let time_provider = make_time_provider(1);
    let http_client = StubHttpClient::new("settings: true");
    let env = base_env();

    let runtime = SyncRuntime {
        env,
        cwd,
        http: Box::new(http_client),
        cli_runner: Box::new(cli_runner.clone()),
        archive: Box::new(FailingArchive),
        database: Box::new(database),
        manifest: Box::new(manifest_service),
        time: Box::new(time_provider),
    };

    let err = run_sync_with(runtime, SyncConfig::default()).unwrap_err();
    assert!(err.to_string().contains("archive download failed"));
    assert!(cli_runner.calls().is_empty());
}

#[test]
fn run_sync_with_fails_when_archive_extract_fails() {
    struct ExtractFailArchive;

    impl ArchiveService for ExtractFailArchive {
        fn download_archive(
            &self,
            _http: &dyn HttpClient,
            _url: &str,
            destination: &Path,
        ) -> Result<PathBuf> {
            if let Some(parent) = destination.parent() {
                std::fs::create_dir_all(parent)?;
            }
            std::fs::write(destination, b"bytes")?;
            Ok(destination.to_path_buf())
        }

        fn extract_binary(&self, _archive_path: &Path, _output_dir: &Path) -> Result<PathBuf> {
            anyhow::bail!("archive extract failed");
        }
    }

    let temp = tempdir().unwrap();
    let cwd = temp.path().to_path_buf();

    let cli_runner = MockCliRunner::default();
    let manifest_service = MockManifestService::new(Manifest::new());
    let database = MockDatabaseManager::new(SyncPlan {
        db_path: PathBuf::new(),
        dump_path: PathBuf::new(),
        last_synced_block: None,
        next_start_block: None,
    });
    let time_provider = make_time_provider(1);
    let http_client = StubHttpClient::new("settings: true");
    let env = base_env();

    let runtime = SyncRuntime {
        env,
        cwd,
        http: Box::new(http_client),
        cli_runner: Box::new(cli_runner.clone()),
        archive: Box::new(ExtractFailArchive),
        database: Box::new(database),
        manifest: Box::new(manifest_service),
        time: Box::new(time_provider),
    };

    let err = run_sync_with(runtime, SyncConfig::default()).unwrap_err();
    assert!(err.to_string().contains("archive extract failed"));
    assert!(cli_runner.calls().is_empty());
}

#[test]
fn run_sync_with_propagates_cli_error() {
    let temp = tempdir().unwrap();
    let cwd = temp.path().to_path_buf();

    let chain_id = 10u64;
    let manifest = manifest_with_chain(chain_id);

    let cli_runner = MockCliRunner::default();
    cli_runner.fail_next_with("cli failed");
    let archive = MockArchiveService::default();
    let plan = SyncPlan {
        db_path: PathBuf::new(),
        dump_path: PathBuf::new(),
        last_synced_block: None,
        next_start_block: None,
    };
    let database = MockDatabaseManager::new(plan);
    let manifest_service = MockManifestService::new(manifest);
    let time_provider = make_time_provider(2);
    let http_client = StubHttpClient::new("settings: true");
    let env = base_env();

    let runtime = SyncRuntime {
        env,
        cwd: cwd.clone(),
        http: Box::new(http_client),
        cli_runner: Box::new(cli_runner.clone()),
        archive: Box::new(archive.clone()),
        database: Box::new(database.clone()),
        manifest: Box::new(manifest_service.clone()),
        time: Box::new(time_provider.clone()),
    };

    let err = run_sync_with(runtime, SyncConfig::default()).unwrap_err();
    assert!(err.to_string().contains("cli failed"));

    assert_eq!(cli_runner.calls().len(), 1);
    assert_eq!(database.prepare_calls().len(), 1);
    assert_eq!(database.plan_calls().len(), 1);
    assert!(database.finalize_calls().is_empty());
    assert!(manifest_service.updates().is_empty());
    assert_eq!(time_provider.remaining(), 0);
    let db_path = cwd.join(format!("data/{chain_id}.db"));
    assert!(!db_path.exists());
}

#[test]
fn run_sync_with_processes_manifest_and_config_chain_ids() {
    let temp = tempdir().unwrap();
    let cwd = temp.path().to_path_buf();

    let manifest_chain = 100u64;
    let config_chain = 200u64;
    let manifest = manifest_with_chain(manifest_chain);

    let cli_runner = MockCliRunner::default();
    let archive = MockArchiveService::default();
    let plan = SyncPlan {
        db_path: PathBuf::new(),
        dump_path: PathBuf::new(),
        last_synced_block: Some(10),
        next_start_block: Some(11),
    };
    let database = MockDatabaseManager::new(plan);
    let manifest_service = MockManifestService::new(manifest);
    let time_provider = make_time_provider(6);
    let http_client = StubHttpClient::new("settings: true");
    let env = base_env();

    let runtime = SyncRuntime {
        env,
        cwd: cwd.clone(),
        http: Box::new(http_client.clone()),
        cli_runner: Box::new(cli_runner.clone()),
        archive: Box::new(archive.clone()),
        database: Box::new(database.clone()),
        manifest: Box::new(manifest_service.clone()),
        time: Box::new(time_provider.clone()),
    };

    let mut config = SyncConfig::default();
    config.chain_ids.push(config_chain);

    run_sync_with(runtime, config).unwrap();

    let calls = cli_runner.calls();
    assert_eq!(calls.len(), 2);
    let chains: Vec<u64> = calls.iter().map(|call| call.chain_id).collect();
    assert_eq!(chains, vec![manifest_chain, config_chain]);
    for call in &calls {
        assert_eq!(call.start_block, Some(11));
        assert_eq!(call.api_token.as_deref(), Some("token"));
    }

    let updates = manifest_service.updates();
    assert_eq!(updates.len(), 2);
    assert_eq!(updates[0].1, manifest_chain);
    assert_eq!(updates[1].1, config_chain);

    let prepare_calls = database.prepare_calls();
    assert_eq!(prepare_calls.len(), 2);
    assert_eq!(prepare_calls[0].0, manifest_chain.to_string());
    assert_eq!(prepare_calls[1].0, config_chain.to_string());

    let plan_calls = database.plan_calls();
    assert_eq!(plan_calls.len(), 2);
    assert_eq!(
        plan_calls[0].0,
        cwd.join(format!("data/{manifest_chain}.db"))
    );
    assert_eq!(plan_calls[1].0, cwd.join(format!("data/{config_chain}.db")));

    assert_eq!(archive.download_calls().len(), 1);
    assert_eq!(archive.extract_calls().len(), 1);
    assert_eq!(
        http_client.requests(),
        vec!["https://example.com/settings.yaml".to_string()]
    );
    assert_eq!(time_provider.remaining(), 0);
}

#[test]
fn run_sync_with_processes_env_chain_ids() {
    let temp = tempdir().unwrap();
    let cwd = temp.path().to_path_buf();

    let manifest = Manifest::new();

    let cli_runner = MockCliRunner::default();
    let archive = MockArchiveService::default();
    let plan = SyncPlan {
        db_path: PathBuf::new(),
        dump_path: PathBuf::new(),
        last_synced_block: Some(20),
        next_start_block: Some(21),
    };
    let database = MockDatabaseManager::new(plan);
    let manifest_service = MockManifestService::new(manifest);
    let time_provider = make_time_provider(8);
    let http_client = StubHttpClient::new("settings: true");
    let mut env = base_env();
    env.insert(
        SYNC_CHAIN_IDS_ENV_VAR.to_string(),
        "101, 202,303".to_string(),
    );

    let runtime = SyncRuntime {
        env,
        cwd: cwd.clone(),
        http: Box::new(http_client.clone()),
        cli_runner: Box::new(cli_runner.clone()),
        archive: Box::new(archive.clone()),
        database: Box::new(database.clone()),
        manifest: Box::new(manifest_service.clone()),
        time: Box::new(time_provider.clone()),
    };

    run_sync_with(runtime, SyncConfig::default()).unwrap();

    let calls = cli_runner.calls();
    assert_eq!(calls.len(), 3);
    let chains: Vec<u64> = calls.iter().map(|call| call.chain_id).collect();
    assert_eq!(chains, vec![101, 202, 303]);
    for call in &calls {
        assert_eq!(call.start_block, Some(21));
        assert_eq!(call.api_token.as_deref(), Some("token"));
    }

    let updates = manifest_service.updates();
    assert_eq!(updates.len(), 3);
    assert_eq!(updates[0].1, 101);
    assert_eq!(updates[1].1, 202);
    assert_eq!(updates[2].1, 303);

    let prepare_calls = database.prepare_calls();
    assert_eq!(prepare_calls.len(), 3);
    assert_eq!(prepare_calls[0].0, "101");
    assert_eq!(prepare_calls[1].0, "202");
    assert_eq!(prepare_calls[2].0, "303");

    let plan_calls = database.plan_calls();
    assert_eq!(plan_calls.len(), 3);
    assert_eq!(plan_calls[0].0, cwd.join("data/101.db"));
    assert_eq!(plan_calls[1].0, cwd.join("data/202.db"));
    assert_eq!(plan_calls[2].0, cwd.join("data/303.db"));

    assert_eq!(archive.download_calls().len(), 1);
    assert_eq!(archive.extract_calls().len(), 1);
    assert_eq!(
        http_client.requests(),
        vec!["https://example.com/settings.yaml".to_string()]
    );
    assert_eq!(time_provider.remaining(), 0);
}

#[test]
fn run_sync_with_fails_when_manifest_download_fails() {
    struct DownloadFailManifest;

    impl ManifestService for DownloadFailManifest {
        fn download_manifest(
            &self,
            _http: &dyn HttpClient,
            _manifest_path: &Path,
        ) -> Result<Manifest> {
            anyhow::bail!("manifest download failed");
        }

        fn download_dumps(
            &self,
            _http: &dyn HttpClient,
            _manifest: &Manifest,
            _db_dir: &Path,
        ) -> Result<()> {
            unreachable!("download_dumps not expected");
        }

        fn update_manifest(
            &self,
            _manifest_path: &Path,
            _chain_id: u64,
            _download_url: &str,
            _timestamp: chrono::DateTime<chrono::Utc>,
        ) -> Result<()> {
            unreachable!("update_manifest not expected");
        }
    }

    let temp = tempdir().unwrap();
    let cwd = temp.path().to_path_buf();

    let cli_runner = MockCliRunner::default();
    let archive = MockArchiveService::default();
    let database = MockDatabaseManager::new(SyncPlan {
        db_path: PathBuf::new(),
        dump_path: PathBuf::new(),
        last_synced_block: None,
        next_start_block: None,
    });
    let time_provider = make_time_provider(1);
    let http_client = StubHttpClient::new("settings: true");
    let env = base_env();

    let runtime = SyncRuntime {
        env,
        cwd,
        http: Box::new(http_client),
        cli_runner: Box::new(cli_runner.clone()),
        archive: Box::new(archive),
        database: Box::new(database),
        manifest: Box::new(DownloadFailManifest),
        time: Box::new(time_provider),
    };

    let err = run_sync_with(runtime, SyncConfig::default()).unwrap_err();
    assert_eq!(err.root_cause().to_string(), "manifest download failed");
    assert!(cli_runner.calls().is_empty());
}

#[test]
fn run_sync_with_fails_when_manifest_dump_hydration_fails() {
    struct DumpFailManifest {
        manifest: Manifest,
    }

    impl ManifestService for DumpFailManifest {
        fn download_manifest(
            &self,
            _http: &dyn HttpClient,
            _manifest_path: &Path,
        ) -> Result<Manifest> {
            Ok(self.manifest.clone())
        }

        fn download_dumps(
            &self,
            _http: &dyn HttpClient,
            _manifest: &Manifest,
            _db_dir: &Path,
        ) -> Result<()> {
            anyhow::bail!("dump hydration failed");
        }

        fn update_manifest(
            &self,
            _manifest_path: &Path,
            _chain_id: u64,
            _download_url: &str,
            _timestamp: chrono::DateTime<chrono::Utc>,
        ) -> Result<()> {
            unreachable!("update_manifest not expected");
        }
    }

    let temp = tempdir().unwrap();
    let cwd = temp.path().to_path_buf();

    let cli_runner = MockCliRunner::default();
    let archive = MockArchiveService::default();
    let database = MockDatabaseManager::new(SyncPlan {
        db_path: PathBuf::new(),
        dump_path: PathBuf::new(),
        last_synced_block: None,
        next_start_block: None,
    });
    let time_provider = make_time_provider(1);
    let http_client = StubHttpClient::new("settings: true");
    let env = base_env();

    let runtime = SyncRuntime {
        env,
        cwd,
        http: Box::new(http_client),
        cli_runner: Box::new(cli_runner.clone()),
        archive: Box::new(archive),
        database: Box::new(database),
        manifest: Box::new(DumpFailManifest {
            manifest: manifest_with_chain(100),
        }),
        time: Box::new(time_provider),
    };

    let err = run_sync_with(runtime, SyncConfig::default()).unwrap_err();
    assert_eq!(err.root_cause().to_string(), "dump hydration failed");
    assert!(cli_runner.calls().is_empty());
}

#[test]
fn run_sync_with_fails_when_manifest_update_fails() {
    struct UpdateFailManifest {
        manifest: Manifest,
        updates: Arc<Mutex<Vec<(PathBuf, u64, String)>>>,
    }

    impl UpdateFailManifest {
        fn new(manifest: Manifest) -> Self {
            Self {
                manifest,
                updates: Arc::new(Mutex::new(Vec::new())),
            }
        }
    }

    impl ManifestService for UpdateFailManifest {
        fn download_manifest(
            &self,
            _http: &dyn HttpClient,
            _manifest_path: &Path,
        ) -> Result<Manifest> {
            Ok(self.manifest.clone())
        }

        fn download_dumps(
            &self,
            _http: &dyn HttpClient,
            _manifest: &Manifest,
            _db_dir: &Path,
        ) -> Result<()> {
            Ok(())
        }

        fn update_manifest(
            &self,
            manifest_path: &Path,
            chain_id: u64,
            download_url: &str,
            _timestamp: chrono::DateTime<chrono::Utc>,
        ) -> Result<()> {
            self.updates.lock().unwrap().push((
                manifest_path.to_path_buf(),
                chain_id,
                download_url.to_string(),
            ));
            anyhow::bail!("manifest update failed");
        }
    }

    let temp = tempdir().unwrap();
    let cwd = temp.path().to_path_buf();

    let manifest_chain = 123u64;
    let manifest = manifest_with_chain(manifest_chain);

    let cli_runner = MockCliRunner::default();
    let archive = MockArchiveService::default();
    let plan = SyncPlan {
        db_path: PathBuf::new(),
        dump_path: PathBuf::new(),
        last_synced_block: Some(5),
        next_start_block: Some(6),
    };
    let database = MockDatabaseManager::new(plan);
    let manifest_service = UpdateFailManifest::new(manifest);
    let manifest_updates = manifest_service.updates.clone();
    let time_provider = make_time_provider(3);
    let http_client = StubHttpClient::new("settings: true");
    let env = base_env();

    let runtime = SyncRuntime {
        env,
        cwd: cwd.clone(),
        http: Box::new(http_client),
        cli_runner: Box::new(cli_runner.clone()),
        archive: Box::new(archive.clone()),
        database: Box::new(database.clone()),
        manifest: Box::new(manifest_service),
        time: Box::new(time_provider.clone()),
    };

    let err = run_sync_with(runtime, SyncConfig::default()).unwrap_err();
    assert!(err.to_string().contains("manifest update failed"));

    let calls = cli_runner.calls();
    assert_eq!(calls.len(), 1);
    let db_path = cwd.join(format!("data/{manifest_chain}.db"));
    assert!(!db_path.exists());
    assert_eq!(database.finalize_calls().len(), 1);

    let updates = manifest_updates.lock().unwrap();
    assert_eq!(updates.len(), 1);
    assert_eq!(updates[0].0, cwd.join("data/manifest.yaml"));
    assert_eq!(updates[0].1, manifest_chain);
}

#[test]
fn run_sync_with_fails_when_database_prepare_fails() {
    struct PrepareFailDatabase;

    impl DatabaseManager for PrepareFailDatabase {
        fn prepare_database(&self, _db_stem: &str, _db_dir: &Path) -> Result<(PathBuf, PathBuf)> {
            anyhow::bail!("prepare failed");
        }

        fn plan_sync(&self, _db_path: &Path, _dump_path: &Path) -> Result<SyncPlan> {
            unreachable!("plan_sync should not be called");
        }

        fn finalize_database(
            &self,
            _db_stem: &str,
            _db_path: &Path,
            _dump_path: &Path,
        ) -> Result<()> {
            unreachable!("finalize should not be called");
        }
    }

    let temp = tempdir().unwrap();
    let cwd = temp.path().to_path_buf();

    let cli_runner = MockCliRunner::default();
    let archive = MockArchiveService::default();
    let manifest_service = MockManifestService::new(manifest_with_chain(1));
    let time_provider = make_time_provider(2);
    let http_client = StubHttpClient::new("settings: true");
    let env = base_env();

    let runtime = SyncRuntime {
        env,
        cwd,
        http: Box::new(http_client),
        cli_runner: Box::new(cli_runner.clone()),
        archive: Box::new(archive),
        database: Box::new(PrepareFailDatabase),
        manifest: Box::new(manifest_service),
        time: Box::new(time_provider),
    };

    let err = run_sync_with(runtime, SyncConfig::default()).unwrap_err();
    assert!(err.to_string().contains("prepare failed"));
    assert!(cli_runner.calls().is_empty());
}

#[test]
fn run_sync_with_fails_when_database_plan_fails() {
    struct PlanFailDatabase;

    impl DatabaseManager for PlanFailDatabase {
        fn prepare_database(&self, db_stem: &str, db_dir: &Path) -> Result<(PathBuf, PathBuf)> {
            std::fs::create_dir_all(db_dir)?;
            let db_path = db_dir.join(format!("{db_stem}.db"));
            std::fs::write(&db_path, b"db")?;
            let dump_path = db_dir.join(format!("{db_stem}.sql.gz"));
            Ok((db_path, dump_path))
        }

        fn plan_sync(&self, _db_path: &Path, _dump_path: &Path) -> Result<SyncPlan> {
            anyhow::bail!("plan failed");
        }

        fn finalize_database(
            &self,
            _db_stem: &str,
            _db_path: &Path,
            _dump_path: &Path,
        ) -> Result<()> {
            unreachable!("finalize should not be called");
        }
    }

    let temp = tempdir().unwrap();
    let cwd = temp.path().to_path_buf();

    let chain_id = 77u64;

    let cli_runner = MockCliRunner::default();
    let archive = MockArchiveService::default();
    let manifest_service = MockManifestService::new(manifest_with_chain(chain_id));
    let time_provider = make_time_provider(2);
    let http_client = StubHttpClient::new("settings: true");
    let env = base_env();

    let runtime = SyncRuntime {
        env,
        cwd: cwd.clone(),
        http: Box::new(http_client),
        cli_runner: Box::new(cli_runner.clone()),
        archive: Box::new(archive),
        database: Box::new(PlanFailDatabase),
        manifest: Box::new(manifest_service),
        time: Box::new(time_provider),
    };

    let err = run_sync_with(runtime, SyncConfig::default()).unwrap_err();
    assert!(err.to_string().contains("plan failed"));
    assert!(cli_runner.calls().is_empty());
    let db_path = cwd.join(format!("data/{chain_id}.db"));
    assert!(!db_path.exists());
}

#[test]
fn run_sync_with_fails_when_database_finalize_fails() {
    struct FinalizeFailDatabase;

    impl DatabaseManager for FinalizeFailDatabase {
        fn prepare_database(&self, db_stem: &str, db_dir: &Path) -> Result<(PathBuf, PathBuf)> {
            std::fs::create_dir_all(db_dir)?;
            let db_path = db_dir.join(format!("{db_stem}.db"));
            std::fs::write(&db_path, b"db")?;
            let dump_path = db_dir.join(format!("{db_stem}.sql.gz"));
            Ok((db_path, dump_path))
        }

        fn plan_sync(&self, db_path: &Path, dump_path: &Path) -> Result<SyncPlan> {
            Ok(SyncPlan {
                db_path: db_path.to_path_buf(),
                dump_path: dump_path.to_path_buf(),
                last_synced_block: Some(5),
                next_start_block: Some(6),
            })
        }

        fn finalize_database(
            &self,
            _db_stem: &str,
            _db_path: &Path,
            _dump_path: &Path,
        ) -> Result<()> {
            anyhow::bail!("finalize failed");
        }
    }

    let temp = tempdir().unwrap();
    let cwd = temp.path().to_path_buf();

    let chain_id = 55u64;

    let cli_runner = MockCliRunner::default();
    let archive = MockArchiveService::default();
    let manifest_service = MockManifestService::new(manifest_with_chain(chain_id));
    let time_provider = make_time_provider(2);
    let http_client = StubHttpClient::new("settings: true");
    let env = base_env();

    let runtime = SyncRuntime {
        env,
        cwd: cwd.clone(),
        http: Box::new(http_client),
        cli_runner: Box::new(cli_runner.clone()),
        archive: Box::new(archive),
        database: Box::new(FinalizeFailDatabase),
        manifest: Box::new(manifest_service),
        time: Box::new(time_provider),
    };

    let err = run_sync_with(runtime, SyncConfig::default()).unwrap_err();
    assert!(err.to_string().contains("finalize failed"));

    let calls = cli_runner.calls();
    assert_eq!(calls.len(), 1);
    let db_path = cwd.join(format!("data/{chain_id}.db"));
    assert!(!db_path.exists());
}

#[test]
fn run_sync_with_errors_when_cli_binary_url_missing() {
    let temp = tempdir().unwrap();
    let cwd = temp.path().to_path_buf();

    let mut env = base_env();
    env.remove(CLI_BINARY_URL_ENV_VAR);

    let runtime = SyncRuntime {
        env,
        cwd,
        http: Box::new(StubHttpClient::new("settings: true")),
        cli_runner: Box::new(MockCliRunner::default()),
        archive: Box::new(MockArchiveService::default()),
        database: Box::new(MockDatabaseManager::new(SyncPlan {
            db_path: PathBuf::new(),
            dump_path: PathBuf::new(),
            last_synced_block: None,
            next_start_block: None,
        })),
        manifest: Box::new(MockManifestService::new(Manifest::new())),
        time: Box::new(make_time_provider(1)),
    };

    let err = run_sync_with(runtime, SyncConfig::default()).unwrap_err();
    assert!(
        err.to_string()
            .contains(format!("{CLI_BINARY_URL_ENV_VAR} must be set").as_str()),
        "unexpected error: {err}"
    );
}

#[test]
fn run_sync_with_errors_when_settings_yaml_missing() {
    let temp = tempdir().unwrap();
    let cwd = temp.path().to_path_buf();

    let mut env = base_env();
    env.insert(SETTINGS_YAML_ENV_VAR.to_string(), "   ".to_string());

    let runtime = SyncRuntime {
        env,
        cwd,
        http: Box::new(StubHttpClient::new("settings: true")),
        cli_runner: Box::new(MockCliRunner::default()),
        archive: Box::new(MockArchiveService::default()),
        database: Box::new(MockDatabaseManager::new(SyncPlan {
            db_path: PathBuf::new(),
            dump_path: PathBuf::new(),
            last_synced_block: None,
            next_start_block: None,
        })),
        manifest: Box::new(MockManifestService::new(Manifest::new())),
        time: Box::new(make_time_provider(1)),
    };

    let err = run_sync_with(runtime, SyncConfig::default()).unwrap_err();
    assert!(
        err.to_string()
            .contains(format!("{SETTINGS_YAML_ENV_VAR} must be set").as_str()),
        "unexpected error: {err}"
    );
}

#[test]
fn run_sync_with_errors_when_api_token_missing() {
    let temp = tempdir().unwrap();
    let cwd = temp.path().to_path_buf();

    let mut env = base_env();
    env.remove(API_TOKEN_ENV_VARS[0]);

    let runtime = SyncRuntime {
        env,
        cwd,
        http: Box::new(StubHttpClient::new("settings: true")),
        cli_runner: Box::new(MockCliRunner::default()),
        archive: Box::new(MockArchiveService::default()),
        database: Box::new(MockDatabaseManager::new(SyncPlan {
            db_path: PathBuf::new(),
            dump_path: PathBuf::new(),
            last_synced_block: None,
            next_start_block: None,
        })),
        manifest: Box::new(MockManifestService::new(Manifest::new())),
        time: Box::new(make_time_provider(2)),
    };

    let err = run_sync_with(runtime, SyncConfig::default()).unwrap_err();
    assert!(
        err.to_string().contains("Missing API token"),
        "unexpected error: {err}"
    );
}
