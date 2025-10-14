use std::collections::BTreeSet;
use std::fs;
use std::path::{Path, PathBuf};

use anyhow::{Context, Result};

use crate::cli::RunCliSyncOptions;
use crate::constants::{
    API_TOKEN_ENV_VARS, CLI_ARCHIVE_NAME, CLI_BINARY_URL_ENV_VAR, RELEASE_DOWNLOAD_URL_TEMPLATE,
    SETTINGS_YAML_ENV_VAR, SYNC_CHAIN_IDS_ENV_VAR,
};
use crate::logging::log_plan;

use super::runtime::{SyncConfig, SyncRuntime};

pub fn run_sync() -> Result<()> {
    run_sync_with(SyncRuntime::default(), SyncConfig::default())
}

pub fn run_sync_with(runtime: SyncRuntime, config: SyncConfig) -> Result<()> {
    let start_time = runtime.time.now();
    println!("Sync started at {}", start_time.to_rfc3339());

    let cli_binary_url = runtime
        .env
        .get(CLI_BINARY_URL_ENV_VAR)
        .map(|value| value.trim().to_string())
        .filter(|value| !value.is_empty())
        .ok_or_else(|| {
            anyhow::anyhow!("{CLI_BINARY_URL_ENV_VAR} must be set to a valid CLI binary URL")
        })?;
    println!("Using CLI binary at {cli_binary_url}");

    let settings_yaml = resolve_settings_yaml(&runtime.env, runtime.http.as_ref())?;

    let archive_path = runtime.cwd.join(CLI_ARCHIVE_NAME);
    runtime
        .archive
        .download_archive(runtime.http.as_ref(), &cli_binary_url, &archive_path)?;

    let cli_dir = resolve_path(&runtime.cwd, &config.cli_dir);
    let cli_binary = runtime.archive.extract_binary(&archive_path, &cli_dir)?;

    if let Err(error) = fs::remove_file(&archive_path) {
        eprintln!(
            "Failed to remove CLI archive {}: {error}",
            archive_path.display()
        );
    }

    let api_token = resolve_api_token(&runtime.env)?;
    println!("Using API token sourced from environment.");

    let db_dir = resolve_path(&runtime.cwd, &config.db_dir);
    fs::create_dir_all(&db_dir)
        .with_context(|| format!("failed to create database directory {}", db_dir.display()))?;

    let manifest_path = db_dir.join("manifest.yaml");
    let manifest = runtime
        .manifest
        .download_manifest(runtime.http.as_ref(), &manifest_path)
        .with_context(|| format!("failed to download manifest to {}", manifest_path.display()))?;
    runtime
        .manifest
        .download_dumps(runtime.http.as_ref(), &manifest, &db_dir)
        .with_context(|| format!("failed to hydrate dumps into {}", db_dir.display()))?;

    let mut chain_ids: BTreeSet<u64> = manifest
        .networks
        .keys()
        .map(|network| u64::from(*network))
        .collect();
    for chain_id in parse_chain_ids_from_env(&runtime.env)? {
        chain_ids.insert(chain_id);
    }
    for chain_id in &config.chain_ids {
        chain_ids.insert(*chain_id);
    }
    for chain_id in chain_ids {
        sync_single_chain(
            &runtime,
            chain_id,
            &cli_binary,
            &api_token,
            &settings_yaml,
            &db_dir,
            &manifest_path,
        )?;
    }

    let completion_time = runtime.time.now();
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
    runtime: &SyncRuntime,
    chain_id: u64,
    cli_binary: &Path,
    api_token: &str,
    settings_yaml: &str,
    db_dir: &Path,
    manifest_path: &Path,
) -> Result<()> {
    println!("Starting sync for chain {chain_id}");
    let chain_start = runtime.time.now();

    let file_stem = chain_id.to_string();
    let (db_path, dump_path) = runtime.database.prepare_database(&file_stem, db_dir)?;
    let result = (|| -> Result<()> {
        let plan = runtime.database.plan_sync(&db_path, &dump_path)?;
        let plan_label = format!("chain {}", chain_id);
        log_plan(&plan_label, &plan);

        runtime.cli_runner.run(&RunCliSyncOptions {
            cli_binary: cli_binary.display().to_string(),
            db_path: db_path.display().to_string(),
            chain_id,
            api_token: Some(api_token.to_string()),
            settings_yaml: settings_yaml.to_string(),
            start_block: plan.next_start_block,
            end_block: None,
        })?;

        runtime
            .database
            .finalize_database(&file_stem, &db_path, &dump_path)?;
        Ok(())
    })();

    if let Err(error) = &result {
        eprintln!("Sync failed for chain {}: {error:?}", chain_id);
    }

    if db_path.exists() {
        let _ = fs::remove_file(&db_path);
    }

    result?;

    let completion_time = runtime.time.now();
    let dump_file_name = dump_path
        .file_name()
        .and_then(|name| name.to_str())
        .ok_or_else(|| anyhow::anyhow!("dump path is missing a valid filename"))?;
    let download_url = RELEASE_DOWNLOAD_URL_TEMPLATE.replace("{file}", dump_file_name);
    runtime
        .manifest
        .update_manifest(manifest_path, chain_id, &download_url, completion_time)?;
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

fn resolve_api_token(env: &std::collections::HashMap<String, String>) -> Result<String> {
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

fn resolve_settings_yaml(
    env: &std::collections::HashMap<String, String>,
    http: &dyn crate::http::HttpClient,
) -> Result<String> {
    let url = env
        .get(SETTINGS_YAML_ENV_VAR)
        .map(|value| value.trim())
        .filter(|value| !value.is_empty())
        .ok_or_else(|| {
            anyhow::anyhow!("{SETTINGS_YAML_ENV_VAR} must be set to a valid settings YAML URL")
        })?;
    println!("Fetching settings YAML from {url}");
    http.fetch_text(url)
        .with_context(|| format!("failed to download settings YAML from {}", url))
}

fn resolve_path(base: &Path, configured: &Path) -> PathBuf {
    if configured.is_absolute() {
        configured.to_path_buf()
    } else {
        base.join(configured)
    }
}

fn parse_chain_ids_from_env(env: &std::collections::HashMap<String, String>) -> Result<Vec<u64>> {
    let Some(raw) = env.get(SYNC_CHAIN_IDS_ENV_VAR) else {
        return Ok(Vec::new());
    };

    let mut chain_ids = Vec::new();
    for token in raw.split(',') {
        let trimmed = token.trim();
        if trimmed.is_empty() {
            continue;
        }
        let chain_id = trimmed.parse::<u64>().with_context(|| {
            format!(
                "{} must contain comma-separated u64 values (invalid value: `{}`)",
                SYNC_CHAIN_IDS_ENV_VAR, trimmed
            )
        })?;
        chain_ids.push(chain_id);
    }

    Ok(chain_ids)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::http::HttpClient;
    use anyhow::anyhow;
    use std::collections::HashMap;
    use std::sync::Mutex;

    #[test]
    fn resolve_api_token_returns_trimmed_value() {
        let mut env = HashMap::new();
        env.insert(API_TOKEN_ENV_VARS[0].to_string(), "  token  ".to_string());

        let token = resolve_api_token(&env).expect("token should be returned");
        assert_eq!(token, "token");
    }

    #[test]
    fn resolve_api_token_errors_when_missing() {
        let env = HashMap::new();
        let err = resolve_api_token(&env).unwrap_err();
        assert!(
            err.to_string().contains("Missing API token"),
            "unexpected error message: {err}"
        );
    }

    struct RecordingHttpClient {
        response: String,
        requests: Mutex<Vec<String>>,
    }

    impl RecordingHttpClient {
        fn new(response: &str) -> Self {
            Self {
                response: response.to_string(),
                requests: Mutex::new(Vec::new()),
            }
        }

        fn requests(&self) -> Vec<String> {
            self.requests.lock().unwrap().clone()
        }
    }

    impl HttpClient for RecordingHttpClient {
        fn fetch_text(&self, url: &str) -> Result<String> {
            self.requests.lock().unwrap().push(url.to_string());
            Ok(self.response.clone())
        }

        fn fetch_binary(&self, _url: &str) -> Result<Vec<u8>> {
            Err(anyhow!("unexpected binary request"))
        }
    }

    #[test]
    fn resolve_settings_yaml_fetches_remote_document() {
        let mut env = HashMap::new();
        env.insert(
            SETTINGS_YAML_ENV_VAR.to_string(),
            " https://example.com/settings.yaml ".to_string(),
        );
        let http = RecordingHttpClient::new("settings: true");

        let yaml = resolve_settings_yaml(&env, &http).expect("settings yaml should load");
        assert_eq!(yaml, "settings: true");
        assert_eq!(
            http.requests(),
            vec!["https://example.com/settings.yaml".to_string()]
        );
    }

    #[test]
    fn resolve_settings_yaml_errors_when_env_missing() {
        let env = HashMap::new();
        let http = RecordingHttpClient::new("ignored");

        let err = resolve_settings_yaml(&env, &http).unwrap_err();
        assert!(
            err.to_string()
                .contains(format!("{SETTINGS_YAML_ENV_VAR} must be set").as_str()),
            "unexpected error: {err}"
        );
    }

    #[test]
    fn resolve_path_joins_relative_segments() {
        let base = Path::new("/data");
        let configured = Path::new("nested/output");
        let resolved = resolve_path(base, configured);
        assert_eq!(resolved, PathBuf::from("/data/nested/output"));
    }

    #[test]
    fn resolve_path_returns_absolute_input() {
        let base = Path::new("/data");
        let configured = Path::new("/tmp/absolute");
        let resolved = resolve_path(base, configured);
        assert_eq!(resolved, PathBuf::from("/tmp/absolute"));
    }

    #[test]
    fn parse_chain_ids_from_env_returns_parsed_values() {
        let mut env = HashMap::new();
        env.insert(
            SYNC_CHAIN_IDS_ENV_VAR.to_string(),
            " 10,20 , , 30 ".to_string(),
        );

        let ids = super::parse_chain_ids_from_env(&env).expect("ids should parse");
        assert_eq!(ids, vec![10, 20, 30]);
    }

    #[test]
    fn parse_chain_ids_from_env_errors_on_invalid_value() {
        let mut env = HashMap::new();
        env.insert(
            SYNC_CHAIN_IDS_ENV_VAR.to_string(),
            "10,not-a-number".to_string(),
        );

        let err = super::parse_chain_ids_from_env(&env).unwrap_err();
        assert!(
            err.to_string().contains("invalid value: `not-a-number`"),
            "unexpected error: {err}"
        );
    }
}
