use std::collections::HashMap;
use std::fs;
use std::path::{Path, PathBuf};

use anyhow::{Context, Result};
use chrono::Utc;

use crate::archive::{download_cli_archive, extract_cli_binary};
use crate::cli::{run_cli_sync, RunCliSyncOptions};
use crate::constants::{API_TOKEN_ENV_VARS, CLI_ARCHIVE_NAME, DEFAULT_COMMIT_HASH};
use crate::database::{finalize_database, plan_sync, prepare_database};
use crate::http::{DefaultHttpClient, HttpClient};
use crate::logging::log_plan;

const ORDERBOOK_DB_FILE_STEM: &str = "orderbook";
const ORDERBOOK_LABEL: &str = "Arbitrum";

#[derive(Clone, Debug)]
pub struct SyncConfig {
    pub db_dir: PathBuf,
    pub cli_dir: PathBuf,
    pub chain_id: u64,
}

impl Default for SyncConfig {
    fn default() -> Self {
        Self {
            db_dir: PathBuf::from("data"),
            cli_dir: PathBuf::from("bin"),
            chain_id: 42161,
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
        .unwrap_or_else(|| DEFAULT_COMMIT_HASH.to_string());

    if commit_hash.trim().is_empty() {
        anyhow::bail!("COMMIT_HASH must be set to a valid rain.orderbook commit hash");
    }
    println!("Using commit hash {commit_hash}");

    let archive_path = runtime.cwd.join(CLI_ARCHIVE_NAME);
    download_cli_archive(runtime.http.as_ref(), &commit_hash, &archive_path)?;

    let cli_dir = resolve_path(&runtime.cwd, &config.cli_dir);
    let cli_binary = extract_cli_binary(&archive_path, &cli_dir)?;

    let _ = fs::remove_file(&archive_path);

    let api_token = resolve_api_token(&runtime.env)?;
    println!("Using API token sourced from environment.");

    let primary_db_dir = resolve_path(&runtime.cwd, &config.db_dir);
    let fallback_db_dir = if !config.db_dir.is_absolute() {
        runtime
            .cwd
            .parent()
            .map(|parent| parent.join(&config.db_dir))
    } else {
        None
    };

    let mut candidates: Vec<PathBuf> = Vec::new();
    if let Some(fallback) = &fallback_db_dir {
        candidates.push(fallback.clone());
    }
    candidates.push(primary_db_dir.clone());

    let mut selected_dir: Option<PathBuf> = None;

    for candidate in &candidates {
        if candidate.exists() {
            selected_dir = Some(candidate.clone());
            break;
        }
    }

    let db_dir = if let Some(dir) = selected_dir {
        if dir == primary_db_dir {
            fs::create_dir_all(&primary_db_dir).with_context(|| {
                format!(
                    "failed to create database directory {}",
                    primary_db_dir.display()
                )
            })?;
        }
        dir
    } else {
        fs::create_dir_all(&primary_db_dir).with_context(|| {
            format!(
                "failed to create database directory {}",
                primary_db_dir.display()
            )
        })?;
        primary_db_dir
    };

    let (db_path, dump_path) = prepare_database(ORDERBOOK_DB_FILE_STEM, &db_dir)?;
    let result = (|| -> Result<()> {
        let plan = plan_sync(&db_path, &dump_path)?;
        let plan_label = format!("chain {}", config.chain_id);
        log_plan(&plan_label, &plan);

        run_cli_sync(&RunCliSyncOptions {
            cli_binary: cli_binary.display().to_string(),
            db_path: db_path.display().to_string(),
            chain_id: config.chain_id,
            api_token: Some(api_token.clone()),
            repo_commit: commit_hash.clone(),
            start_block: plan.next_start_block,
            end_block: None,
        })?;

        finalize_database(ORDERBOOK_DB_FILE_STEM, &db_path, &dump_path)?;
        Ok(())
    })();

    if let Err(error) = &result {
        eprintln!(
            "Sync failed for chain {} ({}): {error:?}",
            config.chain_id, ORDERBOOK_LABEL
        );
    }

    if db_path.exists() {
        let _ = fs::remove_file(&db_path);
    }

    result?;

    let end_time = Utc::now();
    let duration = end_time - start_time;
    let elapsed_seconds = duration.num_milliseconds() as f64 / 1000.0;
    println!(
        "Sync completed at {} (duration: {:.1}s)",
        end_time.to_rfc3339(),
        elapsed_seconds
    );

    Ok(())
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

    #[test]
    fn resolve_api_token_uses_first_non_empty_value() {
        let mut env = HashMap::new();
        env.insert(API_TOKEN_ENV_VARS[1].to_string(), "".to_string());
        env.insert(API_TOKEN_ENV_VARS[2].to_string(), " token ".to_string());

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
}
