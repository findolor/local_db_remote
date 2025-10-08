use std::path::Path;
use std::process::Command;

use anyhow::{Context, Result};

use crate::constants::API_TOKEN_ENV_VARS;

#[derive(Debug, Clone)]
pub struct RunCliSyncOptions {
    pub cli_binary: String,
    pub db_path: String,
    pub chain_id: u64,
    pub api_token: Option<String>,
    pub repo_commit: String,
    pub start_block: Option<u64>,
    pub end_block: Option<u64>,
}

pub fn run_cli_sync(options: &RunCliSyncOptions) -> Result<()> {
    let db_parent = Path::new(&options.db_path)
        .parent()
        .map(|p| p.to_path_buf())
        .unwrap_or_else(|| Path::new(".").to_path_buf());
    std::fs::create_dir_all(&db_parent).with_context(|| {
        format!(
            "failed to create database directory {}",
            db_parent.display()
        )
    })?;

    let api_token = options.api_token.clone().ok_or_else(|| {
        anyhow::anyhow!(
            "no API token provided for chain {}. Set one of: {}",
            options.chain_id,
            API_TOKEN_ENV_VARS.join(", ")
        )
    })?;

    let mut args = vec![
        "local-db".to_string(),
        "sync".to_string(),
        "--db-path".to_string(),
        options.db_path.clone(),
        "--chain-id".to_string(),
        options.chain_id.to_string(),
        "--repo-commit".to_string(),
        options.repo_commit.clone(),
        "--api-token".to_string(),
        api_token,
    ];

    if let Some(start) = options.start_block {
        args.push("--start-block".to_string());
        args.push(start.to_string());
    }

    if let Some(end) = options.end_block {
        args.push("--end-block".to_string());
        args.push(end.to_string());
    }

    let mut log_args = args.clone();
    if let Some(index) = log_args.iter().position(|arg| arg == "--api-token") {
        if let Some(value) = log_args.get_mut(index + 1) {
            *value = "***".to_string();
        }
    }

    println!("Running: {} {}", options.cli_binary, log_args.join(" "));

    let status = Command::new(&options.cli_binary)
        .args(&args)
        .status()
        .with_context(|| "failed to spawn rain-orderbook-cli")?;

    if !status.success() {
        anyhow::bail!(
            "CLI sync failed for chain {} (exit code {:?})",
            options.chain_id,
            status.code()
        );
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    #[test]
    fn run_cli_sync_requires_api_token() {
        let temp = tempdir().unwrap();
        let options = RunCliSyncOptions {
            cli_binary: temp.path().join("cli").display().to_string(),
            db_path: temp.path().join("db/test.db").display().to_string(),
            chain_id: 1,
            api_token: None,
            repo_commit: "commit".to_string(),
            start_block: None,
            end_block: None,
        };

        let err = run_cli_sync(&options).unwrap_err();
        assert!(err.to_string().contains("API token"));
    }

    #[cfg(unix)]
    #[test]
    fn run_cli_sync_invokes_cli_with_expected_arguments() {
        use std::os::unix::fs::PermissionsExt;

        let temp = tempdir().unwrap();
        let cli_path = temp.path().join("rain-orderbook-cli");
        let args_path = temp.path().join("args.txt");

        std::fs::write(
            &cli_path,
            format!("#!/bin/sh\necho \"$@\" > {}\n", args_path.display()),
        )
        .unwrap();
        let mut perms = std::fs::metadata(&cli_path).unwrap().permissions();
        perms.set_mode(0o755);
        std::fs::set_permissions(&cli_path, perms).unwrap();

        let db_path = temp.path().join("nested/db.sqlite");
        let options = RunCliSyncOptions {
            cli_binary: cli_path.display().to_string(),
            db_path: db_path.display().to_string(),
            chain_id: 42161,
            api_token: Some("token".to_string()),
            repo_commit: "hash".to_string(),
            start_block: Some(100),
            end_block: Some(200),
        };

        run_cli_sync(&options).unwrap();

        let captured = std::fs::read_to_string(&args_path).unwrap();
        assert!(captured.contains("local-db sync"));
        assert!(captured.contains("--db-path"));
        assert!(captured.contains(db_path.to_str().unwrap()));
        assert!(captured.contains("--start-block 100"));
        assert!(captured.contains("--end-block 200"));
    }
}
