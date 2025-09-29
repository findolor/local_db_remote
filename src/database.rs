use std::fs;
use std::io;
use std::path::{Path, PathBuf};
use std::process::Command;
use std::sync::atomic::{AtomicBool, Ordering};

use anyhow::{Context, Result};

#[derive(Debug, Clone)]
pub struct SyncPlan {
    pub db_path: PathBuf,
    pub dump_path: PathBuf,
    pub last_synced_block: Option<u64>,
    pub next_start_block: Option<u64>,
}

pub fn prepare_database(db_stem: &str, db_dir: &Path) -> Result<(PathBuf, PathBuf)> {
    let db_path = db_dir.join(format!("{db_stem}.db"));
    let dump_path = db_dir.join(format!("{db_stem}.db.tar.gz"));

    fs::create_dir_all(db_dir)
        .with_context(|| format!("failed to create database directory {}", db_dir.display()))?;

    if path_exists(&db_path)? {
        fs::remove_file(&db_path)
            .with_context(|| format!("failed to remove existing db {}", db_path.display()))?;
    }

    if path_exists(&dump_path)? {
        println!(
            "Extracting dump for {} from {}",
            db_stem,
            dump_path.display()
        );
        let status = Command::new("tar")
            .arg("-xzf")
            .arg(&dump_path)
            .arg("-C")
            .arg(db_dir)
            .status()
            .with_context(|| "failed to spawn tar for dump extraction")?;
        if !status.success() {
            anyhow::bail!(
                "failed to extract dump for {} (exit code {:?})",
                db_stem,
                status.code()
            );
        }
    } else {
        println!(
            "No existing dump for {}; CLI will initialize a new database.",
            db_stem
        );
    }

    Ok((db_path, dump_path))
}

pub fn finalize_database(db_stem: &str, db_path: &Path, dump_path: &Path) -> Result<()> {
    if !path_exists(db_path)? {
        println!(
            "No database file produced for {}; skipping archive.",
            db_stem
        );
        return Ok(());
    }

    let temp_dump_path = dump_path.with_extension("db.tar.gz.tmp");
    println!(
        "Archiving database for {} to {}",
        db_stem,
        dump_path.display()
    );
    let status = Command::new("tar")
        .arg("-czf")
        .arg(&temp_dump_path)
        .arg("-C")
        .arg(
            db_path
                .parent()
                .ok_or_else(|| anyhow::anyhow!("database path has no parent"))?,
        )
        .arg(
            db_path
                .file_name()
                .ok_or_else(|| anyhow::anyhow!("database path has no filename"))?,
        )
        .status()
        .with_context(|| "failed to spawn tar for archiving")?;

    if !status.success() {
        if path_exists(&temp_dump_path)? {
            let _ = fs::remove_file(&temp_dump_path);
        }
        anyhow::bail!(
            "failed to archive database for {} (exit code {:?})",
            db_stem,
            status.code()
        );
    }

    if path_exists(dump_path)? {
        fs::remove_file(dump_path)
            .with_context(|| format!("failed to remove old dump {}", dump_path.display()))?;
    }
    fs::rename(&temp_dump_path, dump_path).with_context(|| {
        format!(
            "failed to move archive {} to {}",
            temp_dump_path.display(),
            dump_path.display()
        )
    })?;
    fs::remove_file(db_path)
        .with_context(|| format!("failed to remove working db {}", db_path.display()))?;
    Ok(())
}

pub fn plan_sync(db_path: &Path, dump_path: &Path) -> Result<SyncPlan> {
    let last_synced_block = get_last_synced_block(db_path)?;
    let next_start_block = last_synced_block.map(|value| value + 1);

    Ok(SyncPlan {
        db_path: db_path.to_path_buf(),
        dump_path: dump_path.to_path_buf(),
        last_synced_block,
        next_start_block,
    })
}

fn get_last_synced_block(db_path: &Path) -> Result<Option<u64>> {
    if !path_exists(db_path)? {
        return Ok(None);
    }

    let table_output = Command::new("sqlite3")
        .arg("-readonly")
        .arg(db_path)
        .arg("SELECT 1 FROM sqlite_master WHERE type='table' AND name='sync_status' LIMIT 1;")
        .output();

    warn_if_sqlite_missing(&table_output);
    let table_output = match table_output {
        Ok(output) => output,
        Err(_) => return Ok(None),
    };

    let has_table = table_output.status.success()
        && !String::from_utf8_lossy(&table_output.stdout)
            .trim()
            .is_empty();
    if !has_table {
        return Ok(None);
    }

    let pragma_output = Command::new("sqlite3")
        .arg("-readonly")
        .arg("-separator")
        .arg("|")
        .arg(db_path)
        .arg("PRAGMA table_info('sync_status');")
        .output();

    warn_if_sqlite_missing(&pragma_output);
    let pragma_output = match pragma_output {
        Ok(output) => output,
        Err(_) => return Ok(None),
    };

    if !pragma_output.status.success() {
        return Ok(None);
    }

    let stdout = String::from_utf8_lossy(&pragma_output.stdout);
    let column_name = stdout
        .lines()
        .filter_map(|line| {
            let mut parts = line.split('|');
            let _id = parts.next()?;
            let name = parts.next()?;
            Some(name.to_string())
        })
        .find(|name| name.to_lowercase().contains("block"));

    let Some(column_name) = column_name else {
        return Ok(None);
    };

    let query = format!(
        "SELECT {} FROM sync_status ORDER BY {} DESC LIMIT 1;",
        quote_identifier(&column_name),
        quote_identifier(&column_name)
    );

    let query_output = Command::new("sqlite3")
        .arg("-readonly")
        .arg(db_path)
        .arg(query)
        .output();

    warn_if_sqlite_missing(&query_output);
    let query_output = match query_output {
        Ok(output) => output,
        Err(_) => return Ok(None),
    };

    if !query_output.status.success() {
        return Ok(None);
    }

    let value_str = String::from_utf8_lossy(&query_output.stdout)
        .trim()
        .to_string();
    let value = value_str.parse::<u64>().ok();
    Ok(value)
}

fn quote_identifier(identifier: &str) -> String {
    format!("\"{}\"", identifier.replace('"', "\"\""))
}

fn path_exists(path: &Path) -> Result<bool> {
    Ok(fs::metadata(path).is_ok())
}

static SQLITE_WARNING_EMITTED: AtomicBool = AtomicBool::new(false);

fn warn_if_sqlite_missing(result: &Result<std::process::Output, io::Error>) {
    if SQLITE_WARNING_EMITTED.load(Ordering::Relaxed) {
        return;
    }

    if let Err(error) = result {
        if error.kind() == io::ErrorKind::NotFound {
            println!("⚠️  sqlite3 CLI not found; skipping local sync-status inspection.");
            SQLITE_WARNING_EMITTED.store(true, Ordering::Relaxed);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::{Mutex, OnceLock};
    use tempfile::{tempdir, NamedTempFile};

    fn path_mutex() -> &'static Mutex<()> {
        static LOCK: OnceLock<Mutex<()>> = OnceLock::new();
        LOCK.get_or_init(|| Mutex::new(()))
    }

    #[test]
    fn prepare_database_initializes_paths_without_dump() {
        let dir = tempdir().unwrap();
        let (db, dump) = prepare_database("orderbook", dir.path()).unwrap();

        assert_eq!(db, dir.path().join("orderbook.db"));
        assert_eq!(dump, dir.path().join("orderbook.db.tar.gz"));
        assert!(!db.exists());
    }

    #[test]
    fn prepare_database_extracts_existing_dump() {
        let dir = tempdir().unwrap();
        let dump_path = dir.path().join("orderbook.db.tar.gz");

        let staging = tempdir().unwrap();
        let db_file = staging.path().join("orderbook.db");
        std::fs::write(&db_file, b"contents").unwrap();
        let status = Command::new("tar")
            .arg("-czf")
            .arg(&dump_path)
            .arg("-C")
            .arg(staging.path())
            .arg(".")
            .status()
            .unwrap();
        assert!(status.success());

        let (db_path, _) = prepare_database("orderbook", dir.path()).unwrap();
        assert!(db_path.exists());
        let restored = std::fs::read(&db_path).unwrap();
        assert_eq!(restored, b"contents");
    }

    #[test]
    fn finalize_database_archives_and_cleans_up() {
        let dir = tempdir().unwrap();
        let db_path = dir.path().join("orderbook.db");
        std::fs::write(&db_path, b"data").unwrap();
        let dump_path = dir.path().join("orderbook.db.tar.gz");
        std::fs::write(&dump_path, b"old").unwrap();

        finalize_database("orderbook", &db_path, &dump_path).unwrap();

        assert!(!db_path.exists());
        assert!(dump_path.exists());
        let status = Command::new("tar")
            .arg("-tzf")
            .arg(&dump_path)
            .status()
            .unwrap();
        assert!(status.success());
    }

    #[test]
    fn finalize_database_skips_when_db_missing() {
        let dir = tempdir().unwrap();
        let db_path = dir.path().join("missing.db");
        let dump_path = dir.path().join("missing.db.tar.gz");

        finalize_database("missing", &db_path, &dump_path).unwrap();
        assert!(!dump_path.exists());
    }

    #[test]
    fn plan_sync_without_existing_db_reports_none() {
        let dir = tempdir().unwrap();
        let db_path = dir.path().join("orderbook.db");
        let dump_path = dir.path().join("orderbook.db.tar.gz");

        let plan = plan_sync(&db_path, &dump_path).unwrap();
        assert!(plan.last_synced_block.is_none());
        assert!(plan.next_start_block.is_none());
    }

    #[cfg(unix)]
    #[test]
    fn plan_sync_reads_last_synced_block_using_sqlite_cli() {
        use std::os::unix::fs::PermissionsExt;

        let _guard = path_mutex().lock().unwrap();
        let dir = tempdir().unwrap();
        let db_path = dir.path().join("orderbook.db");
        std::fs::write(&db_path, b"db").unwrap();
        let dump_path = dir.path().join("orderbook.db.tar.gz");

        let bin_dir = tempdir().unwrap();
        let sqlite_bin = bin_dir.path().join("sqlite3");
        let log_path = bin_dir.path().join("sqlite.log");
        std::fs::write(
            &sqlite_bin,
            format!(
                r#"#!/bin/sh
echo "$@" >> "{log}"
if [ "$3" = "SELECT 1 FROM sqlite_master WHERE type='table' AND name='sync_status' LIMIT 1;" ]; then
  echo 1
  exit 0
fi
if [ "$5" = "PRAGMA table_info('sync_status');" ]; then
  echo '0|id|INTEGER'
  echo '1|last_block|INTEGER'
  exit 0
fi
if [ "$3" = "SELECT \"last_block\" FROM sync_status ORDER BY \"last_block\" DESC LIMIT 1;" ]; then
  echo 123
  exit 0
fi
exit 1
"#,
                log = log_path.display()
            ),
        )
        .unwrap();
        let mut perms = std::fs::metadata(&sqlite_bin).unwrap().permissions();
        perms.set_mode(0o755);
        std::fs::set_permissions(&sqlite_bin, perms).unwrap();

        let original_path = std::env::var_os("PATH");
        let new_path = match original_path.as_ref() {
            Some(value) => {
                let mut combined = bin_dir.path().as_os_str().to_os_string();
                combined.push(":");
                combined.push(value);
                combined
            }
            None => bin_dir.path().as_os_str().to_os_string(),
        };
        std::env::set_var("PATH", &new_path);

        let plan = plan_sync(&db_path, &dump_path).unwrap();
        assert_eq!(plan.last_synced_block, Some(123));
        assert_eq!(plan.next_start_block, Some(124));

        match original_path {
            Some(value) => std::env::set_var("PATH", value),
            None => std::env::remove_var("PATH"),
        }

        let logged = std::fs::read_to_string(&log_path).unwrap();
        assert!(logged.contains("sqlite_master"));
        assert!(logged.contains("PRAGMA table_info"));
        assert!(logged.contains("last_block"));
    }

    #[test]
    fn quote_identifier_escapes_quotes() {
        let quoted = quote_identifier("col\"name");
        assert_eq!(quoted, "\"col\"\"name\"");
    }

    #[test]
    fn path_exists_reports_status() {
        let file = NamedTempFile::new().unwrap();
        assert!(path_exists(file.path()).unwrap());

        let missing = file.path().with_file_name("missing");
        assert!(!path_exists(&missing).unwrap());
    }

    #[test]
    fn warn_if_sqlite_missing_sets_warning_flag() {
        use std::io;

        SQLITE_WARNING_EMITTED.store(false, Ordering::Relaxed);
        let err: Result<std::process::Output, io::Error> =
            Err(io::Error::new(io::ErrorKind::NotFound, "missing"));
        warn_if_sqlite_missing(&err);
        assert!(SQLITE_WARNING_EMITTED.load(Ordering::Relaxed));

        warn_if_sqlite_missing(&err);
        assert!(SQLITE_WARNING_EMITTED.load(Ordering::Relaxed));
    }
}
