use std::fs;
use std::io;
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};
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
    let dump_path = db_dir.join(format!("{db_stem}.sql.gz"));

    fs::create_dir_all(db_dir)
        .with_context(|| format!("failed to create database directory {}", db_dir.display()))?;

    let staging_sql_path = db_dir.join(format!("{db_stem}.sql"));
    if path_exists(&staging_sql_path)? {
        fs::remove_file(&staging_sql_path).with_context(|| {
            format!(
                "failed to remove stale sql dump {}",
                staging_sql_path.display()
            )
        })?;
    }

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
        let staging_file = fs::File::create(&staging_sql_path).with_context(|| {
            format!(
                "failed to create staging sql dump {}",
                staging_sql_path.display()
            )
        })?;
        let status = Command::new("gzip")
            .arg("-dc")
            .arg(&dump_path)
            .stdout(Stdio::from(staging_file))
            .status()
            .with_context(|| {
                format!("failed to spawn gzip to decompress {}", dump_path.display())
            })?;

        if !status.success() {
            let _ = fs::remove_file(&staging_sql_path);
            anyhow::bail!(
                "failed to decompress sql dump for {} (exit code {:?})",
                db_stem,
                status.code()
            );
        }

        if let Err(error) = load_sql_dump(&staging_sql_path, &db_path, db_stem) {
            let _ = fs::remove_file(&staging_sql_path);
            return Err(error);
        }

        fs::remove_file(&staging_sql_path).with_context(|| {
            format!(
                "failed to remove extracted sql dump {}",
                staging_sql_path.display()
            )
        })?;
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

    let sql_path = db_path.with_extension("sql");
    export_sql_dump(db_path, &sql_path, db_stem)?;

    let temp_dump_path = temporary_dump_path(dump_path)?;
    println!(
        "Archiving database for {} to {}",
        db_stem,
        dump_path.display()
    );
    let compressed_file = fs::File::create(&temp_dump_path).with_context(|| {
        format!(
            "failed to create compressed dump {}",
            temp_dump_path.display()
        )
    })?;
    let status = Command::new("gzip")
        .arg("-c")
        .arg(&sql_path)
        .stdout(Stdio::from(compressed_file))
        .status()
        .with_context(|| format!("failed to spawn gzip to compress {}", db_stem))?;

    if !status.success() {
        let _ = fs::remove_file(&temp_dump_path);
        anyhow::bail!(
            "failed to compress sql dump for {} (exit code {:?})",
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
    fs::remove_file(&sql_path)
        .with_context(|| format!("failed to remove sql dump {}", sql_path.display()))?;
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

fn load_sql_dump(sql_path: &Path, db_path: &Path, db_stem: &str) -> Result<()> {
    let sql_file = fs::File::open(sql_path).with_context(|| {
        format!(
            "failed to open sql dump {} while preparing {}",
            sql_path.display(),
            db_stem
        )
    })?;

    let status = Command::new("sqlite3")
        .arg(db_path)
        .stdin(Stdio::from(sql_file))
        .status()
        .with_context(|| format!("failed to spawn sqlite3 to import {db_stem}"))?;

    if !status.success() {
        let _ = fs::remove_file(db_path);
        anyhow::bail!(
            "sqlite3 import for {} failed with exit code {:?}",
            db_stem,
            status.code()
        );
    }

    Ok(())
}

fn export_sql_dump(db_path: &Path, sql_path: &Path, db_stem: &str) -> Result<()> {
    if path_exists(sql_path)? {
        fs::remove_file(sql_path)
            .with_context(|| format!("failed to remove stale sql dump {}", sql_path.display()))?;
    }

    let sql_file = fs::File::create(sql_path).with_context(|| {
        format!(
            "failed to create sql dump {} for {}",
            sql_path.display(),
            db_stem
        )
    })?;

    let status = Command::new("sqlite3")
        .arg(db_path)
        .arg(".dump")
        .stdout(Stdio::from(sql_file))
        .status()
        .with_context(|| format!("failed to spawn sqlite3 to export {db_stem}"))?;

    if !status.success() {
        let _ = fs::remove_file(sql_path);
        anyhow::bail!(
            "sqlite3 export for {} failed with exit code {:?}",
            db_stem,
            status.code()
        );
    }

    Ok(())
}

fn temporary_dump_path(dump_path: &Path) -> Result<PathBuf> {
    let file_name = dump_path
        .file_name()
        .and_then(|name| name.to_str())
        .ok_or_else(|| anyhow::anyhow!("dump path has no filename"))?;
    Ok(dump_path.with_file_name(format!("{file_name}.tmp")))
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
        assert_eq!(dump, dir.path().join("orderbook.sql.gz"));
        assert!(!db.exists());
    }

    #[test]
    fn prepare_database_extracts_existing_dump() {
        let dir = tempdir().unwrap();
        let dump_path = dir.path().join("orderbook.sql.gz");

        let sql_contents = b"CREATE TABLE stub;\n";
        let staging = tempdir().unwrap();
        let sql_path = staging.path().join("orderbook.sql");
        std::fs::write(&sql_path, sql_contents).unwrap();
        let output = Command::new("gzip")
            .arg("-c")
            .arg(&sql_path)
            .output()
            .unwrap();
        assert!(output.status.success());
        std::fs::write(&dump_path, &output.stdout).unwrap();

        let _guard = path_mutex().lock().unwrap();
        let bin_dir = tempdir().unwrap();
        let sqlite_bin = bin_dir.path().join("sqlite3");
        std::fs::write(
            &sqlite_bin,
            r#"#!/bin/sh
if [ "$2" = ".dump" ]; then
  if [ -n "$SQLITE_STUB_DUMP_PATH" ]; then
    cat "$SQLITE_STUB_DUMP_PATH"
  else
    echo "-- stub dump"
  fi
  exit 0
fi
cat > "$1"
"#,
        )
        .unwrap();
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            let mut perms = std::fs::metadata(&sqlite_bin).unwrap().permissions();
            perms.set_mode(0o755);
            std::fs::set_permissions(&sqlite_bin, perms).unwrap();
        }

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

        let (db_path, _) = prepare_database("orderbook", dir.path()).unwrap();

        match original_path {
            Some(value) => std::env::set_var("PATH", value),
            None => std::env::remove_var("PATH"),
        }

        assert!(db_path.exists());
        let restored = std::fs::read(&db_path).unwrap();
        assert_eq!(restored, sql_contents);
    }

    #[test]
    fn finalize_database_archives_and_cleans_up() {
        let dir = tempdir().unwrap();
        let db_path = dir.path().join("orderbook.db");
        std::fs::write(&db_path, b"data").unwrap();
        let dump_path = dir.path().join("orderbook.sql.gz");
        std::fs::write(&dump_path, b"old").unwrap();

        let _guard = path_mutex().lock().unwrap();
        let bin_dir = tempdir().unwrap();
        let sqlite_bin = bin_dir.path().join("sqlite3");
        let dump_contents = dir.path().join("dump.sql");
        std::fs::write(&dump_contents, b"-- exported\n").unwrap();
        std::fs::write(
            &sqlite_bin,
            r#"#!/bin/sh
if [ "$2" = ".dump" ]; then
  if [ -n "$SQLITE_STUB_DUMP_PATH" ]; then
    cat "$SQLITE_STUB_DUMP_PATH"
  else
    echo "-- stub dump"
  fi
  exit 0
fi
cat > "$1"
"#,
        )
        .unwrap();
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            let mut perms = std::fs::metadata(&sqlite_bin).unwrap().permissions();
            perms.set_mode(0o755);
            std::fs::set_permissions(&sqlite_bin, perms).unwrap();
        }

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
        std::env::set_var("SQLITE_STUB_DUMP_PATH", &dump_contents);

        finalize_database("orderbook", &db_path, &dump_path).unwrap();

        match original_path {
            Some(value) => std::env::set_var("PATH", value),
            None => std::env::remove_var("PATH"),
        }
        std::env::remove_var("SQLITE_STUB_DUMP_PATH");

        assert!(!db_path.exists());
        assert!(dump_path.exists());
        let output = Command::new("gzip")
            .arg("-dc")
            .arg(&dump_path)
            .output()
            .unwrap();
        assert!(output.status.success());
        assert_eq!(output.stdout, b"-- exported\n");
        assert!(!db_path.exists());
    }

    #[test]
    fn finalize_database_skips_when_db_missing() {
        let dir = tempdir().unwrap();
        let db_path = dir.path().join("missing.db");
        let dump_path = dir.path().join("missing.sql.gz");

        finalize_database("missing", &db_path, &dump_path).unwrap();
        assert!(!dump_path.exists());
    }

    #[test]
    fn plan_sync_without_existing_db_reports_none() {
        let dir = tempdir().unwrap();
        let db_path = dir.path().join("orderbook.db");
        let dump_path = dir.path().join("orderbook.sql.gz");

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
        let dump_path = dir.path().join("orderbook.sql.gz");

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
