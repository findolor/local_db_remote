use std::fs;
use std::path::{Path, PathBuf};
use std::process::Command;

use anyhow::{Context, Result};
use walkdir::WalkDir;

use crate::constants::CLI_ARCHIVE_URL_TEMPLATE;
use crate::http::HttpClient;

pub fn download_cli_archive(
    http: &dyn HttpClient,
    commit_hash: &str,
    destination: &Path,
) -> Result<PathBuf> {
    let url = CLI_ARCHIVE_URL_TEMPLATE.replace("{commit}", commit_hash);
    let bytes = http.fetch_binary(&url)?;
    fs::write(destination, &bytes)
        .with_context(|| format!("failed to write archive to {}", destination.display()))?;
    println!(
        "Downloaded CLI archive to {} ({} bytes)",
        destination.display(),
        bytes.len()
    );
    Ok(destination.to_path_buf())
}

pub fn extract_cli_binary(archive_path: &Path, output_dir: &Path) -> Result<PathBuf> {
    fs::create_dir_all(output_dir)
        .with_context(|| format!("failed to create directory {}", output_dir.display()))?;

    let status = Command::new("tar")
        .arg("-xzf")
        .arg(archive_path)
        .arg("-C")
        .arg(output_dir)
        .status()
        .with_context(|| "failed to spawn tar for archive extraction")?;

    if !status.success() {
        anyhow::bail!(
            "failed to extract CLI archive (exit code {:?})",
            status.code()
        );
    }

    let candidate = find_binary(output_dir)?.ok_or_else(|| {
        anyhow::anyhow!(
            "unable to locate rain-orderbook-cli binary under {}",
            output_dir.display()
        )
    })?;

    set_executable(&candidate)?;

    println!("Extracted CLI binary to {}", candidate.display());
    Ok(candidate)
}

fn find_binary(root: &Path) -> Result<Option<PathBuf>> {
    for entry in WalkDir::new(root) {
        let entry = entry?;
        if entry.file_type().is_file() && entry.file_name() == "rain-orderbook-cli" {
            return Ok(Some(entry.into_path()));
        }
    }
    Ok(None)
}

fn set_executable(path: &Path) -> Result<()> {
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        let mut permissions = fs::metadata(path)
            .with_context(|| format!("failed to read permissions for {}", path.display()))?
            .permissions();
        permissions.set_mode(0o755);
        fs::set_permissions(path, permissions)
            .with_context(|| format!("failed to set executable bit on {}", path.display()))?;
        Ok(())
    }
    #[cfg(not(unix))]
    {
        let _ = path;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;
    use tempfile::tempdir;

    struct StubHttpClient {
        payload: Vec<u8>,
    }

    impl HttpClient for StubHttpClient {
        fn fetch_text(&self, _url: &str) -> Result<String> {
            Ok(String::from_utf8(self.payload.clone()).unwrap())
        }

        fn fetch_binary(&self, _url: &str) -> Result<Vec<u8>> {
            Ok(self.payload.clone())
        }
    }

    #[test]
    fn download_cli_archive_writes_bytes_to_disk() {
        let temp = tempdir().unwrap();
        let destination = temp.path().join("archive.tar.gz");
        let client = StubHttpClient {
            payload: b"test-bytes".to_vec(),
        };

        let path = download_cli_archive(&client, "deadbeef", &destination).unwrap();

        assert_eq!(path, destination);
        let written = std::fs::read(&destination).unwrap();
        assert_eq!(written, b"test-bytes");
    }

    #[test]
    fn extract_cli_binary_unpacks_archive_and_sets_permissions() {
        let temp = tempdir().unwrap();
        let archive_path = temp.path().join("cli.tar.gz");
        let staging = tempdir().unwrap();

        let binary_path = staging.path().join("rain-orderbook-cli");
        {
            let mut file = std::fs::File::create(&binary_path).unwrap();
            writeln!(file, "#!/bin/sh\necho cli").unwrap();
        }

        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            let mut perms = std::fs::metadata(&binary_path).unwrap().permissions();
            perms.set_mode(0o644);
            std::fs::set_permissions(&binary_path, perms).unwrap();
        }

        let status = Command::new("tar")
            .arg("-czf")
            .arg(&archive_path)
            .arg("-C")
            .arg(staging.path())
            .arg(".")
            .status()
            .unwrap();
        assert!(status.success());

        let output_dir = temp.path().join("output");
        let extracted = extract_cli_binary(&archive_path, &output_dir).unwrap();
        assert!(extracted.exists());

        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            let mode = std::fs::metadata(&extracted).unwrap().permissions().mode() & 0o777;
            assert_eq!(mode, 0o755);
        }
    }

    #[test]
    fn find_binary_locates_cli() {
        let temp = tempdir().unwrap();
        let nested = temp.path().join("a/b");
        std::fs::create_dir_all(&nested).unwrap();
        let target = nested.join("rain-orderbook-cli");
        std::fs::write(&target, b"bin").unwrap();

        let found = find_binary(temp.path()).unwrap();
        assert_eq!(found.unwrap(), target);
    }

    #[cfg(unix)]
    #[test]
    fn set_executable_applies_expected_mode() {
        use std::os::unix::fs::PermissionsExt;
        let temp = tempdir().unwrap();
        let path = temp.path().join("cli");
        std::fs::write(&path, b"bin").unwrap();

        let mut perms = std::fs::metadata(&path).unwrap().permissions();
        perms.set_mode(0o600);
        std::fs::set_permissions(&path, perms).unwrap();

        set_executable(&path).unwrap();
        let mode = std::fs::metadata(&path).unwrap().permissions().mode() & 0o777;
        assert_eq!(mode, 0o755);
    }
}
