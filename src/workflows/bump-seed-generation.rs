use std::env;
use std::path::PathBuf;

use anyhow::{bail, Context, Result};
use rain_local_db_remote::manifest::{bump_seed_generation, NetworkId, SeedGenerationBump};

fn main() {
    if let Err(error) = run() {
        eprintln!("error: {error:?}");
        std::process::exit(1);
    }
}

fn run() -> Result<()> {
    let bump = run_with_args(env::args().skip(1))?;
    println!(
        "Bumped seed generation for chain {} from {} to {}",
        u64::from(bump.network_id),
        bump.previous,
        bump.next
    );
    println!("previous={}", bump.previous);
    println!("next={}", bump.next);
    Ok(())
}

fn run_with_args<I>(mut args: I) -> Result<SeedGenerationBump>
where
    I: Iterator<Item = String>,
{
    let chain_id_str = args
        .next()
        .context("expected chain id argument (e.g. 42161)")?;
    let chain_id: u64 = chain_id_str
        .parse()
        .with_context(|| format!("failed to parse chain id '{chain_id_str}' as u64"))?;

    let manifest_path = args
        .next()
        .map(PathBuf::from)
        .unwrap_or_else(|| PathBuf::from("data/manifest.yaml"));

    if args.next().is_some() {
        bail!("usage: bump-seed-generation <chain-id> [manifest-path]");
    }

    let bump = bump_seed_generation(&manifest_path, NetworkId::from(chain_id))
        .with_context(|| format!("failed to bump seed generation for chain {}", chain_id))?;
    Ok(bump)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;
    use tempfile::tempdir;

    use rain_local_db_remote::manifest::{Manifest, ManifestEntry};

    #[test]
    fn run_with_args_bumps_seed_generation() -> Result<()> {
        let dir = tempdir()?;
        let manifest_path = dir.path().join("manifest.yaml");

        let chain_id = 42u64;
        let mut manifest = Manifest::new();
        manifest.networks.insert(
            NetworkId::from(chain_id),
            ManifestEntry {
                dump_url: "https://example.com/dump.sql.gz".to_string(),
                dump_timestamp: "2024-01-01T00:00:00Z".to_string(),
                seed_generation: 7,
            },
        );
        fs::write(&manifest_path, serde_yaml::to_string(&manifest)?)?;

        let bump = run_with_args(
            vec![
                chain_id.to_string(),
                manifest_path.to_string_lossy().into_owned(),
            ]
            .into_iter(),
        )?;

        assert_eq!(bump.network_id, NetworkId::from(chain_id));
        assert_eq!(bump.previous, 7);
        assert_eq!(bump.next, 8);

        let stored: Manifest = serde_yaml::from_str(&fs::read_to_string(&manifest_path)?)?;
        assert_eq!(
            stored
                .networks
                .get(&NetworkId::from(chain_id))
                .expect("entry exists")
                .seed_generation,
            8
        );
        Ok(())
    }

    #[test]
    fn run_with_args_errors_on_invalid_chain_id() {
        let err =
            run_with_args(vec!["abc".into()].into_iter()).expect_err("should error on invalid id");
        assert!(err
            .to_string()
            .contains("failed to parse chain id 'abc' as u64"));
    }
}
