use std::env;
use std::path::PathBuf;

use anyhow::{bail, Context, Result};
use rain_local_db_remote::manifest::{bump_schema_version, SchemaVersionBump};

fn main() {
    if let Err(error) = run() {
        eprintln!("error: {error:?}");
        std::process::exit(1);
    }
}

fn run() -> Result<()> {
    let bump = run_with_args(env::args().skip(1))?;
    println!(
        "Bumped manifest schema version from {} to {}",
        bump.previous, bump.next
    );
    println!("previous={}", bump.previous);
    println!("next={}", bump.next);
    Ok(())
}

fn run_with_args<I>(mut args: I) -> Result<SchemaVersionBump>
where
    I: Iterator<Item = String>,
{
    let manifest_path = args
        .next()
        .map(PathBuf::from)
        .unwrap_or_else(|| PathBuf::from("data/manifest.yaml"));
    let source_path = args
        .next()
        .map(PathBuf::from)
        .unwrap_or_else(|| PathBuf::from("src/manifest.rs"));

    if args.next().is_some() {
        bail!("usage: bump-schema-version [manifest-path] [source-path]");
    }

    let bump = bump_schema_version(&manifest_path, &source_path).with_context(|| {
        format!(
            "failed to bump schema version in {}",
            manifest_path.display()
        )
    })?;
    Ok(bump)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;
    use tempfile::tempdir;

    use rain_local_db_remote::manifest::Manifest;

    #[test]
    fn run_with_args_bumps_schema_version() -> Result<()> {
        let dir = tempdir()?;
        let manifest_path = dir.path().join("manifest.yaml");
        let source_path = dir.path().join("manifest.rs");

        let manifest = Manifest::new();
        fs::write(&manifest_path, serde_yaml::to_string(&manifest)?)?;
        fs::write(
            &source_path,
            format!(
                "pub const CURRENT_SCHEMA_VERSION: u32 = {};\n",
                Manifest::CURRENT_SCHEMA_VERSION
            ),
        )?;

        let bump = run_with_args(
            vec![
                manifest_path.to_string_lossy().into_owned(),
                source_path.to_string_lossy().into_owned(),
            ]
            .into_iter(),
        )?;

        assert_eq!(bump.previous, Manifest::CURRENT_SCHEMA_VERSION);
        assert_eq!(bump.next, Manifest::CURRENT_SCHEMA_VERSION + 1);

        let stored: Manifest = serde_yaml::from_str(&fs::read_to_string(&manifest_path)?)?;
        assert_eq!(stored.schema_version, bump.next);

        let updated_source = fs::read_to_string(&source_path)?;
        assert!(updated_source.contains(&format!(
            "pub const CURRENT_SCHEMA_VERSION: u32 = {};",
            bump.next
        )));
        Ok(())
    }

    #[test]
    fn run_with_args_errors_on_extra_arguments() {
        let err = run_with_args(vec!["a".into(), "b".into(), "c".into()].into_iter()).unwrap_err();
        assert!(err
            .to_string()
            .contains("usage: bump-schema-version [manifest-path] [source-path]"));
    }
}
