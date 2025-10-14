use num_format::{Locale, ToFormattedString};

pub const CLI_ARCHIVE_NAME: &str = "rain-orderbook-cli.tar.gz";
pub const CLI_BINARY_URL_ENV_VAR: &str = "CLI_BINARY_URL";
pub const RELEASE_DOWNLOAD_URL_TEMPLATE: &str =
    "https://github.com/findolor/local_db_remote/releases/latest/download/{file}";
pub const API_TOKEN_ENV_VARS: &[&str] = &["HYPERRPC_API_TOKEN"];
pub const SETTINGS_YAML_ENV_VAR: &str = "SETTINGS_YAML_URL";
pub const SYNC_CHAIN_IDS_ENV_VAR: &str = "SYNC_CHAIN_IDS";

pub fn format_number(value: u64) -> String {
    value.to_formatted_string(&Locale::en)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn format_number_adds_grouping() {
        assert_eq!(format_number(1_234_567), "1,234,567");
    }
}
