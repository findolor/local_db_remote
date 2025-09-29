use crate::constants::format_number;
use crate::database::SyncPlan;

pub fn log_plan(network: &str, plan: &SyncPlan) {
    for line in plan_lines(network, plan) {
        println!("{line}");
    }
}

fn plan_lines(network: &str, plan: &SyncPlan) -> Vec<String> {
    vec![
        String::new(),
        format!("Plan for {network}"),
        format!("  Database path: {}", plan.db_path.display()),
        format!("  Dump path: {}", plan.dump_path.display()),
        format!(
            "  Last synced block: {}",
            plan.last_synced_block
                .map(format_number)
                .unwrap_or_else(|| "none".to_string())
        ),
        format!(
            "  Next start block: {}",
            plan.next_start_block
                .map(format_number)
                .unwrap_or_else(|| "determined by CLI".to_string())
        ),
        "  Blocks to fetch: determined by CLI".to_string(),
    ]
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::path::PathBuf;

    #[test]
    fn plan_lines_includes_expected_fields() {
        let plan = SyncPlan {
            db_path: PathBuf::from("db/path"),
            dump_path: PathBuf::from("dump/path"),
            last_synced_block: Some(1_000),
            next_start_block: Some(1_001),
        };

        let lines = plan_lines("network", &plan);
        assert_eq!(lines[1], "Plan for network");
        assert!(lines.iter().any(|line| line.contains("db/path")));
        assert!(lines.iter().any(|line| line.contains("dump/path")));
        assert!(lines.iter().any(|line| line.contains("1,000")));
        assert!(lines.iter().any(|line| line.contains("1,001")));
    }

    #[test]
    fn plan_lines_handles_missing_blocks() {
        let plan = SyncPlan {
            db_path: PathBuf::from("db"),
            dump_path: PathBuf::from("dump"),
            last_synced_block: None,
            next_start_block: None,
        };

        let lines = plan_lines("net", &plan);
        assert!(lines.iter().any(|line| line.contains("none")));
        assert!(lines.iter().any(|line| line.contains("determined by CLI")));
    }
}
