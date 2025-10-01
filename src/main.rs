use rain_local_db_remote::run_sync;

fn main() {
    if let Err(error) = run_sync() {
        eprintln!("error: {error:?}");
        std::process::exit(1);
    }
}
