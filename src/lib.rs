mod cli;
mod config;
mod file_hash;
mod fs_utils;
mod history;
mod manifest;
mod progress;
mod load;
mod snapshot;
mod store;

// Re-export public APIs
pub use cli::{Cli, Commands};
pub use config::{Config, DatabaseConfig, find_repo_root, CONFIG_FILENAME, KIBO_DIR, HISTORY_LOG_FILE};
pub use file_hash::HashCache;
pub use history::{HistoryEntry, log_entry, read_history, filter_by_snapshot, take_last};
pub use manifest::{FileEntry, DirectoryEntry, Manifest, list_snapshots, format_size};
pub use progress::{ProgressConfig, ByteProgress, ItemProgress, Spinner, Timer};
pub use load::{load_snapshot, LoadStats};
pub use snapshot::{create_snapshot};
pub use store::Store;
