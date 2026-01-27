use clap::{Parser, Subcommand};

#[derive(Parser, Debug)]
#[command(name = "kibo")]
#[command(author, version, about, long_about = None)]
pub struct Cli {
    #[command(subcommand)]
    pub command: Commands,
}

#[derive(Subcommand, Debug)]
pub enum Commands {
    /// Save a snapshot of tracked directories and files
    Save {
        /// Name for the snapshot
        #[arg(value_name = "SNAPSHOT_NAME")]
        name: String,

        /// Automatically overwrite existing snapshot without prompting
        #[arg(short = 'y', long = "yes")]
        yes: bool,

        /// Show verbose output
        #[arg(short = 'v', long = "verbose")]
        verbose: bool,

        /// Include database snapshot with optional database name (e.g., --include-db or --include-db=mydb)
        /// If database name is not specified, uses the name from config [database] section
        #[arg(long = "include-db")]
        include_db: Option<String>,

        /// Force enable progress bar
        #[arg(long = "progress")]
        progress: bool,

        /// Force disable progress bar
        #[arg(long = "no-progress", conflicts_with = "progress")]
        no_progress: bool,

        /// Override compression level (0 = no compression, 1-10 = zstd levels)
        #[arg(long = "compression-level")]
        compression_level: Option<u32>,

        /// Override directories from config (comma-separated, e.g., --directories="dir1,dir2")
        #[arg(long = "directories", value_delimiter = ',', conflicts_with = "add_directories", require_equals = true)]
        directories: Option<Vec<String>>,

        /// Add directories to config (comma-separated, e.g., --add-directories="dir1,dir2")
        #[arg(long = "add-directories", value_delimiter = ',', require_equals = true)]
        add_directories: Option<Vec<String>>,

        /// Override file patterns from config (comma-separated, e.g., --files="*.dat,*.o")
        #[arg(long = "files", value_delimiter = ',', conflicts_with = "add_files", require_equals = true)]
        files: Option<Vec<String>>,

        /// Add file patterns to config (comma-separated, e.g., --add-files="*.dat,*.o")
        #[arg(long = "add-files", value_delimiter = ',', require_equals = true)]
        add_files: Option<Vec<String>>,
    },

    /// Load a previously saved snapshot
    Load {
        /// Name of the snapshot to load
        #[arg(value_name = "SNAPSHOT_NAME")]
        name: String,

        /// Show verbose output
        #[arg(short = 'v', long = "verbose")]
        verbose: bool,

        /// Include database load (snapshot must include a database dump)
        #[arg(long = "include-db")]
        include_db: bool,

        /// Force enable progress bar
        #[arg(long = "progress")]
        progress: bool,

        /// Force disable progress bar
        #[arg(long = "no-progress", conflicts_with = "progress")]
        no_progress: bool,
    },

    /// List all saved snapshots
    #[command(alias = "ls")]
    List {
        /// Sort by snapshot name
        #[arg(long = "name", conflicts_with_all = ["size", "created", "files"])]
        sort_by_name: bool,

        /// Sort by size (largest first)
        #[arg(long = "size", conflicts_with_all = ["name", "created", "files"])]
        sort_by_size: bool,

        /// Sort by creation date (newest first)
        #[arg(long = "created", conflicts_with_all = ["name", "size", "files"])]
        sort_by_created: bool,

        /// Sort by number of files (most first)
        #[arg(long = "files", conflicts_with_all = ["name", "size", "created"])]
        sort_by_files: bool,
    },

    /// Remove one or more snapshots by name
    #[command(alias = "rm")]
    Remove {
        /// Names of snapshots to remove
        #[arg(value_name = "SNAPSHOT_NAME", required = true)]
        names: Vec<String>,

        /// Force enable progress bar
        #[arg(long = "progress")]
        progress: bool,

        /// Force disable progress bar
        #[arg(long = "no-progress", conflicts_with = "progress")]
        no_progress: bool,
    },

    /// Remove unreferenced blobs from the store
    Prune {
        /// Show verbose output
        #[arg(short = 'v', long = "verbose")]
        verbose: bool,

        /// Force enable progress bar
        #[arg(long = "progress")]
        progress: bool,

        /// Force disable progress bar
        #[arg(long = "no-progress", conflicts_with = "progress")]
        no_progress: bool,
    },

    /// Initialize a new .kibo.toml configuration file
    Init,

    /// Open the .kibo.toml configuration file in vim
    Config,

    /// View command history log
    History {
        /// Show only the last N entries
        #[arg(long = "last")]
        last: Option<usize>,

        /// Filter by snapshot name
        #[arg(long = "snapshot")]
        snapshot: Option<String>,

        /// Output as JSON
        #[arg(long = "json")]
        json: bool,
    },
}

impl Cli {
    pub fn parse_args() -> Self {
        Cli::parse()
    }
}
