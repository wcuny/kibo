use anyhow::{Context, Result};
use std::collections::HashSet;

use kibo::{
    Cli, Commands,
    Config, DatabaseConfig,
    HistoryEntry, log_entry, read_history, filter_by_snapshot, take_last,
    Manifest, list_snapshots, format_size,
    load_snapshot,
    create_snapshot,
    Store,
    find_repo_root,
    CONFIG_FILENAME,
    ProgressConfig, Timer, ItemProgress,
};

fn main() {
    if let Err(e) = run() {
        eprintln!("Error: {:#}", e);
        std::process::exit(1);
    }
}

fn run() -> Result<()> {
    let cli = Cli::parse_args();

    if matches!(cli.command, Commands::Init) {
        return cmd_init();
    }

    let root = find_repo_root()?;

    if matches!(cli.command, Commands::Config) {
        return cmd_config(&root);
    }
    if matches!(cli.command, Commands::History { .. }) {
        let Commands::History { last, snapshot, json } = cli.command else { unreachable!() };
        return cmd_history(&root, last, snapshot, json);
    }

    let config_path = root.join(CONFIG_FILENAME);
    let config = Config::load(&config_path)?;

    match cli.command {
        Commands::Save { 
            name, 
            yes, 
            verbose, 
            include_db, 
            progress, 
            no_progress,
            compression_level,
            directories,
            add_directories,
            files,
            add_files,
        } => {
            let progress_config = ProgressConfig::from_flags(progress, no_progress, config.progress);
            
            let mut effective_config = config.clone();
            
            if let Some(level) = compression_level {
                effective_config.compression_level = level;
            }
            
            if let Some(ref dirs) = directories {
                effective_config.directories = dirs.clone();
            } else if let Some(ref add_dirs) = add_directories {
                effective_config.directories.extend(add_dirs.iter().cloned());
            }
            
            if let Some(ref file_patterns) = files {
                effective_config.files = file_patterns.clone();
            } else if let Some(ref add_file_patterns) = add_files {
                effective_config.files.extend(add_file_patterns.iter().cloned());
            }
            
            if effective_config.directories.is_empty() && effective_config.files.is_empty() {
                anyhow::bail!(
                    "Configuration error: both 'directories' and 'files' lists are empty.\n\
                     You must specify at least one directory or file pattern to snapshot."
                );
            }
            effective_config.validate_compression_level();
            
            cmd_save(&root, &name, &effective_config, yes, verbose, include_db, progress_config, &config, 
                     compression_level, &directories, &add_directories, &files, &add_files)?
        },
        Commands::Load { name, verbose, include_db, progress, no_progress } => {
            let progress_config = ProgressConfig::from_flags(progress, no_progress, config.progress);
            cmd_load(&root, &name, verbose, include_db, &config, progress_config)?
        },
        Commands::List { sort_by_name, sort_by_size, sort_by_created, sort_by_files } => {
            cmd_list(&root, sort_by_name, sort_by_size, sort_by_created, sort_by_files)?
        },
        Commands::Remove { names, progress, no_progress } => {
            let progress_config = ProgressConfig::from_flags(progress, no_progress, config.progress);
            cmd_remove(&root, &names, progress_config)?
        },
        Commands::Prune { verbose, progress, no_progress } => {
            let progress_config = ProgressConfig::from_flags(progress, no_progress, config.progress);
            cmd_prune(&root, verbose, progress_config)?
        },
        Commands::Init => unreachable!(), // Already handled above
        Commands::Config => unreachable!(), // Handled above
        Commands::History { .. } => unreachable!(), // Handled above
    }

    Ok(())
}

/// Initialize a new .kibo.toml configuration file
fn cmd_init() -> Result<()> {
    use std::io::Write;
    
    let config_path = std::path::Path::new(CONFIG_FILENAME);
    
    if config_path.exists() {
        anyhow::bail!(
            "Configuration file '{}' already exists.\n\
             Delete it first if you want to recreate it.",
            CONFIG_FILENAME
        );
    }

    // Default configuration content
    let default_config = r#"# Kibo Configuration
# 
# At least one of 'directories' or 'files' must be specified

# Directories to snapshot (optional)
# These directories will be recursively snapshotted
directories = ["build", "target", "out"]

# Specific file patterns to snapshot (optional)
# Uses glob patterns, searched recursively from the project root
# Patterns without ** will automatically search recursively
# Examples:
#   "moc_*.cpp"           -> finds all moc_*.cpp files anywhere
#   "*.o"                 -> finds all .o files anywhere
#   "frontend/dist/*.js"  -> finds .js files in any frontend/dist directory
#   "**/out/**/*.a"       -> finds .a files in any out directory (explicit recursive)
files = [
    # "moc_*.cpp",
    # "*.o",
]

# Patterns to ignore (optional)
# These files/directories will be excluded from snapshots
ignore = [
    "*.log",
    "*.tmp",
    ".cache",
    "temp",
]

# Maximum snapshot size warning threshold in GB (optional)
# A warning will be shown if a snapshot exceeds this size
max_snapshot_size_gb = 10.0

# Compression level (zstd)
# 0  = No compression (fastest)
# 1  = Fast compression
# 3  = Balanced (recommended)
# 6  = High compression (slower)
# 10 = Maximum allowed (very slow)
# Range: 0–10
compression_level = 0

# Show progress bars (optional)
# true  = Always show progress bars
# false = Never show progress bars
# Omit this option to auto-detect (shows progress if stderr is a TTY)
# Can be overridden with --progress or --no-progress flags
# progress = true

# Database configuration (optional)
# Uncomment to enable database snapshots with --include-db flag
# The [database] section provides connection settings for database snapshots.
# When using --include-db:
#   - Without value (e.g., --include-db): uses the 'name' field below
#   - With value (e.g., --include-db=mydb): uses specified database name
# SQL dump files are auto-generated per snapshot and stored in .kibo/db_snapshots/
# [database]
# type = "mysql"
# user = "root"
# password = ""
# host = "localhost"
# port = 3306
# name = "mydb"  # Default database name when --include-db has no value
# tables = ["*"]  # "*" means all tables, or specify: ["users", "products"]
# single_transaction = true  # Ensures consistent InnoDB snapshots without locking
"#;

    let mut file = std::fs::File::create(config_path)?;
    file.write_all(default_config.as_bytes())?;

    println!("Created '{}'", CONFIG_FILENAME);
    println!("\nNext steps:");
    println!("  1. Edit {}", CONFIG_FILENAME);
    println!("  2. Run 'kibo save <name>' to create your first snapshot");

    Ok(())
}

/// Save a snapshot
fn cmd_save(
    root: &std::path::Path, 
    name: &str, 
    config: &Config, 
    yes: bool, 
    verbose: bool, 
    include_db: Option<String>, 
    progress_config: ProgressConfig, 
    base_config: &Config,
    compression_level: Option<u32>,
    directories: &Option<Vec<String>>,
    add_directories: &Option<Vec<String>>,
    files: &Option<Vec<String>>,
    add_files: &Option<Vec<String>>,
) -> Result<()> {
    let timer = Timer::new();
    
    validate_snapshot_name(name)?;

    // Determine database name if --include-db is specified
    let db_name_to_dump = if let Some(ref db_flag_value) = include_db {
        // --include-db flag was used
        if db_flag_value.is_empty() {
            // --include-db without value: use config database name
            if let Some(ref db_config) = base_config.database {
                Some(db_config.name.clone())
            }
            else {
                anyhow::bail!(
                    "--include-db specified but no [database] section found in {}\n\
                     Add a [database] section to your config or specify database name with --include-db=<dbname>",
                    CONFIG_FILENAME
                );
            }
        }
        else {
            // --include-db=<dbname>: use specified database name
            if base_config.database.is_none() {
                anyhow::bail!(
                    "--include-db specified but no [database] section found in {}\n\
                     The [database] section is required for connection settings (host, user, password, etc.)",
                    CONFIG_FILENAME
                );
            }
            Some(db_flag_value.clone())
        }
    }
    else {
        None
    };

    if Manifest::exists(root, name) {
        if !yes {
            use std::io::{self, Write};
            print!("Snapshot '{}' already exists. Overwrite with current workspace state? [y/N] ", name);
            io::stdout().flush()?;

            let mut input = String::new();
            io::stdin().read_line(&mut input)?;
            let input = input.trim().to_lowercase();

            if input != "y" && input != "yes" {
                println!("Aborted.");
                return Ok(());
            }
        }

        println!("Removing existing snapshot '{}'", name);
        
        // Load the old manifest to get the old database dump filename
        if let Ok(old_manifest) = Manifest::load(root, name) {
            if let Some(ref old_db_filename) = old_manifest.db_dump_filename {
                let old_dump_path = root.join(".kibo").join("db_snapshots").join(old_db_filename);
                if old_dump_path.exists() {
                    if let Err(e) = std::fs::remove_file(&old_dump_path) {
                        eprintln!("Warning: Failed to delete old database dump {}: {}", old_db_filename, e);
                    } 
                    else if verbose {
                        eprintln!("Deleted old database dump: {}", old_db_filename);
                    }
                }
            }
        }
        
        Manifest::delete(root, name)?;
    }

    // Dump database if --include-db is specified
    let db_dump_filename = if let Some(db_name) = db_name_to_dump {
        if let Some(ref db_config) = base_config.database {
            Some(dump_database(root, name, &db_name, db_config, verbose)?)
        }
        else {
            None
        }
    }
    else {
        None
    };

    let mut manifest = create_snapshot(root, name, config, verbose, progress_config)?;
    manifest.db_dump_filename = db_dump_filename.clone();
    
    // Save manifest now that db_dump_filename is set
    manifest.save(root)?;

    let store = Store::new(root);
    let disk_size = store.total_size().unwrap_or(0);

    println!(
        "\nSnapshot '{}' saved successfully",
        manifest.name
    );
    
    if manifest.file_count == 0 {
        println!("  Files: 0");
        println!("  Note: An empty snapshot represents an intentionally clean artifact state and can be loaded just like any other snapshot.");
    } 
    else {
        println!("  Files: {}", manifest.file_count);
    }
    
    println!("  Size of snapshot: {}", manifest.human_size());
    println!("  Size of store: {}", format_size(disk_size));
    println!("  Time: {}", timer.elapsed_string());
    
    // Log to history
    let mut flags = Vec::new();
    if verbose { flags.push("--verbose".to_string()); }
    if let Some(ref db_val) = include_db {
        if db_val.is_empty() {
            flags.push("--include-db".to_string());
        } else {
            flags.push(format!("--include-db={}", db_val));
        }
    }
    if yes { flags.push("--yes".to_string()); }
    if let Some(level) = compression_level {
        flags.push(format!("--compression-level={}", level));
    }
    if let Some(dirs) = directories {
        flags.push(format!("--directories={}", dirs.join(",")));
    }
    if let Some(add_dirs) = add_directories {
        flags.push(format!("--add-directories={}", add_dirs.join(",")));
    }
    if let Some(file_patterns) = files {
        flags.push(format!("--files={}", file_patterns.join(",")));
    }
    if let Some(add_file_patterns) = add_files {
        flags.push(format!("--add-files={}", add_file_patterns.join(",")));
    }
    let entry = HistoryEntry::new("SAVE", Some(name), flags);
    log_entry(root, &entry);

    Ok(())
}

/// Load a snapshot
fn cmd_load(root: &std::path::Path, name: &str, verbose: bool, include_db: bool, config: &Config, progress_config: ProgressConfig) -> Result<()> {

    // Load manifest first to check for database dump
    let manifest = Manifest::load(root, name)?;

    let stats = load_snapshot(root, name, verbose, progress_config)?;

    println!("\nSnapshot '{}' loaded successfully", name);
    println!("  Files loaded: {}", stats.files_loaded);
    println!(
        "  {} copied, {} unchanged, {} symlinks",
        stats.copies, stats.unchanged, stats.symlinks
    );

    if include_db {
        if config.database.is_none() {
            eprintln!("\nWarning: --include-db specified but no [database] section found in config");
            eprintln!("Database connection settings are required to load database dumps.");
        } else if let Some(ref db_config) = config.database {
            let loaded = load_database(root, &manifest, db_config, verbose)?;
            if loaded {
                println!("\nDatabase loaded successfully");
            }
        }
    }

    let mut flags = Vec::new();
    if verbose { flags.push("--verbose".to_string()); }
    if include_db { flags.push("--include-db".to_string()); }
    let entry = HistoryEntry::new("LOAD", Some(name), flags);
    log_entry(root, &entry);

    Ok(())
}

/// List all snapshots
fn cmd_list(root: &std::path::Path, sort_by_name: bool, sort_by_size: bool, sort_by_created: bool, sort_by_files: bool) -> Result<()> {
    let mut snapshots = list_snapshots(root)?;

    if snapshots.is_empty() {
        println!("No snapshots found.");
        println!("\nCreate a snapshot with: kibo save <name>");
        return Ok(());
    }

    if sort_by_name {
        snapshots.sort_by(|a, b| a.name.cmp(&b.name));
    } else if sort_by_size {
        snapshots.sort_by(|a, b| b.total_size.cmp(&a.total_size)); // Largest first
    } else if sort_by_created {
        snapshots.sort_by(|a, b| b.created_at.cmp(&a.created_at)); // Newest first
    } else if sort_by_files {
        snapshots.sort_by(|a, b| b.file_count.cmp(&a.file_count)); // Most first
    }
    // Default: keep order from list_snapshots (newest first by creation time)

    println!("Snapshots ({}):\n", snapshots.len());
    println!(
        "{:<20} {:<20} {:>10} {:>8}",
        "NAME", "CREATED", "SIZE", "FILES"
    );
    println!("{}", "-".repeat(60));

    for snapshot in &snapshots {
        println!(
            "{:<20} {:<20} {:>10} {:>8}",
            snapshot.name,
            snapshot.created_at.format("%Y-%m-%d %H:%M:%S"),
            snapshot.human_size(),
            snapshot.file_count
        );
    }

    let store = Store::new(root);
    if let Ok(total_size) = store.total_size() {
        if let Ok(blob_count) = store.blob_count() {
            println!("\nStore: {} blobs, {} on disk", blob_count, format_size(total_size));
        }
    }

    Ok(())
}

/// Prune unreferenced blobs from the store
fn cmd_prune(root: &std::path::Path, verbose: bool, progress_config: ProgressConfig) -> Result<()> {
    use std::collections::HashSet;

    let timer = Timer::new();
    
    println!("Scanning snapshots for referenced blobs and database dumps...");

    let snapshots = list_snapshots(root)?;
    let mut referenced_hashes = HashSet::new();
    let mut referenced_db_dumps = HashSet::new();
    
    if verbose {
        eprintln!("Found {} snapshot(s)", snapshots.len());
    }
    
    for snapshot in &snapshots {
        if verbose {
            eprintln!("  Scanning: {} ({} files)", snapshot.name, snapshot.file_count);
        }
        for entry in snapshot.files.values() {
            referenced_hashes.insert(entry.hash.clone());
        }
        
        // Track referenced database dumps
        if let Some(ref db_filename) = snapshot.db_dump_filename {
            referenced_db_dumps.insert(db_filename.clone());
        }
    }

    if verbose {
        eprintln!("Found {} unique referenced blob(s)", referenced_hashes.len());
        eprintln!("Found {} referenced database dump(s)", referenced_db_dumps.len());
    }

    println!("Pruning unreferenced blobs...");

    let store = Store::new(root);
    let (removed, freed) = store.garbage_collect(&referenced_hashes, progress_config.should_show_progress())?;

    // Prune unreferenced database dumps
    let db_dumps_dir = root.join(".kibo").join("db_snapshots");
    let mut db_removed = 0;
    let mut db_freed = 0u64;
    
    if db_dumps_dir.exists() {
        if verbose {
            eprintln!("Scanning database dumps directory...");
        }
        
        for entry in std::fs::read_dir(&db_dumps_dir)? {
            let entry = entry?;
            if entry.file_type()?.is_file() {
                let filename = entry.file_name().to_string_lossy().to_string();
                
                // Only remove .sql files that aren't referenced
                if filename.ends_with(".sql") && !referenced_db_dumps.contains(&filename) {
                    let metadata = entry.metadata()?;
                    let size = metadata.len();
                    
                    if verbose {
                        eprintln!("  Removing unreferenced database dump: {}", filename);
                    }
                    
                    std::fs::remove_file(entry.path())?;
                    db_removed += 1;
                    db_freed += size;
                }
            }
        }
    }

    if removed > 0 || db_removed > 0 {
        println!(
            "\nPruned {} unreferenced blob(s) (freed {}) and {} database dump(s) (freed {}) in {}",
            removed,
            format_size(freed),
            db_removed,
            format_size(db_freed),
            timer.elapsed_string()
        );
    }
    else {
        println!("\nNo unreferenced blobs or database dumps found in {}", timer.elapsed_string());
    }

    let mut flags = Vec::new();
    if verbose { flags.push("--verbose".to_string()); }
    let entry = HistoryEntry::new("PRUNE", None, flags);
    log_entry(root, &entry);

    Ok(())
}

/// Remove one or more snapshots by name
fn cmd_remove(root: &std::path::Path, names: &[String], progress_config: ProgressConfig) -> Result<()> {
    if names.is_empty() {
        anyhow::bail!("No snapshot names provided");
    }

    let timer = Timer::new();
    
    println!("Removing {} snapshot(s)", names.len());

    let mut snapshots_to_delete = Vec::new();
    
    for name in names {
        if !Manifest::exists(root, name) {
            eprintln!("Warning: Snapshot '{}' does not exist, skipping", name);
            continue;
        }
        
        if let Ok(manifest) = Manifest::load(root, name) {
            snapshots_to_delete.push((name.clone(), manifest));
        }
    }

    if snapshots_to_delete.is_empty() {
        println!("No snapshots were deleted");
        return Ok(());
    }

    let progress = ItemProgress::new(
        snapshots_to_delete.len() as u64,
        progress_config,
        "snapshots"
    );

    let mut deleted_count = 0;
    let mut db_dumps_deleted = 0;
    for (name, manifest) in &snapshots_to_delete {
        if progress.is_enabled() {
            progress.set_message(format!("Deleting: {}", name));
        }
        else {
            println!("  Deleting: {}", name);
        }
        
        // Delete associated database dump if it exists
        if let Some(ref db_filename) = manifest.db_dump_filename {
            let db_dump_path = root.join(".kibo").join("db_snapshots").join(db_filename);
            if db_dump_path.exists() {
                if let Err(e) = std::fs::remove_file(&db_dump_path) {
                    eprintln!("Warning: Failed to delete database dump {}: {}", db_filename, e);
                } else {
                    db_dumps_deleted += 1;
                }
            }
        }
        
        Manifest::delete(root, name)?;
        deleted_count += 1;
        progress.inc(1);
    }

    progress.finish();

    let remaining_snapshots = list_snapshots(root)?;
    let mut referenced_hashes = HashSet::new();
    
    for snapshot in &remaining_snapshots {
        for entry in snapshot.files.values() {
            referenced_hashes.insert(entry.hash.clone());
        }
    }

    let store = Store::new(root);
    let (removed, freed) = store.garbage_collect(&referenced_hashes, progress_config.should_show_progress())?;

    let mut msg = format!(
        "\nRemoved {} snapshot(s)",
        deleted_count
    );
    
    if db_dumps_deleted > 0 {
        msg.push_str(&format!(", {} database dump(s)", db_dumps_deleted));
    }
    
    msg.push_str(&format!(
        ", {} unreferenced blob(s), freed {} in {}",
        removed,
        format_size(freed),
        timer.elapsed_string()
    ));
    
    println!("{}", msg);

    for (name, _) in &snapshots_to_delete {
        let entry = HistoryEntry::new("RM", Some(name), Vec::new());
        log_entry(root, &entry);
    }

    Ok(())
}

/// Dump MySQL database to SQL file with auto-generated filename
fn dump_database(
    root: &std::path::Path,
    snapshot_name: &str,
    db_name: &str,
    db_config: &DatabaseConfig,
    verbose: bool,
) -> Result<String> {
    use std::process::Command;
    use chrono::Utc;

    if db_config.db_type != "mysql" {
        anyhow::bail!("Only MySQL databases are currently supported");
    }

    println!("Dumping database '{}'", db_name);

    // Auto-generate filename: snapshot_name-dbname-timestamp.sql
    let timestamp = Utc::now().format("%Y%m%d_%H%M%S");
    let dump_filename = format!("{}-{}-{}.sql", snapshot_name, db_name, timestamp);
    let db_dumps_dir = root.join(".kibo").join("db_snapshots");
    std::fs::create_dir_all(&db_dumps_dir)
        .with_context(|| format!("Failed to create dump directory: {}", db_dumps_dir.display()))?;
    
    let dump_path = db_dumps_dir.join(&dump_filename);

    let mut cmd = Command::new("mysqldump");
    cmd.arg(format!("--user={}", db_config.user))
        .arg(format!("--host={}", db_config.host))
        .arg(format!("--port={}", db_config.port))
        .arg("--databases")
        .arg(db_name)
        .arg("--routines")
        .arg("--triggers")
        .arg("--events");

    if !db_config.password.is_empty() {
        cmd.arg(format!("--password={}", db_config.password));
    }

    if db_config.single_transaction {
        cmd.arg("--single-transaction");
    }

    cmd.stdout(std::fs::File::create(&dump_path)?);

    if verbose {
        eprintln!("Running: mysqldump to {}", dump_path.display());
    }

    let status = cmd.status()
        .with_context(|| "Failed to execute mysqldump. Is MySQL client installed?")?;

    if !status.success() {
        anyhow::bail!("mysqldump failed with exit code: {}", status);
    }

    println!("Database dumped to {}", dump_path.display());

    Ok(dump_filename)
}

/// Load MySQL database from SQL file using manifest metadata
/// Returns Ok(true) if database was loaded, Ok(false) if skipped/not available
fn load_database(
    root: &std::path::Path,
    manifest: &Manifest,
    db_config: &DatabaseConfig,
    verbose: bool,
) -> Result<bool> {
    use std::process::Command;
    use std::io::{self, Write};

    if db_config.db_type != "mysql" {
        anyhow::bail!("Only MySQL databases are currently supported");
    }

    let dump_filename = match &manifest.db_dump_filename {
        Some(filename) => filename,
        None => {
            eprintln!("\nWarning: This snapshot does not include a database dump.");
            eprintln!("To include database dumps, use --include-db when saving snapshots.");
            return Ok(false);
        }
    };

    let dump_path = root.join(".kibo").join("db_snapshots").join(dump_filename);
    
    if !dump_path.exists() {
        eprintln!("\nWarning: Database dump file not found: {}", dump_path.display());
        eprintln!("The snapshot metadata references this file, but it may have been deleted.");
        return Ok(false);
    }

    // Prompt user before loading database
    print!("\n   Loading database from snapshot. This will overwrite the current database. Continue? [y/N] ");
    io::stdout().flush()?;

    let mut input = String::new();
    io::stdin().read_line(&mut input)?;
    let input = input.trim().to_lowercase();

    if input != "y" && input != "yes" {
        println!("Database load skipped.");
        return Ok(false);
    }

    println!("Loading database from {}", dump_filename);

    let mut cmd = Command::new("mysql");
    cmd.arg(format!("--user={}", db_config.user))
        .arg(format!("--host={}", db_config.host))
        .arg(format!("--port={}", db_config.port));

    if !db_config.password.is_empty() {
        cmd.arg(format!("--password={}", db_config.password));
    }

    cmd.stdin(std::fs::File::open(&dump_path)?);

    if verbose {
        eprintln!("Running: mysql < {}", dump_path.display());
    }

    let status = cmd.status()
        .with_context(|| "Failed to execute mysql. Is MySQL client installed?")?;

    if !status.success() {
        anyhow::bail!("mysql load failed with exit code: {}", status);
    }

    Ok(true)
}

/// Validate that a snapshot name is safe
fn validate_snapshot_name(name: &str) -> Result<()> {
    if name.is_empty() {
        anyhow::bail!("Snapshot name cannot be empty");
    }

    if name.contains('/') || name.contains('\\') || name.contains('\0') {
        anyhow::bail!("Snapshot name contains invalid characters");
    }

    if name.starts_with('.') {
        anyhow::bail!("Snapshot name cannot start with a dot");
    }

    if name.len() > 255 {
        anyhow::bail!("Snapshot name is too long (max 255 characters)");
    }

    let reserved = [".", "..", "store", "manifests", "hash_cache"];
    if reserved.contains(&name) {
        anyhow::bail!("Snapshot name '{}' is reserved", name);
    }

    Ok(())
}

/// Open the .kibo.toml configuration file in vim
fn cmd_config(root: &std::path::Path) -> Result<()> {
    let config_path = root.join(CONFIG_FILENAME);
    
    if !config_path.exists() {
        eprintln!("Configuration file not found: {}", config_path.display());
        eprintln!("\nRun 'kibo init' to create a new configuration file.");
        anyhow::bail!("Configuration file does not exist");
    }

    println!("Opening {} with vim", CONFIG_FILENAME);

    let status = std::process::Command::new("vim")
        .arg(&config_path)
        .status()
        .with_context(|| "Failed to launch vim")?;

    if !status.success() {
        anyhow::bail!("vim exited with error");
    }

    Ok(())
}

/// Display command history
fn cmd_history(root: &std::path::Path, last: Option<usize>, snapshot_filter: Option<String>, json: bool) -> Result<()> {
    let mut entries = read_history(root)?;

    if entries.is_empty() {
        println!("No history available.");
        return Ok(());
    }

    if let Some(ref snapshot) = snapshot_filter {
        entries = filter_by_snapshot(entries, snapshot);
        if entries.is_empty() {
            println!("No history entries found for snapshot '{}'", snapshot);
            return Ok(());
        }
    }

    if let Some(n) = last {
        entries = take_last(entries, n);
    }

    if json {
        let json_output = serde_json::to_string_pretty(&entries)?;
        println!("{}", json_output);
    }
    else {
        println!("Command History:\n");
        println!("{:<20} {:<8} {:<20} {}", "TIMESTAMP", "COMMAND", "SNAPSHOT", "FLAGS");
        println!("{}", "-".repeat(80));
        for entry in &entries {
            println!("{}", entry.display());
        }
        println!("\nTotal entries: {}", entries.len());
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_validate_snapshot_name_valid() {
        assert!(validate_snapshot_name("my-snapshot").is_ok());
        assert!(validate_snapshot_name("feature_branch_v1").is_ok());
        assert!(validate_snapshot_name("2026-01-01").is_ok());
        assert!(validate_snapshot_name("backup-2026-01-01").is_ok());
        assert!(validate_snapshot_name("a").is_ok());
    }

    #[test]
    fn test_validate_snapshot_name_empty() {
        assert!(validate_snapshot_name("").is_err());
        let err = validate_snapshot_name("").unwrap_err();
        assert!(err.to_string().contains("empty"));
    }

    #[test]
    fn test_validate_snapshot_name_with_slash() {
        assert!(validate_snapshot_name("path/with/slash").is_err());
        assert!(validate_snapshot_name("path\\with\\backslash").is_err());
    }

    #[test]
    fn test_validate_snapshot_name_with_null() {
        assert!(validate_snapshot_name("name\0with\0null").is_err());
    }

    #[test]
    fn test_validate_snapshot_name_starts_with_dot() {
        assert!(validate_snapshot_name(".hidden").is_err());
        assert!(validate_snapshot_name("..parent").is_err());
    }

    #[test]
    fn test_validate_snapshot_name_too_long() {
        let long_name = "a".repeat(256);
        assert!(validate_snapshot_name(&long_name).is_err());
        let err = validate_snapshot_name(&long_name).unwrap_err();
        assert!(err.to_string().contains("too long"));
    }

    #[test]
    fn test_validate_snapshot_name_max_length() {
        let max_name = "a".repeat(255);
        assert!(validate_snapshot_name(&max_name).is_ok());
    }

    #[test]
    fn test_validate_snapshot_name_reserved_words() {
        assert!(validate_snapshot_name("store").is_err());
        assert!(validate_snapshot_name("manifests").is_err());
        assert!(validate_snapshot_name(".").is_err());
        assert!(validate_snapshot_name("..").is_err());
        assert!(validate_snapshot_name("hash_cache").is_err());
    }

    #[test]
    fn test_validate_snapshot_name_reserved_error_message() {
        let err = validate_snapshot_name("store").unwrap_err();
        assert!(err.to_string().contains("reserved"));
    }

    #[test]
    fn test_validate_snapshot_name_special_chars() {
        // These should be valid
        assert!(validate_snapshot_name("snapshot-with-dash").is_ok());
        assert!(validate_snapshot_name("snapshot_with_underscore").is_ok());
        assert!(validate_snapshot_name("snapshot.with.dots").is_ok());
    }
}


