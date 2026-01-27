![Kibo](kibo.jpg)

# Kibo

**Fast, deterministic snapshot and restore for build artifacts**

Kibo is a command line tool useful when working on large legacy codebases with expensive builds. Save fully built directory states and load them quickly without rebuilding, for rapid context switching between branches, configurations, and experiments.

[![License](https://img.shields.io/badge/license-GPLv3-blue)]()

---

## Quick Start

### Installation

```bash
# Clone and build from source
git clone https://github.com/yourusername/kibo.git
cd kibo
cargo build --release

# Install to /usr/local/bin
sudo cp target/release/kibo /usr/local/bin/
```

### Basic Usage

```bash
# Initialize configuration
kibo init

# Edit .kibo.toml to define what to track
kibo config

# Save a snapshot
kibo save my-snapshot

# List all snapshots
kibo list

# Load a snapshot
kibo load my-snapshot

# View history
kibo history
```

---

## Configuration

Kibo uses `.kibo.toml` in your project root:

```toml
# Directories to track (matches by name, recursively searches workspace)
directories = ["build", "target", "out"]

# File patterns to track (glob patterns, recursive search)
files = ["*.o", "*.a", "moc_*.cpp"]

# Patterns to ignore within tracked paths
ignore = ["*.log", "temp", "*.tmp"]

# Optional: Compression level (0 = none, 1-10 = zstd levels)
compression_level = 3

# Optional: Show progress bars (auto-detects TTY by default)
progress = true
```

### How Tracking Works

- **Directories**: Kibo recursively searches your workspace for directories matching the specified names (e.g., `build`). All instances are tracked, including nested ones like `temp/build`, `project/build`, etc.
- **Files**: Glob patterns are searched recursively across the entire workspace. Use `**` for explicit recursive matching.
- **Empty directories**: Tracked and restored with exact permissions and modification times.

### Database Configuration

Kibo can include MySQL database snapshots alongside your build artifacts. Add a `[database]` section to `.kibo.toml`:

```toml
[database]
# Database type (currently only "mysql" is supported)
db_type = "mysql"

# Database credentials
user = "root"
password = "your_password"
host = "localhost"
port = 3306

# Default database name (can be overridden with --include-db flag)
name = "mydb"

# Tables to snapshot ("*" means all tables)
tables = ["*"]

# Use single transaction for consistent snapshots
single_transaction = true
```

**Security Note**: Database passwords are stored in plain text in `.kibo.toml`. Consider using environment variables or restrictive file permissions (`chmod 600 .kibo.toml`).

---

## Commands

### `kibo save <name>`

Save a snapshot of tracked directories and files.

```bash
# Basic save
kibo save my-snapshot

# Auto-overwrite existing snapshot
kibo save my-snapshot -y

# Override tracking from config
kibo save my-snapshot --files="*.dat,*.o"
kibo save my-snapshot --directories="dist"

# Custom compression
kibo save my-snapshot --compression-level=6

# Verbose output
kibo save my-snapshot -v
```

**Options:**
- `-y, --yes` - Overwrite existing snapshot without prompting
- `-v, --verbose` - Show detailed output
- `--include-db=<name>` - Include MySQL database dump (uses config database name if not specified)
- `--compression-level=<N>` - Override compression level (0-10)
- `--directories=<LIST>` - Override directories from config (comma-separated)
- `--add-directories=<LIST>` - Add directories to config list
- `--files=<LIST>` - Override file patterns from config
- `--add-files=<LIST>` - Add file patterns to config list
- `--progress` / `--no-progress` - Force enable/disable progress bars

**Database Examples:**

```bash
# Include database using name from config
kibo save my-snapshot --include-db

# Include database with specific name (overrides config)
kibo save my-snapshot --include-db=production_db

# Save with both artifacts and database
kibo save release-v1.0 --include-db -v
```

Database dumps are stored as `.sql` files in `.kibo/db_snapshots/` with auto-generated filenames like `snapshot-dbname-timestamp.sql`. Each snapshot's manifest tracks which database dump (if any) belongs to it.

### `kibo load <name>`

Load a previously saved snapshot.

```bash
# Basic load
kibo load my-snapshot

# Load including database restore
kibo load my-snapshot --include-db

# Verbose output
kibo load my-snapshot -v
```

**Options:**
- `-v, --verbose` - Show detailed output
- `--include-db` - Restore database dump if included in snapshot
- `--progress` / `--no-progress` - Force enable/disable progress bars

**Behavior:**
- Restores all tracked directories and files from the snapshot
- Deletes tracked paths not present in the snapshot (ensures exact state)
- Preserves untracked files and directories
- Restores file permissions and modification times
- If `--include-db` is specified and the snapshot contains a database dump, restores the database using `mysql` command

**Database Restore Requirements:**
- `mysql` command must be available in PATH
- Database credentials from `.kibo.toml` are used
- Target database must exist (or will be created if user has permissions)
- Existing database data will be overwritten

### `kibo list`

List all saved snapshots with details.

```bash
# List all snapshots (sorted by creation date, newest first)
kibo list

# Sort by name
kibo list --name

# Sort by size (largest first)
kibo list --size

# Sort by file count (most first)
kibo list --files

# Sort by creation date (newest first)
kibo list --created
```

### `kibo rm <names...>`

Remove one or more snapshots.

```bash
# Remove single snapshot
kibo rm my-snapshot

# Remove multiple snapshots
kibo rm snapshot1 snapshot2 snapshot3
```

**Note**: When removing a snapshot that includes a database dump, the associated database dump file is automatically deleted as well.

### `kibo prune`

Remove unreferenced blobs and database dumps from storage.

```bash
kibo prune -v
```

This command scans all snapshots and removes:
- **Blob files** in `.kibo/store/` that are not referenced by any snapshot
- **Database dump files** in `.kibo/db_snapshots/` that are not referenced by any snapshot


### `kibo history`

View command history log.

```bash
# Show all history
kibo history

# Show last N entries
kibo history --last 10

# Filter by snapshot name
kibo history --snapshot my-snapshot

# Output as JSON
kibo history --json
```

### `kibo init`

Initialize a new `.kibo.toml` configuration file.

```bash
kibo init
```

### `kibo config`

Open `.kibo.toml` in your editor (uses `vim` by default).

```bash
kibo config
```

---

## Architecture

### Content-Addressed Storage

Kibo uses a content-addressed blob store (`.kibo/store/`) where each file is stored by its BLAKE3 hash. This enables:
- **Automatic deduplication**: Identical files are stored only once
- **Integrity verification**: Corruption is detected on load
- **Efficient storage**: Only unique content consumes disk space

### Snapshot Manifests

Each snapshot is stored as a JSON manifest (`.kibo/manifests/<name>.json`) containing:
- Tracked directory and file patterns
- Complete directory structure with metadata (permissions, mtimes)
- File entries with hashes, sizes, permissions, and mtimes
- Snapshot metadata (creation time, version)
- Optional database dump filename (if `--include-db` was used)

### Database Storage

When `--include-db` is used:
- MySQL databases are dumped using `mysqldump` with consistent snapshot options
- Dumps are stored in `.kibo/db_snapshots/` with auto-generated names: `<snapshot>-<dbname>-<timestamp>.sql`
- Each snapshot's manifest tracks its associated database dump file
- Old database dumps are automatically cleaned up when snapshots are overwritten or removed
- The `kibo prune` command removes database dumps that are not referenced by any existing snapshots

---

## Use Cases

### Branch Switching with Expensive Builds

```bash
# Save current branch build artifacts
kibo save feature-branch-build

# Switch branches
git checkout main
kibo load main-build

# Switch back
git checkout feature-branch
kibo load feature-branch-build
```

### Configuration Testing

```bash
# Test different build configurations
kibo save config-debug
kibo save config-release
kibo save config-profile

# Switch between them instantly
kibo load config-release
```

### CI/Build Artifact Caching

```bash
# Save artifacts after expensive build
./run-long-build.sh
kibo save ci-build-$COMMIT_SHA

# Restore on another machine or run
kibo load ci-build-$COMMIT_SHA
```

### Clean State Management

```bash
# Save empty clean state
# (delete build dirs, then save)
rm -rf build target
kibo save clean

# Restore clean state anytime
kibo load clean
```

### Database Snapshot Workflows

```bash
# Development workflow with database state
kibo save dev-state --include-db

# Test with different database states
kibo save test-empty-db --include-db=test_db
kibo save test-populated-db --include-db=test_db

# Switch between database states instantly
kibo load test-empty-db --include-db
./run-tests.sh
kibo load test-populated-db --include-db
./run-integration-tests.sh

# Backup before risky operations
kibo save pre-migration --include-db
./run-migration.sh
# If something goes wrong:
kibo load pre-migration --include-db
```

### Combined Artifacts and Database

```bash
# Save complete application state (build + database)
kibo save release-candidate --include-db -v

# Restore everything at once
kibo load release-candidate --include-db

# This restores:
# - All build artifacts
# - Complete database state
# - File permissions and timestamps
```

---

## Performance

**Benchmark example** (850 MB of build artifacts, 27300 files):
- First save: ~12s
- Subsequent save (no changes): ~3.8s
- Load (from clean directory): ~3.2s
- Load (few changes): ~1.3s
- Remove snapshot: ~2.6s

---

## Contributing

Contributions are welcome! Please:

1. Fork the repository
2. Create a feature branch (`git checkout -b feature/amazing-feature`)
3. Commit your changes (`git commit -m 'Add amazing feature'`)
4. Push to the branch (`git push origin feature/amazing-feature`)
5. Open a Pull Request

---

## Acknowledgments

- Built with [clap](https://github.com/clap-rs/clap) for CLI parsing
- Uses [BLAKE3](https://github.com/BLAKE3-team/BLAKE3) for fast cryptographic hashing
- Compression powered by [zstd](https://github.com/facebook/zstd)
- Parallel processing via [rayon](https://github.com/rayon-rs/rayon)

---

## Contact

Issues and questions: [GitHub Issues](https://github.com/wcuny/kibo/issues)
