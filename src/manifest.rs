use anyhow::{Context, Result, bail};
use chrono::{DateTime, Utc};
use glob;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::fs;
use std::path::{Path, PathBuf};

use crate::config::get_manifests_dir;
use crate::fs_utils;

/// File entry in a manifest
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct FileEntry {
    /// Blake3 hash of the file content
    pub hash: String,
    
    /// File size in bytes
    pub size: u64,
    
    /// Unix file permissions
    #[cfg(unix)]
    pub mode: u32,
    
    /// Whether this entry is a symlink
    #[serde(default)]
    pub is_symlink: bool,
    
    /// Symlink target if is_symlink is true
    #[serde(skip_serializing_if = "Option::is_none")]
    pub symlink_target: Option<String>,
    
    /// Modification time in seconds since Unix epoch
    #[serde(default)]
    pub mtime_secs: i64,
    
    /// Modification time nanoseconds component
    #[serde(default)]
    pub mtime_nanos: u32,
}

/// Directory entry in a manifest
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct DirectoryEntry {
    /// Unix file permissions
    #[cfg(unix)]
    pub mode: u32,
    
    /// Modification time in seconds since Unix epoch
    pub mtime_secs: i64,
    
    /// Modification time nanoseconds component
    pub mtime_nanos: u32,
}

/// Snapshot manifest containing all metadata
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Manifest {
    /// Snapshot name
    pub name: String,
    
    /// When the snapshot was created
    pub created_at: DateTime<Utc>,
    
    /// List of tracked directories
    #[serde(default)]
    pub tracked_directories: Vec<String>,
    
    /// List of tracked file patterns
    #[serde(default)]
    pub tracked_files: Vec<String>,
    
    /// List of ignore patterns that were active during snapshot creation
    #[serde(default)]
    pub ignored_patterns: Vec<String>,
    
    /// Mapping of relative paths to directory entries
    #[serde(default)]
    pub directories: HashMap<String, DirectoryEntry>,
    
    /// Mapping of relative paths to file entries
    pub files: HashMap<String, FileEntry>,
    
    /// Total size of all files in bytes
    pub total_size: u64,
    
    /// Number of files in the snapshot
    pub file_count: usize,
    
    /// Optional toolchain/environment info
    #[serde(skip_serializing_if = "Option::is_none")]
    pub toolchain_info: Option<String>,
    
    /// Version of kibo that created this manifest
    pub kibo_version: String,
    
    /// Optional database dump filename for this snapshot
    #[serde(skip_serializing_if = "Option::is_none")]
    pub db_dump_filename: Option<String>,
}

impl Manifest {
    /// Create a new empty manifest
    pub fn new(name: String) -> Self {
        Self {
            name,
            created_at: Utc::now(),
            tracked_directories: Vec::new(),
            tracked_files: Vec::new(),
            ignored_patterns: Vec::new(),
            directories: HashMap::new(),
            files: HashMap::new(),
            total_size: 0,
            file_count: 0,
            toolchain_info: None,
            kibo_version: env!("CARGO_PKG_VERSION").to_string(),
            db_dump_filename: None,
        }
    }
    
    /// Set tracked directories and files
    pub fn set_tracked_paths(&mut self, directories: Vec<String>, files: Vec<String>) {
        self.tracked_directories = directories;
        self.tracked_files = files;
    }
    
    /// Set ignored patterns from the config
    pub fn set_ignored_patterns(&mut self, ignored_patterns: Vec<String>) {
        self.ignored_patterns = ignored_patterns;
    }

    /// Add a directory entry to the manifest
    pub fn add_directory(&mut self, relative_path: String, entry: DirectoryEntry) {
        self.directories.insert(relative_path, entry);
    }

    /// Add a file entry to the manifest
    pub fn add_file(&mut self, relative_path: String, entry: FileEntry) {
        self.total_size += entry.size;
        self.file_count += 1;
        self.files.insert(relative_path, entry);
    }

    /// Get the manifest file path for a given snapshot name
    pub fn get_path(root: &Path, name: &str) -> PathBuf {
        get_manifests_dir(root).join(format!("{}.json", name))
    }

    /// Load a manifest from disk
    pub fn load(root: &Path, name: &str) -> Result<Self> {
        let manifest_path = Self::get_path(root, name);
        
        if !manifest_path.exists() {
            bail!("Snapshot '{}' not found", name);
        }

        let content = fs::read_to_string(&manifest_path)
            .with_context(|| format!("Failed to read manifest: {}", manifest_path.display()))?;

        let manifest: Manifest = serde_json::from_str(&content)
            .with_context(|| format!("Failed to parse manifest: {}", manifest_path.display()))?;

        Ok(manifest)
    }

    /// Save the manifest to disk atomically
    pub fn save(&self, root: &Path) -> Result<()> {
        let manifests_dir = get_manifests_dir(root);
        fs::create_dir_all(&manifests_dir)
            .with_context(|| format!("Failed to create manifests directory: {}", manifests_dir.display()))?;

        let manifest_path = Self::get_path(root, &self.name);
        let content = serde_json::to_string_pretty(self)
            .context("Failed to serialize manifest")?;

        fs_utils::atomic_write(&manifest_path, content.as_bytes())
            .with_context(|| format!("Failed to write manifest: {}", manifest_path.display()))?;

        Ok(())
    }

    /// Delete the manifest file
    pub fn delete(root: &Path, name: &str) -> Result<()> {
        let manifest_path = Self::get_path(root, name);
        
        if manifest_path.exists() {
            fs::remove_file(&manifest_path)
                .with_context(|| format!("Failed to delete manifest: {}", manifest_path.display()))?;
        }

        Ok(())
    }

    /// Check if a snapshot exists
    pub fn exists(root: &Path, name: &str) -> bool {
        Self::get_path(root, name).exists()
    }

    /// Get human-readable size
    pub fn human_size(&self) -> String {
        format_size(self.total_size)
    }
    
    /// Check if a path should be ignored based on manifest's ignore patterns
    pub fn should_ignore(&self, relative_path: &Path) -> bool {
        let path_str = relative_path.to_string_lossy();
        
        for pattern in &self.ignored_patterns {
            // Try glob pattern matching
            if let Ok(glob_pattern) = glob::Pattern::new(pattern) {
                if glob_pattern.matches(&path_str) {
                    return true;
                }
            }
            
            // Try prefix matching
            if path_str.starts_with(pattern) {
                return true;
            }
            
            // Try component matching
            for component in relative_path.components() {
                if let std::path::Component::Normal(c) = component {
                    if c.to_string_lossy() == *pattern {
                        return true;
                    }
                }
            }
        }
        
        false
    }
}

/// List all available snapshots
pub fn list_snapshots(root: &Path) -> Result<Vec<Manifest>> {
    let manifests_dir = get_manifests_dir(root);
    
    if !manifests_dir.exists() {
        return Ok(Vec::new());
    }

    let mut snapshots = Vec::new();

    for entry in fs::read_dir(&manifests_dir)
        .with_context(|| format!("Failed to read manifests directory: {}", manifests_dir.display()))?
    {
        let entry = entry?;
        let path = entry.path();
        
        if path.extension().map_or(false, |ext| ext == "json") {
            if let Some(name) = path.file_stem().and_then(|s| s.to_str()) {
                match Manifest::load(root, name) {
                    Ok(manifest) => snapshots.push(manifest),
                    Err(e) => {
                        eprintln!("Warning: Failed to load manifest '{}': {}", name, e);
                    }
                }
            }
        }
    }

    snapshots.sort_by(|a, b| b.created_at.cmp(&a.created_at));

    Ok(snapshots)
}

/// Format a byte size into human-readable format
pub fn format_size(bytes: u64) -> String {
    const KB: u64 = 1024;
    const MB: u64 = KB * 1024;
    const GB: u64 = MB * 1024;
    const TB: u64 = GB * 1024;

    if bytes >= TB {
        format!("{:.2} TB", bytes as f64 / TB as f64)
    }
    else if bytes >= GB {
        format!("{:.2} GB", bytes as f64 / GB as f64)
    }
    else if bytes >= MB {
        format!("{:.2} MB", bytes as f64 / MB as f64)
    }
    else if bytes >= KB {
        format!("{:.2} KB", bytes as f64 / KB as f64)
    }
    else {
        format!("{} B", bytes)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    #[test]
    fn test_manifest_new() {
        let manifest = Manifest::new("test_snapshot".to_string());
        
        assert_eq!(manifest.name, "test_snapshot");
        assert!(manifest.files.is_empty());
        assert!(manifest.directories.is_empty());
        assert_eq!(manifest.total_size, 0);
        assert_eq!(manifest.file_count, 0);
        assert_eq!(manifest.kibo_version, env!("CARGO_PKG_VERSION"));
        assert!(manifest.db_dump_filename.is_none());
    }

    #[test]
    fn test_manifest_set_tracked_paths() {
        let mut manifest = Manifest::new("test".to_string());
        
        manifest.set_tracked_paths(
            vec!["src".to_string(), "tests".to_string()],
            vec!["*.txt".to_string()],
        );
        
        assert_eq!(manifest.tracked_directories, vec!["src", "tests"]);
        assert_eq!(manifest.tracked_files, vec!["*.txt"]);
    }

    #[test]
    fn test_manifest_set_ignored_patterns() {
        let mut manifest = Manifest::new("test".to_string());
        
        manifest.set_ignored_patterns(
            vec!["*.log".to_string(), "tmp/".to_string(), "node_modules".to_string()],
        );
        
        assert_eq!(manifest.ignored_patterns, vec!["*.log", "tmp/", "node_modules"]);
    }

    #[test]
    fn test_manifest_should_ignore_glob_pattern() {
        let mut manifest = Manifest::new("test".to_string());
        manifest.set_ignored_patterns(vec!["*.log".to_string(), "*.tmp".to_string()]);
        
        assert!(manifest.should_ignore(Path::new("debug.log")));
        assert!(manifest.should_ignore(Path::new("test.tmp")));
        assert!(!manifest.should_ignore(Path::new("test.txt")));
    }

    #[test]
    fn test_manifest_should_ignore_prefix_pattern() {
        let mut manifest = Manifest::new("test".to_string());
        manifest.set_ignored_patterns(vec!["tmp/".to_string(), "build/".to_string()]);
        
        assert!(manifest.should_ignore(Path::new("tmp/file.txt")));
        assert!(manifest.should_ignore(Path::new("build/output.o")));
        assert!(!manifest.should_ignore(Path::new("src/main.rs")));
    }

    #[test]
    fn test_manifest_should_ignore_component_pattern() {
        let mut manifest = Manifest::new("test".to_string());
        manifest.set_ignored_patterns(vec!["node_modules".to_string(), ".git".to_string()]);
        
        assert!(manifest.should_ignore(Path::new("node_modules/package/index.js")));
        assert!(manifest.should_ignore(Path::new("src/node_modules/lib.js")));
        assert!(manifest.should_ignore(Path::new(".git/config")));
        assert!(!manifest.should_ignore(Path::new("src/index.js")));
    }

    #[test]
    fn test_manifest_add_directory() {
        let mut manifest = Manifest::new("test".to_string());
        
        let dir_entry = DirectoryEntry {
            #[cfg(unix)]
            mode: 0o755,
            mtime_secs: 1234567890,
            mtime_nanos: 0,
        };
        
        manifest.add_directory("src".to_string(), dir_entry.clone());
        
        assert_eq!(manifest.directories.len(), 1);
        assert_eq!(manifest.directories.get("src"), Some(&dir_entry));
    }

    #[test]
    fn test_manifest_add_file() {
        let mut manifest = Manifest::new("test".to_string());
        
        let file_entry = FileEntry {
            hash: "abc123".to_string(),
            size: 1024,
            #[cfg(unix)]
            mode: 0o644,
            is_symlink: false,
            symlink_target: None,
            mtime_secs: 1234567890,
            mtime_nanos: 0,
        };
        
        manifest.add_file("test.txt".to_string(), file_entry.clone());
        
        assert_eq!(manifest.files.len(), 1);
        assert_eq!(manifest.file_count, 1);
        assert_eq!(manifest.total_size, 1024);
        assert_eq!(manifest.files.get("test.txt"), Some(&file_entry));
    }

    #[test]
    fn test_manifest_add_multiple_files() {
        let mut manifest = Manifest::new("test".to_string());
        
        for i in 0..5 {
            let file_entry = FileEntry {
                hash: format!("hash{}", i),
                size: 100,
                #[cfg(unix)]
                mode: 0o644,
                is_symlink: false,
                symlink_target: None,
                mtime_secs: 1234567890,
                mtime_nanos: 0,
            };
            
            manifest.add_file(format!("file{}.txt", i), file_entry);
        }
        
        assert_eq!(manifest.file_count, 5);
        assert_eq!(manifest.total_size, 500);
    }

    #[test]
    fn test_manifest_save_and_load() {
        let temp_dir = TempDir::new().unwrap();
        let root = temp_dir.path();
        
        let mut manifest = Manifest::new("test_snapshot".to_string());
        manifest.add_file(
            "test.txt".to_string(),
            FileEntry {
                hash: "abc123".to_string(),
                size: 1024,
                #[cfg(unix)]
                mode: 0o644,
                is_symlink: false,
                symlink_target: None,
                mtime_secs: 1234567890,
                mtime_nanos: 0,
            },
        );
        
        manifest.save(root).unwrap();
        
        let loaded = Manifest::load(root, "test_snapshot").unwrap();
        assert_eq!(loaded.name, "test_snapshot");
        assert_eq!(loaded.file_count, 1);
        assert_eq!(loaded.total_size, 1024);
    }

    #[test]
    fn test_manifest_load_nonexistent() {
        let temp_dir = TempDir::new().unwrap();
        let root = temp_dir.path();
        
        let result = Manifest::load(root, "nonexistent");
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("not found"));
    }

    #[test]
    fn test_manifest_exists() {
        let temp_dir = TempDir::new().unwrap();
        let root = temp_dir.path();
        
        assert!(!Manifest::exists(root, "test"));
        
        let manifest = Manifest::new("test".to_string());
        manifest.save(root).unwrap();
        
        assert!(Manifest::exists(root, "test"));
    }

    #[test]
    fn test_manifest_delete() {
        let temp_dir = TempDir::new().unwrap();
        let root = temp_dir.path();
        
        let manifest = Manifest::new("test".to_string());
        manifest.save(root).unwrap();
        
        assert!(Manifest::exists(root, "test"));
        
        Manifest::delete(root, "test").unwrap();
        
        assert!(!Manifest::exists(root, "test"));
    }

    #[test]
    fn test_manifest_delete_nonexistent() {
        let temp_dir = TempDir::new().unwrap();
        let root = temp_dir.path();
        
        let result = Manifest::delete(root, "nonexistent");
        assert!(result.is_ok());
    }

    #[test]
    fn test_manifest_get_path() {
        let root = Path::new("/project");
        let path = Manifest::get_path(root, "snapshot1");
        
        assert!(path.ends_with(".kibo/manifests/snapshot1.json"));
    }

    #[test]
    fn test_manifest_human_size() {
        let manifest = Manifest {
            name: "test".to_string(),
            created_at: Utc::now(),
            tracked_directories: vec![],
            tracked_files: vec![],
            ignored_patterns: vec![],
            directories: HashMap::new(),
            files: HashMap::new(),
            total_size: 1024 * 1024, // 1 MB
            file_count: 10,
            toolchain_info: None,
            kibo_version: "1.0.0".to_string(),
            db_dump_filename: None,
        };
        
        assert_eq!(manifest.human_size(), "1.00 MB");
    }

    #[test]
    fn test_list_snapshots_empty() {
        let temp_dir = TempDir::new().unwrap();
        let root = temp_dir.path();
        
        let snapshots = list_snapshots(root).unwrap();
        assert!(snapshots.is_empty());
    }

    #[test]
    fn test_list_snapshots_with_manifests() {
        let temp_dir = TempDir::new().unwrap();
        let root = temp_dir.path();
        
        let m1 = Manifest::new("snapshot1".to_string());
        let m2 = Manifest::new("snapshot2".to_string());
        let m3 = Manifest::new("snapshot3".to_string());
        
        m1.save(root).unwrap();
        m2.save(root).unwrap();
        m3.save(root).unwrap();
        
        let snapshots = list_snapshots(root).unwrap();
        assert_eq!(snapshots.len(), 3);
    }

    #[test]
    fn test_list_snapshots_sorted_by_date() {
        let temp_dir = TempDir::new().unwrap();
        let root = temp_dir.path();
        
        let mut m1 = Manifest::new("old".to_string());
        m1.created_at = DateTime::parse_from_rfc3339("2020-01-01T00:00:00Z")
            .unwrap()
            .with_timezone(&Utc);
        
        let mut m2 = Manifest::new("new".to_string());
        m2.created_at = DateTime::parse_from_rfc3339("2026-01-01T00:00:00Z")
            .unwrap()
            .with_timezone(&Utc);
        
        m1.save(root).unwrap();
        m2.save(root).unwrap();
        
        let snapshots = list_snapshots(root).unwrap();
        assert_eq!(snapshots[0].name, "new");
        assert_eq!(snapshots[1].name, "old");
    }

    #[test]
    fn test_format_size_bytes() {
        assert_eq!(format_size(100), "100 B");
        assert_eq!(format_size(1023), "1023 B");
    }

    #[test]
    fn test_format_size_kilobytes() {
        assert_eq!(format_size(1024), "1.00 KB");
        assert_eq!(format_size(2048), "2.00 KB");
        assert_eq!(format_size(1536), "1.50 KB");
    }

    #[test]
    fn test_format_size_megabytes() {
        assert_eq!(format_size(1024 * 1024), "1.00 MB");
        assert_eq!(format_size(1024 * 1024 * 5), "5.00 MB");
    }

    #[test]
    fn test_format_size_gigabytes() {
        assert_eq!(format_size(1024 * 1024 * 1024), "1.00 GB");
        assert_eq!(format_size(1024 * 1024 * 1024 * 3), "3.00 GB");
    }

    #[test]
    fn test_format_size_terabytes() {
        assert_eq!(format_size(1024u64 * 1024 * 1024 * 1024), "1.00 TB");
        assert_eq!(format_size(1024u64 * 1024 * 1024 * 1024 * 2), "2.00 TB");
    }

    #[test]
    fn test_file_entry_symlink() {
        let entry = FileEntry {
            hash: "abc123".to_string(),
            size: 0,
            #[cfg(unix)]
            mode: 0o777,
            is_symlink: true,
            symlink_target: Some("/path/to/target".to_string()),
            mtime_secs: 1234567890,
            mtime_nanos: 0,
        };
        
        assert!(entry.is_symlink);
        assert_eq!(entry.symlink_target, Some("/path/to/target".to_string()));
    }

    #[test]
    fn test_directory_entry_creation() {
        let entry = DirectoryEntry {
            #[cfg(unix)]
            mode: 0o755,
            mtime_secs: 1234567890,
            mtime_nanos: 123456789,
        };
        
        #[cfg(unix)]
        assert_eq!(entry.mode, 0o755);
        assert_eq!(entry.mtime_secs, 1234567890);
        assert_eq!(entry.mtime_nanos, 123456789);
    }

    #[test]
    fn test_manifest_with_db_dump() {
        let mut manifest = Manifest::new("test".to_string());
        manifest.db_dump_filename = Some("snapshot-mydb-20260101.sql".to_string());
        
        assert_eq!(
            manifest.db_dump_filename,
            Some("snapshot-mydb-20260101.sql".to_string())
        );
    }
}
