use anyhow::{Context, Result};
use glob::glob;
use rayon::prelude::*;
use std::collections::HashSet;
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use walkdir::WalkDir;

use crate::config::Config;
use crate::file_hash::{self, HashCache};
use crate::manifest::{FileEntry, DirectoryEntry, Manifest};
use crate::progress::{ProgressConfig, Spinner};
use crate::store::Store;

/// Result of scanning a single file
struct ScanResult {
    relative_path: String,
    absolute_path: PathBuf,
    entry: FileEntry,
}

/// Create a snapshot of the tracked directories
pub fn create_snapshot(
    root: &Path,
    name: &str,
    config: &Config,
    verbose: bool,
    progress_config: ProgressConfig,
) -> Result<Manifest> {
    let effective_level = config.effective_compression_level();
    let store = Store::with_compression(root, effective_level);
    store.init()?;

    if verbose && effective_level > 0 {
        eprintln!("Using compression level: {}", effective_level);
    }

    let spinner = Spinner::new(progress_config, &format!("Preparing snapshot '{}'", name));

    let hash_cache = HashCache::load(root).unwrap_or_else(|_| HashCache::new());

    let files_to_process = collect_files(root, config, verbose)?;

    let file_count = files_to_process.len();
    
    let directories_to_save = collect_directories(root, config, verbose)?;
    let dir_count = directories_to_save.len();

    spinner.finish();
    
    eprintln!("Found {} files and {} directories to snapshot", file_count, dir_count);
    
    let spinner = Spinner::new(progress_config, &format!("Processing {} files", file_count));

    let hash_cache = Arc::new(Mutex::new(hash_cache));

    let results: Vec<Result<ScanResult>> = files_to_process
        .into_par_iter()
        .map(|(relative_path, absolute_path)| {
            process_file(&absolute_path, &relative_path, hash_cache.clone())
        })
        .collect();

    let mut scan_results = Vec::new();
    for result in results {
        match result {
            Ok(scan_result) => scan_results.push(scan_result),
            Err(e) => {
                eprintln!("Warning: Failed to process file: {}", e);
            }
        }
    }

    let mut manifest = Manifest::new(name.to_string());
    
    manifest.set_tracked_paths(
        config.directories.clone(),
        config.files.clone(),
    );
    
    manifest.set_ignored_patterns(config.ignore.clone());
    
    for (relative_path, entry) in directories_to_save {
        manifest.add_directory(relative_path, entry);
    }

    let mut new_blobs = 0usize;
    let mut reused_blobs = 0usize;

    for scan_result in scan_results {
        if scan_result.entry.is_symlink {
            if let Some(ref target) = scan_result.entry.symlink_target {
                let was_new = store.store_symlink(Path::new(target), &scan_result.entry.hash)?;
                if was_new {
                    new_blobs += 1;
                }
                else {
                    reused_blobs += 1;
                }
            }
        }
        else {
            let was_new = store.store_file(&scan_result.absolute_path, &scan_result.entry.hash)?;
            if was_new {
                new_blobs += 1;
            }
            else {
                reused_blobs += 1;
            }
        }

        manifest.add_file(scan_result.relative_path, scan_result.entry);
    }

    // Note: manifest is not saved here - caller must save it after setting optional fields
    // like db_dump_filename

    let hash_cache = Arc::try_unwrap(hash_cache)
        .expect("Hash cache still has references")
        .into_inner()
        .expect("Hash cache mutex poisoned");
    hash_cache.save(root)?;

    spinner.finish();

    if verbose {
        eprintln!(
            "Snapshot '{}' created: {} files, {} total",
            name,
            manifest.file_count,
            manifest.human_size()
        );
        eprintln!("  New blobs: {}, Reused: {}", new_blobs, reused_blobs);
    }

    if let Some(max_size_gb) = config.max_snapshot_size_gb {
        let size_gb = manifest.total_size as f64 / (1024.0 * 1024.0 * 1024.0);
        if size_gb > max_size_gb {
            eprintln!(
                "Warning: Snapshot size ({}) exceeds configured maximum ({:.2} GB)",
                manifest.human_size(),
                max_size_gb
            );
        }
    }

    Ok(manifest)
}

/// Collect all files from tracked directories and file patterns
fn collect_files(
    root: &Path,
    config: &Config,
    verbose: bool,
) -> Result<Vec<(String, PathBuf)>> {
    let mut files = Vec::new();
    let mut found_dirs = Vec::new();
    let mut seen_paths: HashSet<PathBuf> = HashSet::new();

    for tracked_dir_name in &config.directories {
        for entry in WalkDir::new(root)
            .follow_links(false)
            .into_iter()
            .filter_entry(|e| {
                if e.file_name() == ".kibo" {
                    return false;
                }

                if let Ok(rel_path) = e.path().strip_prefix(root) {
                    if config.should_ignore(rel_path) {
                        return false;
                    }
                }
                true
            })
            .filter_map(|e| e.ok())
        {
            let path = entry.path();
            
            if entry.file_type().is_dir() {
                if let Some(dir_name) = path.file_name() {
                    if dir_name == tracked_dir_name.as_str() {
                        found_dirs.push(path.to_path_buf());
                        if verbose {
                            let rel_path = path.strip_prefix(root).unwrap_or(path);
                            eprintln!("Found tracked directory: {}", rel_path.display());
                        }
                    }
                }
            }
        }
    }

    if found_dirs.is_empty() && config.files.is_empty() && verbose {
        eprintln!("Warning: No directories matching tracked names found");
    }

    for dir_path in found_dirs {
        for entry in WalkDir::new(&dir_path)
            .follow_links(false)
            .into_iter()
            .filter_entry(|e| {
                if let Ok(rel_path) = e.path().strip_prefix(root) {
                    if config.should_ignore(rel_path) {
                        return false;
                    }
                }
                true
            })
            .filter_map(|e| e.ok())
        {
            let path = entry.path();
            
            if entry.file_type().is_dir() {
                continue;
            }

            let relative_path = path
                .strip_prefix(root)
                .with_context(|| format!("Failed to get relative path: {}", path.display()))?
                .to_string_lossy()
                .to_string();

            // Use the actual path for deduplication to preserve symlinks
            // Don't use canonicalize() as it resolves symlinks to their target
            if seen_paths.insert(path.to_path_buf()) {
                files.push((relative_path, path.to_path_buf()));
            }
        }
    }

    for pattern in &config.files {
        let full_pattern = if pattern.starts_with("./") {
            let pattern_without_prefix = &pattern[2..];
            format!("{}/{}", root.display(), pattern_without_prefix)
        }
        else if pattern.contains("**") {
            if pattern.starts_with('/') {
                format!("{}{}", root.display(), pattern)
            }
            else {
                format!("{}/{}", root.display(), pattern)
            }
        }
        else {
            if pattern.starts_with('/') {
                format!("{}/**{}", root.display(), pattern)
            }
            else {
                format!("{}/**/{}", root.display(), pattern)
            }
        };

        if verbose {
            eprintln!("Searching for files matching: {} -> {}", pattern, full_pattern);
        }

        match glob(&full_pattern) {
            Ok(paths) => {
                for entry in paths.filter_map(Result::ok) {
                    if entry.is_dir() {
                        continue;
                    }

                    if entry.starts_with(root.join(".kibo")) {
                        continue;
                    }

                    let relative_path = match entry.strip_prefix(root) {
                        Ok(rel) => rel.to_string_lossy().to_string(),
                        Err(_) => continue,
                    };

                    if config.should_ignore(Path::new(&relative_path)) {
                        continue;
                    }

                    // Use the actual path for deduplication to preserve symlinks
                    // Don't use canonicalize() as it resolves symlinks to their target
                    if seen_paths.insert(entry.clone()) {
                        // if verbose {
                        //     eprintln!("  Found: {}", relative_path);
                        // }
                        files.push((relative_path, entry));
                    }
                }
            }
            Err(e) => {
                eprintln!("Warning: Invalid glob pattern '{}': {}", pattern, e);
            }
        }
    }

    Ok(files)
}

/// Collect all directories from tracked directory patterns
fn collect_directories(
    root: &Path,
    config: &Config,
    verbose: bool,
) -> Result<Vec<(String, DirectoryEntry)>> {
    let mut directories = Vec::new();
    let mut seen_paths: HashSet<PathBuf> = HashSet::new();
    let mut found_dir_roots = Vec::new();

    for tracked_dir_name in &config.directories {
        for entry in WalkDir::new(root)
            .follow_links(false)
            .into_iter()
            .filter_entry(|e| {
                if e.file_name() == ".kibo" {
                    return false;
                }

                if let Ok(rel_path) = e.path().strip_prefix(root) {
                    if config.should_ignore(rel_path) {
                        return false;
                    }
                }
                true
            })
            .filter_map(|e| e.ok())
        {
            let path = entry.path();
            
            if entry.file_type().is_dir() {
                if let Some(dir_name) = path.file_name() {
                    if dir_name == tracked_dir_name.as_str() {
                        found_dir_roots.push(path.to_path_buf());
                        if verbose {
                            let rel_path = path.strip_prefix(root).unwrap_or(path);
                            eprintln!("Found tracked directory: {}", rel_path.display());
                        }
                    }
                }
            }
        }
    }

    for dir_path in found_dir_roots {
        for entry in WalkDir::new(&dir_path)
            .follow_links(false)
            .into_iter()
            .filter_map(|e| e.ok())
        {
            let path = entry.path();
            
            if !entry.file_type().is_dir() {
                continue;
            }

            let relative_path = path
                .strip_prefix(root)
                .with_context(|| format!("Failed to get relative path: {}", path.display()))?
                .to_string_lossy()
                .to_string();

            if config.should_ignore(Path::new(&relative_path)) {
                continue;
            }

            // Use the actual path for deduplication to preserve symlinks
            // Don't use canonicalize() as it resolves symlinks to their target
            if seen_paths.insert(path.to_path_buf()) {
                let metadata = fs::metadata(path)?;
                
                let mtime = metadata.modified()?;
                let (mtime_secs, mtime_nanos) = {
                    use std::time::UNIX_EPOCH;
                    let duration = mtime.duration_since(UNIX_EPOCH)
                        .unwrap_or_else(|_| std::time::Duration::from_secs(0));
                    (duration.as_secs() as i64, duration.subsec_nanos())
                };
                
                #[cfg(unix)]
                let mode = {
                    use std::os::unix::fs::MetadataExt;
                    metadata.mode()
                };
                
                let entry = DirectoryEntry {
                    #[cfg(unix)]
                    mode,
                    mtime_secs,
                    mtime_nanos,
                };
                
                if verbose {
                    eprintln!("  Directory: {}", relative_path);
                }
                
                directories.push((relative_path, entry));
            }
        }
    }

    Ok(directories)
}

/// Process a single file: compute hash and build entry
fn process_file(
    absolute_path: &Path,
    relative_path: &str,
    hash_cache: Arc<Mutex<HashCache>>,
) -> Result<ScanResult> {
    let metadata = fs::symlink_metadata(absolute_path)
        .with_context(|| format!("Failed to get metadata: {}", absolute_path.display()))?;

    let is_symlink = metadata.file_type().is_symlink();

    let mtime = metadata.modified()?;
    let (mtime_secs, mtime_nanos) = {
        use std::time::UNIX_EPOCH;
        let duration = mtime.duration_since(UNIX_EPOCH)
            .unwrap_or_else(|_| std::time::Duration::from_secs(0));
        (duration.as_secs() as i64, duration.subsec_nanos())
    };

    let (hash, size, symlink_target) = if is_symlink {
        let target = fs::read_link(absolute_path)?;
        let hash = file_hash::hash_symlink(absolute_path)?;
        (hash, 0, Some(target.to_string_lossy().to_string()))
    }
    else {
        let size = metadata.len();

        let hash = {
            let cache = hash_cache.lock().expect("Hash cache mutex poisoned");
            
            if let Some(cached_hash) = cache.get(absolute_path, size, mtime) {
                cached_hash
            }
            else {
                drop(cache);
                let hash = file_hash::hash_file(absolute_path)?;
                
                let mut cache = hash_cache.lock().expect("Hash cache mutex poisoned");
                cache.insert(absolute_path, size, mtime, hash.clone());
                hash
            }
        };

        (hash, size, None)
    };

    #[cfg(unix)]
    let mode = {
        use std::os::unix::fs::MetadataExt;
        metadata.mode()
    };

    let entry = FileEntry {
        hash,
        size,
        #[cfg(unix)]
        mode,
        is_symlink,
        symlink_target,
        mtime_secs,
        mtime_nanos,
    };

    Ok(ScanResult {
        relative_path: relative_path.to_string(),
        absolute_path: absolute_path.to_path_buf(),
        entry,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs::File;
    use std::io::Write;
    use tempfile::TempDir;
    use std::sync::{Arc, Mutex};

    #[test]
    fn test_process_file_regular_file() {
        let temp_dir = TempDir::new().unwrap();
        let file_path = temp_dir.path().join("test.txt");
        File::create(&file_path).unwrap().write_all(b"Hello").unwrap();
        
        let hash_cache = Arc::new(Mutex::new(HashCache::new()));
        let result = process_file(&file_path, "test.txt", hash_cache).unwrap();
        
        assert_eq!(result.relative_path, "test.txt");
        assert_eq!(result.entry.size, 5);
        assert!(!result.entry.is_symlink);
        assert_eq!(result.entry.symlink_target, None);
        assert!(!result.entry.hash.is_empty());
    }

    #[test]
    fn test_process_file_empty_file() {
        let temp_dir = TempDir::new().unwrap();
        let file_path = temp_dir.path().join("empty.txt");
        File::create(&file_path).unwrap();
        
        let hash_cache = Arc::new(Mutex::new(HashCache::new()));
        let result = process_file(&file_path, "empty.txt", hash_cache).unwrap();
        
        assert_eq!(result.entry.size, 0);
        assert!(!result.entry.is_symlink);
    }

    #[test]
    #[cfg(unix)]
    fn test_process_file_symlink() {
        let temp_dir = TempDir::new().unwrap();
        let target_path = temp_dir.path().join("target.txt");
        let link_path = temp_dir.path().join("link.txt");
        
        File::create(&target_path).unwrap().write_all(b"Target").unwrap();
        std::os::unix::fs::symlink(&target_path, &link_path).unwrap();
        
        let hash_cache = Arc::new(Mutex::new(HashCache::new()));
        let result = process_file(&link_path, "link.txt", hash_cache).unwrap();
        
        assert!(result.entry.is_symlink);
        assert_eq!(result.entry.size, 0);
        assert!(result.entry.symlink_target.is_some());
    }

    #[test]
    fn test_process_file_uses_cache() {
        let temp_dir = TempDir::new().unwrap();
        let file_path = temp_dir.path().join("cached.txt");
        File::create(&file_path).unwrap().write_all(b"Cached").unwrap();
        
        let hash_cache = Arc::new(Mutex::new(HashCache::new()));
        
        // First call - should compute hash
        let result1 = process_file(&file_path, "cached.txt", hash_cache.clone()).unwrap();
        
        // Second call - should use cached hash
        let result2 = process_file(&file_path, "cached.txt", hash_cache.clone()).unwrap();
        
        assert_eq!(result1.entry.hash, result2.entry.hash);
        
        // Verify cache has the entry
        let cache = hash_cache.lock().unwrap();
        let metadata = fs::metadata(&file_path).unwrap();
        let cached = cache.get(&file_path, metadata.len(), metadata.modified().unwrap());
        assert!(cached.is_some());
    }

    #[test]
    fn test_process_file_nonexistent() {
        let temp_dir = TempDir::new().unwrap();
        let file_path = temp_dir.path().join("nonexistent.txt");
        
        let hash_cache = Arc::new(Mutex::new(HashCache::new()));
        let result = process_file(&file_path, "nonexistent.txt", hash_cache);
        
        assert!(result.is_err());
    }

    #[test]
    #[cfg(unix)]
    fn test_process_file_preserves_permissions() {
        use std::os::unix::fs::PermissionsExt;
        
        let temp_dir = TempDir::new().unwrap();
        let file_path = temp_dir.path().join("perms.txt");
        File::create(&file_path).unwrap();
        
        let perms = std::fs::Permissions::from_mode(0o755);
        std::fs::set_permissions(&file_path, perms).unwrap();
        
        let hash_cache = Arc::new(Mutex::new(HashCache::new()));
        let result = process_file(&file_path, "perms.txt", hash_cache).unwrap();
        
        assert_eq!(result.entry.mode & 0o777, 0o755);
    }

    #[test]
    fn test_collect_files_empty_config() {
        let temp_dir = TempDir::new().unwrap();
        let config = Config {
            directories: vec![],
            files: vec![],
            ..Default::default()
        };
        
        let files = collect_files(temp_dir.path(), &config, false).unwrap();
        assert!(files.is_empty());
    }

    #[test]
    fn test_collect_files_root_only_pattern() {
        let temp_dir = TempDir::new().unwrap();
        let root = temp_dir.path();
        
        File::create(root.join("config.txt")).unwrap().write_all(b"root config").unwrap();
        
        fs::create_dir(root.join("subdir")).unwrap();
        File::create(root.join("subdir/config.txt")).unwrap().write_all(b"subdir config").unwrap();
        
        let config = Config {
            directories: vec![],
            files: vec!["./config.txt".to_string()],
            ..Default::default()
        };
        
        let files = collect_files(root, &config, false).unwrap();
        
        assert_eq!(files.len(), 1, "Expected 1 file (root-level only), got {}", files.len());
        assert_eq!(files[0].0, "config.txt");
    }

    #[test]
    fn test_collect_files_root_only_wildcard() {
        let temp_dir = TempDir::new().unwrap();
        let root = temp_dir.path();
        
        File::create(root.join("data1.bin")).unwrap().write_all(b"data1").unwrap();
        File::create(root.join("data2.bin")).unwrap().write_all(b"data2").unwrap();
        
        fs::create_dir(root.join("nested")).unwrap();
        File::create(root.join("nested/data3.bin")).unwrap().write_all(b"data3").unwrap();
        
        let config = Config {
            directories: vec![],
            files: vec!["./*.bin".to_string()],
            ..Default::default()
        };
        
        let files = collect_files(root, &config, false).unwrap();
        
        assert_eq!(files.len(), 2, "Expected 2 files (root-level only), got {}", files.len());
        
        let file_names: Vec<String> = files.iter().map(|(name, _)| name.clone()).collect();
        assert!(file_names.contains(&"data1.bin".to_string()));
        assert!(file_names.contains(&"data2.bin".to_string()));
        assert!(!file_names.contains(&"nested/data3.bin".to_string()));
    }

    #[test]
    fn test_collect_files_root_only_subdirectory() {
        let temp_dir = TempDir::new().unwrap();
        let root = temp_dir.path();
        
        fs::create_dir(root.join("data")).unwrap();
        File::create(root.join("data/file1.txt")).unwrap().write_all(b"file1").unwrap();
        File::create(root.join("data/file2.txt")).unwrap().write_all(b"file2").unwrap();
        
        fs::create_dir_all(root.join("project/data")).unwrap();
        File::create(root.join("project/data/file3.txt")).unwrap().write_all(b"file3").unwrap();
        
        let config = Config {
            directories: vec![],
            files: vec!["./data/*.txt".to_string()],
            ..Default::default()
        };
        
        let files = collect_files(root, &config, false).unwrap();
        
        assert_eq!(files.len(), 2, "Expected 2 files (root-level data/ only), got {}", files.len());
        
        let file_names: Vec<String> = files.iter().map(|(name, _)| name.clone()).collect();
        assert!(file_names.contains(&"data/file1.txt".to_string()));
        assert!(file_names.contains(&"data/file2.txt".to_string()));
        assert!(!file_names.contains(&"project/data/file3.txt".to_string()));
    }

    #[test]
    fn test_collect_files_recursive_vs_root_only() {
        let temp_dir = TempDir::new().unwrap();
        let root = temp_dir.path();
        
        File::create(root.join("Makefile")).unwrap().write_all(b"root makefile").unwrap();
        
        fs::create_dir(root.join("sub1")).unwrap();
        File::create(root.join("sub1/Makefile")).unwrap().write_all(b"sub1 makefile").unwrap();
        fs::create_dir(root.join("sub2")).unwrap();
        File::create(root.join("sub2/Makefile")).unwrap().write_all(b"sub2 makefile").unwrap();
        
        let config_recursive = Config {
            directories: vec![],
            files: vec!["Makefile".to_string()],
            ..Default::default()
        };
        
        let files_recursive = collect_files(root, &config_recursive, false).unwrap();
        assert_eq!(files_recursive.len(), 3, "Recursive should find all 3 Makefiles");
        
        let config_root_only = Config {
            directories: vec![],
            files: vec!["./Makefile".to_string()],
            ..Default::default()
        };
        
        let files_root_only = collect_files(root, &config_root_only, false).unwrap();
        assert_eq!(files_root_only.len(), 1, "Root-only should find only 1 Makefile");
        assert_eq!(files_root_only[0].0, "Makefile");
    }

    // Note: collect_files searches the entire workspace tree for directories
    // whose NAME matches tracked directories. It requires complex integration testing.

    #[test]
    fn test_collect_directories_empty_config() {
        let temp_dir = TempDir::new().unwrap();
        let config = Config {
            directories: vec![],
            ..Default::default()
        };
        
        let dirs = collect_directories(temp_dir.path(), &config, false).unwrap();
        assert!(dirs.is_empty());
    }

    #[test]
    #[cfg(unix)]
    fn test_collect_files_preserves_multiple_symlinks_to_same_target() {
        use std::os::unix::fs::symlink;
        
        let temp_dir = TempDir::new().unwrap();
        let root = temp_dir.path();
        
        let target_file = root.join("data.bin");
        File::create(&target_file).unwrap().write_all(b"file contents").unwrap();
        
        let link1 = root.join("data-latest.bin");
        let link2 = root.join("data-v1.bin");
        let link3 = root.join("data-stable.bin");
        
        symlink("data.bin", &link1).unwrap();
        symlink("data.bin", &link2).unwrap();
        symlink("data.bin", &link3).unwrap();
        
        let config = Config {
            directories: vec![],
            files: vec![
                "data.bin".to_string(),
                "data-latest.bin".to_string(),
                "data-v1.bin".to_string(),
                "data-stable.bin".to_string(),
            ],
            ..Default::default()
        };
        
        let files = collect_files(root, &config, false).unwrap();
        
        assert_eq!(files.len(), 4, "Expected 4 files (3 symlinks + 1 target), got {}", files.len());
        
        let file_names: Vec<String> = files.iter()
            .map(|(rel_path, _)| rel_path.clone())
            .collect();
        
        assert!(file_names.contains(&"data.bin".to_string()), "Missing data.bin");
        assert!(file_names.contains(&"data-latest.bin".to_string()), "Missing data-latest.bin");
        assert!(file_names.contains(&"data-v1.bin".to_string()), "Missing data-v1.bin");
        assert!(file_names.contains(&"data-stable.bin".to_string()), "Missing data-stable.bin");
    }

    #[test]
    #[cfg(unix)]
    fn test_process_file_distinguishes_symlinks_from_target() {
        use std::os::unix::fs::symlink;
        
        let temp_dir = TempDir::new().unwrap();
        let root = temp_dir.path();
        
        let target = root.join("target.txt");
        File::create(&target).unwrap().write_all(b"content").unwrap();
        
        let link = root.join("link.txt");
        symlink("target.txt", &link).unwrap();
        
        let hash_cache = Arc::new(Mutex::new(HashCache::new()));
        
        let target_result = process_file(&target, "target.txt", hash_cache.clone()).unwrap();
        let link_result = process_file(&link, "link.txt", hash_cache).unwrap();
        
        assert!(!target_result.entry.is_symlink);
        assert_eq!(target_result.entry.symlink_target, None);
        assert_eq!(target_result.entry.size, 7); // "content" is 7 bytes
        
        assert!(link_result.entry.is_symlink);
        assert_eq!(link_result.entry.symlink_target, Some("target.txt".to_string()));
        assert_eq!(link_result.entry.size, 0);
        
        assert_ne!(target_result.entry.hash, link_result.entry.hash);
    }

    #[test]
    #[cfg(unix)]
    fn test_collect_files_does_not_deduplicate_by_canonical_path() {
        use std::os::unix::fs::symlink;
        
        let temp_dir = TempDir::new().unwrap();
        let root = temp_dir.path();
        
        let target = root.join("target.txt");
        File::create(&target).unwrap().write_all(b"test").unwrap();
        
        let link1 = root.join("link1.txt");
        let link2 = root.join("link2.txt");
        symlink("target.txt", &link1).unwrap();
        symlink("target.txt", &link2).unwrap();
        
        let config = Config {
            directories: vec![],
            files: vec!["*.txt".to_string()],
            ..Default::default()
        };
        
        let files = collect_files(root, &config, false).unwrap();
        
        assert_eq!(files.len(), 3);
        
        let file_names: Vec<String> = files.iter()
            .map(|(rel_path, _)| rel_path.clone())
            .collect();
        
        assert!(file_names.contains(&"target.txt".to_string()));
        assert!(file_names.contains(&"link1.txt".to_string()));
        assert!(file_names.contains(&"link2.txt".to_string()));
    }

    #[test]
    fn test_collect_files_dot_directory() {
        let temp_dir = TempDir::new().unwrap();
        let root = temp_dir.path();
        
        // Create a dot directory with files
        fs::create_dir(root.join(".moc")).unwrap();
        let moc_file1 = root.join(".moc/file1.o");
        let moc_file2 = root.join(".moc/file2.o");
        File::create(&moc_file1).unwrap().write_all(b"moc1").unwrap();
        File::create(&moc_file2).unwrap().write_all(b"moc2").unwrap();
        
        // Create another dot directory
        fs::create_dir(root.join(".ui")).unwrap();
        let ui_file = root.join(".ui/main.ui");
        File::create(&ui_file).unwrap().write_all(b"ui").unwrap();
        
        let config = Config {
            directories: vec![".moc".to_string(), ".ui".to_string()],
            ..Default::default()
        };
        
        let files = collect_files(root, &config, false).unwrap();
        
        assert_eq!(files.len(), 3, "Expected 3 files from dot directories");
        
        let file_names: Vec<String> = files.iter()
            .map(|(rel_path, _)| rel_path.clone())
            .collect();
        
        assert!(file_names.contains(&".moc/file1.o".to_string()), "Missing .moc/file1.o");
        assert!(file_names.contains(&".moc/file2.o".to_string()), "Missing .moc/file2.o");
        assert!(file_names.contains(&".ui/main.ui".to_string()), "Missing .ui/main.ui");
    }

    #[test]
    fn test_collect_directories_dot_directory() {
        let temp_dir = TempDir::new().unwrap();
        let root = temp_dir.path();
        
        // Create dot directories
        fs::create_dir(root.join(".cache")).unwrap();
        fs::create_dir(root.join(".config")).unwrap();
        
        // Create a nested structure with dot directory
        fs::create_dir_all(root.join("project/.build")).unwrap();
        
        let config = Config {
            directories: vec![".cache".to_string(), ".config".to_string(), ".build".to_string()],
            ..Default::default()
        };
        
        let dirs = collect_directories(root, &config, false).unwrap();
        
        assert_eq!(dirs.len(), 3, "Expected 3 dot directories");
        
        let dir_names: Vec<String> = dirs.iter()
            .map(|(rel_path, _)| rel_path.clone())
            .collect();
        
        assert!(dir_names.contains(&".cache".to_string()), "Missing .cache");
        assert!(dir_names.contains(&".config".to_string()), "Missing .config");
        assert!(dir_names.contains(&"project/.build".to_string()), "Missing project/.build");
    }

    #[test]
    fn test_collect_files_nested_in_dot_directory() {
        let temp_dir = TempDir::new().unwrap();
        let root = temp_dir.path();
        
        // Create structure: .hidden/src/code.rs
        fs::create_dir_all(root.join(".hidden/src")).unwrap();
        let code_file = root.join(".hidden/src/code.rs");
        File::create(&code_file).unwrap().write_all(b"code").unwrap();
        
        let config = Config {
            directories: vec!["src".to_string()],
            ..Default::default()
        };
        
        let files = collect_files(root, &config, false).unwrap();
        
        assert_eq!(files.len(), 1, "Expected 1 file from src inside .hidden");
        assert_eq!(files[0].0, ".hidden/src/code.rs");
    }

    // Note: collect_directories requires integration testing with proper workspace structure
}
