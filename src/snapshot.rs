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
                let name = e.file_name().to_string_lossy();
                if name.starts_with('.') && name != "." {
                    return false;
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

            if config.should_ignore(Path::new(&relative_path)) {
                continue;
            }

            let canonical_path = path.canonicalize().unwrap_or_else(|_| path.to_path_buf());
            if seen_paths.insert(canonical_path) {
                files.push((relative_path, path.to_path_buf()));
            }
        }
    }

    for pattern in &config.files {
        let full_pattern = if pattern.contains("**") {
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

                    let canonical_path = entry.canonicalize().unwrap_or_else(|_| entry.clone());
                    if seen_paths.insert(canonical_path) {
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
                let name = e.file_name().to_string_lossy();
                if name.starts_with('.') && name != "." {
                    return false;
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

            let canonical_path = path.canonicalize().unwrap_or_else(|_| path.to_path_buf());
            if seen_paths.insert(canonical_path) {
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

    // Note: collect_directories requires integration testing with proper workspace structure
}
