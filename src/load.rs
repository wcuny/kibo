use anyhow::{Context, Result, bail};
use glob;
use rayon::prelude::*;
use std::collections::{HashMap, HashSet};
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use walkdir::WalkDir;

use crate::file_hash;
use crate::fs_utils;
use crate::manifest::Manifest;
use crate::progress::{ProgressConfig, ByteProgress};
use crate::store::Store;

/// Load a snapshot to the tracked directories
pub fn load_snapshot(
    root: &Path,
    name: &str,
    verbose: bool,
    progress_config: ProgressConfig,
) -> Result<LoadStats> {
    use crate::progress::Timer;
    let timer = Timer::new();
    
    let manifest = Manifest::load(root, name)?;

    if verbose {
        eprintln!(
            "Loading snapshot '{}' ({} files, {})",
            name,
            manifest.file_count,
            manifest.human_size()
        );
    }

    let store = Store::new(root);

    let setup_spinner = if !verbose {
        Some(crate::progress::Spinner::new(progress_config, "Preparing load"))
    }
    else {
        None
    };

    if verbose {
        eprintln!("Verifying snapshot integrity");
    }
    verify_snapshot(&manifest, &store)?;

    if verbose {
        eprintln!("Tracked directories: {:?}", manifest.tracked_directories);
        eprintln!("Tracked files: {:?}", manifest.tracked_files);
    }

    cleanup_stale_files(root, &manifest, verbose)?;

    cleanup_empty_directories(root, &manifest, verbose)?;
    
    restore_directories(root, &manifest, verbose)?;

    if let Some(spinner) = setup_spinner {
        spinner.finish();
    }

    let stats = load_files(root, &manifest, &store, verbose, progress_config)?;

    let elapsed = timer.elapsed_string();
    
    if verbose {
        eprintln!(
            "Load complete: {} files ({} copied, {} unchanged, {} symlinks)",
            stats.files_loaded,
            stats.copies,
            stats.unchanged,
            stats.symlinks
        );
    }
    else {
        println!("Load completed in {}", elapsed);
    }

    Ok(stats)
}

/// Clean up stale files within tracked paths
fn cleanup_stale_files(
    root: &Path,
    manifest: &Manifest,
    verbose: bool,
) -> Result<()> {
    if verbose {
        eprintln!("Cleaning up stale files in tracked paths");
    }

    let manifest_files: HashSet<PathBuf> = manifest
        .files
        .keys()
        .map(|p| root.join(p))
        .collect();

    // Determine directories to scan by finding all directories in the workspace
    // whose name matches any of the tracked directory names. This works even when
    // the manifest is empty (saved state has no files).
    let mut directories_to_scan: HashSet<PathBuf> = find_tracked_directory_roots(root, manifest);

    // Also include any directories inferred from manifest file paths (for completeness)
    for file_path in manifest.files.keys() {
        let path = Path::new(file_path);
        for tracked_dir in &manifest.tracked_directories {
            let mut current = PathBuf::new();
            for component in path.components() {
                if let Some(comp_str) = component.as_os_str().to_str() {
                    current.push(comp_str);
                    if comp_str == tracked_dir {
                        directories_to_scan.insert(root.join(&current));
                        break;
                    }
                }
            }
        }
    }

    let mut deleted_count = 0;

    for dir_path in directories_to_scan {
        if !dir_path.exists() {
            continue;
        }

        if verbose {
            let rel_path = dir_path.strip_prefix(root).unwrap_or(&dir_path);
            eprintln!("  Checking directory: {}", rel_path.display());
        }

        for entry in WalkDir::new(&dir_path)
            .into_iter()
            .filter_map(|e| e.ok())
            .filter(|e| !e.file_type().is_dir())
        {
            let file_path = entry.path();
            
            if file_path.starts_with(root.join(".kibo")) {
                continue;
            }

            if !manifest_files.contains(file_path) {
                if verbose {
                    let rel_path = file_path.strip_prefix(root).unwrap_or(file_path);
                    eprintln!("    Deleting stale file: {}", rel_path.display());
                }
                
                fs::remove_file(file_path)
                    .with_context(|| format!("Failed to delete stale file: {}", file_path.display()))?;
                deleted_count += 1;
            }
        }
    }

    for file_pattern in &manifest.tracked_files {
        let full_pattern = if file_pattern.contains("**") {
            if file_pattern.starts_with('/') {
                format!("{}{}", root.display(), file_pattern)
            }
            else {
                format!("{}/{}", root.display(), file_pattern)
            }
        }
        else {
            if file_pattern.starts_with('/') {
                format!("{}/**{}", root.display(), file_pattern)
            }
            else {
                format!("{}/**/{}", root.display(), file_pattern)
            }
        };

        if let Ok(paths) = glob::glob(&full_pattern) {
            for entry in paths.filter_map(Result::ok) {
                if entry.is_file() && !manifest_files.contains(&entry) {
                    if entry.starts_with(root.join(".kibo")) {
                        continue;
                    }

                    if verbose {
                        let rel_path = entry.strip_prefix(root).unwrap_or(&entry);
                        eprintln!("    Deleting stale file: {}", rel_path.display());
                    }
                    
                    fs::remove_file(&entry)
                        .with_context(|| format!("Failed to delete stale file: {}", entry.display()))?;
                    deleted_count += 1;
                }
            }
        }
    }

    if verbose && deleted_count > 0 {
        eprintln!("  Deleted {} stale files", deleted_count);
    }

    Ok(())
}

/// Clean up empty directories that are not required by the snapshot
fn cleanup_empty_directories(
    root: &Path,
    manifest: &Manifest,
    verbose: bool,
) -> Result<()> {
    if verbose {
        eprintln!("Cleaning up empty directories");
    }

    let mut required_dirs: HashSet<PathBuf> = HashSet::new();
    
    for file_path in manifest.files.keys() {
        let full_path = root.join(file_path);
        let mut current = full_path.as_path();
        
        while let Some(parent) = current.parent() {
            if parent == root || !parent.starts_with(root) {
                break;
            }
            
            required_dirs.insert(parent.to_path_buf());
            current = parent;
        }
    }

    let directories_to_scan: HashSet<PathBuf> = find_tracked_directory_roots(root, manifest);

    let mut all_dirs: Vec<PathBuf> = Vec::new();
    
    for scan_root in &directories_to_scan {
        if !scan_root.exists() {
            continue;
        }
        
        for entry in WalkDir::new(&scan_root)
            .into_iter()
            .filter_map(|e| e.ok())
            .filter(|e| e.file_type().is_dir())
        {
            let dir_path = entry.path();
            
            if dir_path.starts_with(root.join(".kibo")) {
                continue;
            }
            
            all_dirs.push(dir_path.to_path_buf());
        }
    }

    all_dirs.sort_by(|a, b| {
        let depth_a = a.components().count();
        let depth_b = b.components().count();
        depth_b.cmp(&depth_a) // Reverse order, deepest first
    });

    let mut deleted_count = 0;

    for dir_path in all_dirs {
        if required_dirs.contains(&dir_path) {
            continue;
        }

        if let Ok(mut entries) = fs::read_dir(&dir_path) {
            if entries.next().is_none() {
                if verbose {
                    let rel_path = dir_path.strip_prefix(root).unwrap_or(&dir_path);
                    eprintln!("    Deleting empty directory: {}", rel_path.display());
                }
                
                if let Err(e) = fs::remove_dir(&dir_path) {
                    if verbose {
                        eprintln!("    Warning: Failed to remove directory {}: {}", dir_path.display(), e);
                    }
                } 
                else {
                    deleted_count += 1;
                }
            }
        }
    }

    if verbose && deleted_count > 0 {
        eprintln!("  Deleted {} empty directories", deleted_count);
    }

    Ok(())
}

/// Load all files from manifest
fn load_files(
    root: &Path,
    manifest: &Manifest,
    store: &Store,
    verbose: bool,
    progress_config: ProgressConfig,
) -> Result<LoadStats> {
    if verbose {
        eprintln!("Loading files from snapshot");
    }

    let existing_files = scan_existing_files_in_manifest(root, manifest, progress_config)?;

    let total_bytes = manifest.total_size;
    let progress = ByteProgress::new(total_bytes, progress_config);

    let stats = Arc::new(Mutex::new(LoadStats::default()));

    let entries: Vec<(&String, &crate::manifest::FileEntry)> = manifest
        .files
        .iter()
        .collect();

    let results: Vec<Result<()>> = entries
        .par_iter()
        .map(|(relative_path, entry)| {
            load_single_file(
                root,
                relative_path,
                entry,
                &existing_files,
                store,
                stats.clone(),
                verbose,
                &progress,
            )
        })
        .collect();

    for result in results {
        result?;
    }

    progress.finish();

    let final_stats = Arc::try_unwrap(stats)
        .expect("Stats still has references")
        .into_inner()
        .expect("Stats mutex poisoned");

    Ok(final_stats)
}

/// Scan existing files mentioned in manifest and compute their hashes
fn scan_existing_files_in_manifest(
    root: &Path,
    manifest: &Manifest,
    progress_config: ProgressConfig,
) -> Result<HashMap<String, String>> {
    let spinner = crate::progress::Spinner::new(progress_config, "Scanning existing files");

    let files: Vec<String> = manifest.files.keys().cloned().collect();

    let hashes: Vec<(String, String)> = files
        .par_iter()
        .filter_map(|relative_path| {
            let path = root.join(relative_path);
            if !path.exists() {
                return None;
            }
            let hash = file_hash::hash_file(&path).ok()?;
            Some((relative_path.clone(), hash))
        })
        .collect();

    spinner.finish();
    Ok(hashes.into_iter().collect())
}

/// Load a single file
fn load_single_file(
    root: &Path,
    relative_path: &str,
    entry: &crate::manifest::FileEntry,
    existing_files: &HashMap<String, String>,
    store: &Store,
    stats: Arc<Mutex<LoadStats>>,
    verbose: bool,
    progress: &ByteProgress,
) -> Result<()> {
    let dest_path = root.join(relative_path);

    if let Some(parent) = dest_path.parent() {
        fs::create_dir_all(parent)?;
    }

    if entry.is_symlink {
        if let Some(ref _target) = entry.symlink_target {
            if dest_path.exists() || dest_path.symlink_metadata().is_ok() {
                fs::remove_file(&dest_path)?;
            }

            let target_path = store.retrieve_symlink_target(&entry.hash)?;
            fs_utils::create_symlink(&target_path, &dest_path)?;
            
            let mut s = stats.lock().unwrap();
            s.symlinks += 1;
            s.files_loaded += 1;
            
            progress.inc(entry.size);
        }
    }
    else {
        let needs_copy = if let Some(existing_hash) = existing_files.get(relative_path) {
            if existing_hash == &entry.hash {
                if verbose {
                    eprintln!("  Unchanged: {}", relative_path);
                }
                
                let mut s = stats.lock().unwrap();
                s.unchanged += 1;
                s.files_loaded += 1;
                
                progress.inc(entry.size);
                
                false
            }
            else {
                true
            }
        }
        else {
            true
        };

        if needs_copy {
            store.copy_blob_to_file(&entry.hash, &dest_path)
                .with_context(|| format!("Failed to copy blob for: {}", relative_path))?;
            
            if verbose {
                eprintln!("  Loaded: {}", relative_path);
            }
            
            let mut s = stats.lock().unwrap();
            s.copies += 1;
            s.files_loaded += 1;
            
            progress.inc(entry.size);
        }

        #[cfg(unix)]
        {
            fs_utils::set_file_mode(&dest_path, entry.mode)?;
        }
        
        fs_utils::set_file_mtime(&dest_path, entry.mtime_secs, entry.mtime_nanos)?;
    }

    Ok(())
}

/// Restore directories from manifest with proper metadata
fn restore_directories(
    root: &Path,
    manifest: &Manifest,
    verbose: bool,
) -> Result<()> {
    if manifest.directories.is_empty() {
        return Ok(());
    }
    
    if verbose {
        eprintln!("Restoring {} directories", manifest.directories.len());
    }
    
    let mut dirs: Vec<(&String, &crate::manifest::DirectoryEntry)> = 
        manifest.directories.iter().collect();
    dirs.sort_by(|a, b| {
        let depth_a = a.0.matches('/').count();
        let depth_b = b.0.matches('/').count();
        depth_a.cmp(&depth_b)
    });
    
    for (relative_path, entry) in dirs {
        let dir_path = root.join(relative_path);
        
        if !dir_path.exists() {
            fs::create_dir_all(&dir_path)
                .with_context(|| format!("Failed to create directory: {}", dir_path.display()))?;
            
            if verbose {
                eprintln!("  Created directory: {}", relative_path);
            }
        }
        
        #[cfg(unix)]
        {
            fs_utils::set_file_mode(&dir_path, entry.mode)?;
        }
        
        fs_utils::set_file_mtime(&dir_path, entry.mtime_secs, entry.mtime_nanos)?;
    }
    
    Ok(())
}

/// Verify that all blobs in the manifest exist in the store
fn verify_snapshot(manifest: &Manifest, store: &Store) -> Result<()> {
    let mut missing = Vec::new();

    for (path, entry) in &manifest.files {
        if !store.has_blob(&entry.hash) {
            missing.push(path.clone());
        }
    }

    if !missing.is_empty() {
        let sample: Vec<_> = missing.iter().take(5).collect();
        bail!(
            "Snapshot is incomplete: {} files missing from store.\n\
             Sample: {:?}{}",
            missing.len(),
            sample,
            if missing.len() > 5 { " ..." } else { "" }
        );
    }

    Ok(())
}

/// Statistics about a load operation
#[derive(Debug, Default)]
pub struct LoadStats {
    pub files_loaded: usize,
    pub copies: usize,
    pub unchanged: usize,
    pub symlinks: usize,
}

/// Find tracked directory roots by scanning the workspace for directories whose
/// name matches any entry in `manifest.tracked_directories`. Skips the .kibo directory
/// and hidden/VCS directories to mirror snapshot collection behavior.
fn find_tracked_directory_roots(root: &Path, manifest: &Manifest) -> HashSet<PathBuf> {
    let mut found: HashSet<PathBuf> = HashSet::new();

    if manifest.tracked_directories.is_empty() {
        return found;
    }

    let tracked: HashSet<&str> = manifest
        .tracked_directories
        .iter()
        .map(|s| s.as_str())
        .collect();

    for entry in WalkDir::new(root)
        .follow_links(false)
        .into_iter()
        .filter_entry(|e| {
            if e.file_name() == ".kibo" { return false; }
            let name = e.file_name().to_string_lossy();
            if name.starts_with('.') && name != "." { return false; }
            true
        })
        .filter_map(|e| e.ok())
    {
        let path = entry.path();
        if entry.file_type().is_dir() {
            if let Some(dir_name) = path.file_name() {
                if let Some(name_str) = dir_name.to_str() {
                    if tracked.contains(name_str) {
                        found.insert(path.to_path_buf());
                    }
                }
            }
        }
    }

    found
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;
    use std::fs::File;
    use std::io::Write;
    use crate::manifest::FileEntry;

    #[test]
    fn test_load_stats_default() {
        let stats = LoadStats::default();
        assert_eq!(stats.files_loaded, 0);
        assert_eq!(stats.copies, 0);
        assert_eq!(stats.unchanged, 0);
        assert_eq!(stats.symlinks, 0);
    }

    #[test]
    fn test_verify_snapshot_all_blobs_present() {
        let temp_dir = TempDir::new().unwrap();
        let root = temp_dir.path();
        
        let store = Store::new(root);
        store.init().unwrap();
        
        // Create a test file and store it
        let test_file = root.join("test.txt");
        File::create(&test_file).unwrap().write_all(b"test").unwrap();
        
        let hash = file_hash::hash_file(&test_file).unwrap();
        store.store_file(&test_file, &hash).unwrap();
        
        // Create manifest with that file
        let mut manifest = Manifest::new("test".to_string());
        let entry = FileEntry {
            hash: hash.clone(),
            size: 4,
            #[cfg(unix)]
            mode: 0o644,
            is_symlink: false,
            symlink_target: None,
            mtime_secs: 0,
            mtime_nanos: 0,
        };
        manifest.add_file("test.txt".to_string(), entry);
        
        let result = verify_snapshot(&manifest, &store);
        assert!(result.is_ok());
    }

    #[test]
    fn test_verify_snapshot_missing_blob() {
        let temp_dir = TempDir::new().unwrap();
        let root = temp_dir.path();
        
        let store = Store::new(root);
        store.init().unwrap();
        
        // Create manifest with non-existent blob
        let mut manifest = Manifest::new("test".to_string());
        let entry = FileEntry {
            hash: "nonexistent_hash".to_string(),
            size: 4,
            #[cfg(unix)]
            mode: 0o644,
            is_symlink: false,
            symlink_target: None,
            mtime_secs: 0,
            mtime_nanos: 0,
        };
        manifest.add_file("test.txt".to_string(), entry);
        
        let result = verify_snapshot(&manifest, &store);
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("incomplete"));
    }

    #[test]
    fn test_verify_snapshot_empty_manifest() {
        let temp_dir = TempDir::new().unwrap();
        let root = temp_dir.path();
        
        let store = Store::new(root);
        store.init().unwrap();
        
        let manifest = Manifest::new("test".to_string());
        
        let result = verify_snapshot(&manifest, &store);
        assert!(result.is_ok());
    }

    #[test]
    fn test_find_tracked_directory_roots_empty() {
        let temp_dir = TempDir::new().unwrap();
        let manifest = Manifest::new("test".to_string());
        
        let found = find_tracked_directory_roots(temp_dir.path(), &manifest);
        assert!(found.is_empty());
    }

    // Note: find_tracked_directory_roots is an integration-level function
    // that scans the entire filesystem tree looking for directories by name.
    // Testing it properly requires a more complex setup with actual project structure.

    #[test]
    fn test_find_tracked_directory_roots_skips_kibo_dir() {
        let temp_dir = TempDir::new().unwrap();
        let kibo_dir = temp_dir.path().join(".kibo");
        let src_in_kibo = kibo_dir.join("src");
        std::fs::create_dir_all(&src_in_kibo).unwrap();
        
        let mut manifest = Manifest::new("test".to_string());
        manifest.set_tracked_paths(vec!["src".to_string()], vec![]);
        
        let found = find_tracked_directory_roots(temp_dir.path(), &manifest);
        // Should not find src inside .kibo
        assert!(found.is_empty());
    }
}

