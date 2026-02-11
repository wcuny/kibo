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
    dry_run: bool,
    progress_config: ProgressConfig,
) -> Result<LoadStats> {
    use crate::progress::Timer;
    let timer = Timer::new();
    
    let manifest = Manifest::load(root, name)?;

    if dry_run {
        println!("[DRY RUN] Loading snapshot '{}' ({} files, {})", name, manifest.file_count, manifest.human_size());
        println!("\nTracked directories: {:?}", manifest.tracked_directories);
        println!("Tracked file patterns: {:?}", manifest.tracked_files);
        if !manifest.ignored_patterns.is_empty() {
            println!("Ignored patterns: {:?}", manifest.ignored_patterns);
        }
    } else if verbose {
        eprintln!(
            "Loading snapshot '{}' ({} files, {})",
            name,
            manifest.file_count,
            manifest.human_size()
        );
    }

    let store = Store::new(root);

    let setup_spinner = if !verbose && !dry_run {
        Some(crate::progress::Spinner::new(progress_config, "Preparing load"))
    }
    else {
        None
    };

    if verbose || dry_run {
        if dry_run {
            println!("\n[DRY RUN] Verifying snapshot integrity");
        } else {
            eprintln!("Verifying snapshot integrity");
        }
    }
    verify_snapshot(&manifest, &store)?;

    if verbose || dry_run {
        if dry_run {
            println!("[DRY RUN] Tracked directories: {:?}", manifest.tracked_directories);
            println!("[DRY RUN] Tracked files: {:?}", manifest.tracked_files);
        } else {
            eprintln!("Tracked directories: {:?}", manifest.tracked_directories);
            eprintln!("Tracked files: {:?}", manifest.tracked_files);
        }
    }

    let stats = Arc::new(Mutex::new(LoadStats::default()));

    cleanup_stale_files(root, &manifest, verbose, dry_run, stats.clone())?;

    cleanup_empty_directories(root, &manifest, verbose, dry_run)?;
    
    restore_directories(root, &manifest, verbose, dry_run)?;

    if let Some(spinner) = setup_spinner {
        spinner.finish();
    }

    load_files(root, &manifest, &store, verbose, dry_run, progress_config, stats.clone())?;

    let stats = Arc::try_unwrap(stats)
        .expect("Stats still has references")
        .into_inner()
        .expect("Stats mutex poisoned");

    let elapsed = timer.elapsed_string();
    
    if dry_run {
        println!(
            "\n[DRY RUN] Would load: {} files ({} to copy, {} unchanged, {} symlinks, {} removed)",
            stats.files_loaded,
            stats.copies,
            stats.unchanged,
            stats.symlinks,
            stats.removed
        );
        
        if !stats.copied_files.is_empty() {
            println!("\nFiles to copy:");
            for file in &stats.copied_files {
                println!("  - {}", file);
            }
        }
        
        if !stats.unchanged_files.is_empty() {
            println!("\nFiles unchanged:");
            for file in &stats.unchanged_files {
                println!("  - {}", file);
            }
        }
        
        if !stats.symlink_files.is_empty() {
            println!("\nSymlinks to restore:");
            for file in &stats.symlink_files {
                println!("  - {}", file);
            }
        }
        
        if !stats.removed_files.is_empty() {
            println!("\nFiles to remove:");
            for file in &stats.removed_files {
                println!("  - {}", file);
            }
        }
        
        println!("\n[DRY RUN] Completed in {}", elapsed);
    } else if verbose {
        eprintln!(
            "Load complete: {} files ({} copied, {} unchanged, {} symlinks, {} removed)",
            stats.files_loaded,
            stats.copies,
            stats.unchanged,
            stats.symlinks,
            stats.removed
        );
        
        if !stats.copied_files.is_empty() {
            eprintln!("\nFiles copied:");
            for file in &stats.copied_files {
                eprintln!("  - {}", file);
            }
        }
        
        if !stats.unchanged_files.is_empty() {
            eprintln!("\nFiles unchanged:");
            for file in &stats.unchanged_files {
                eprintln!("  - {}", file);
            }
        }
        
        if !stats.symlink_files.is_empty() {
            eprintln!("\nSymlinks restored:");
            for file in &stats.symlink_files {
                eprintln!("  - {}", file);
            }
        }
        
        if !stats.removed_files.is_empty() {
            eprintln!("\nFiles removed:");
            for file in &stats.removed_files {
                eprintln!("  - {}", file);
            }
        }
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
    dry_run: bool,
    stats: Arc<Mutex<LoadStats>>,
) -> Result<()> {
    if verbose || dry_run {
        if dry_run {
            println!("\n[DRY RUN] Would clean up stale files in tracked paths");
        } else {
            eprintln!("Cleaning up stale files in tracked paths");
        }
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
            .filter_entry(|e| {
                if e.file_name() == ".kibo" {
                    return false;
                }

                if let Ok(rel_path) = e.path().strip_prefix(root) {
                    if manifest.should_ignore(rel_path) {
                        return false;
                    }
                }
                true
            })
            .filter_map(|e| e.ok())
            .filter(|e| !e.file_type().is_dir())
        {
            let file_path = entry.path();

            if !manifest_files.contains(file_path) {
                
                if verbose || dry_run {
                    let rel_path = file_path.strip_prefix(root).unwrap_or(file_path);
                    if dry_run {
                        println!("    [DRY RUN] Would delete stale file: {}", rel_path.display());
                    } else {
                        eprintln!("    Deleting stale file: {}", rel_path.display());
                    }
                }
                
                let relative_path = file_path.strip_prefix(root)
                    .unwrap_or(file_path)
                    .to_string_lossy()
                    .to_string();
                
                if !dry_run {
                    fs::remove_file(file_path)
                        .with_context(|| format!("Failed to delete stale file: {}", file_path.display()))?;
                }
                
                let mut s = stats.lock().unwrap();
                s.removed += 1;
                s.removed_files.push(relative_path);
                deleted_count += 1;
            }
        }
    }

    for file_pattern in &manifest.tracked_files {
        let full_pattern = if file_pattern.starts_with("./") {
            let pattern_without_prefix = &file_pattern[2..];
            format!("{}/{}", root.display(), pattern_without_prefix)
        }
        else if file_pattern.contains("**") {
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

                    let relative_path = entry.strip_prefix(root).unwrap_or(&entry);
                    if manifest.should_ignore(relative_path) {
                        if verbose {
                            eprintln!("    Skipping ignored file: {}", relative_path.display());
                        }
                        continue;
                    }

                    if verbose || dry_run {
                        let rel_path = entry.strip_prefix(root).unwrap_or(&entry);
                        if dry_run {
                            println!("    [DRY RUN] Would delete stale file: {}", rel_path.display());
                        } else {
                            eprintln!("    Deleting stale file: {}", rel_path.display());
                        }
                    }
                    
                    let relative_path = entry.strip_prefix(root)
                        .unwrap_or(&entry)
                        .to_string_lossy()
                        .to_string();
                    
                    if !dry_run {
                        fs::remove_file(&entry)
                            .with_context(|| format!("Failed to delete stale file: {}", entry.display()))?;
                    }
                    
                    let mut s = stats.lock().unwrap();
                    s.removed += 1;
                    s.removed_files.push(relative_path);
                    deleted_count += 1;
                }
            }
        }
    }

    if (verbose || dry_run) && deleted_count > 0 {
        if dry_run {
            println!("  [DRY RUN] Would delete {} stale files", deleted_count);
        } else {
            eprintln!("  Deleted {} stale files", deleted_count);
        }
    }

    Ok(())
}

/// Clean up empty directories that are not required by the snapshot
fn cleanup_empty_directories(
    root: &Path,
    manifest: &Manifest,
    verbose: bool,
    dry_run: bool,
) -> Result<()> {
    if verbose || dry_run {
        if dry_run {
            println!("\n[DRY RUN] Would clean up empty directories");
        } else {
            eprintln!("Cleaning up empty directories");
        }
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
                if verbose || dry_run {
                    let rel_path = dir_path.strip_prefix(root).unwrap_or(&dir_path);
                    if dry_run {
                        println!("    [DRY RUN] Would delete empty directory: {}", rel_path.display());
                    } else {
                        eprintln!("    Deleting empty directory: {}", rel_path.display());
                    }
                }
                
                if !dry_run {
                    if let Err(e) = fs::remove_dir(&dir_path) {
                        if verbose {
                            eprintln!("    Warning: Failed to remove directory {}: {}", dir_path.display(), e);
                        }
                    } 
                    else {
                        deleted_count += 1;
                    }
                } else {
                    deleted_count += 1;
                }
            }
        }
    }

    if (verbose || dry_run) && deleted_count > 0 {
        if dry_run {
            println!("  [DRY RUN] Would delete {} empty directories", deleted_count);
        } else {
            eprintln!("  Deleted {} empty directories", deleted_count);
        }
    }

    Ok(())
}

/// Load all files from manifest
fn load_files(
    root: &Path,
    manifest: &Manifest,
    store: &Store,
    verbose: bool,
    dry_run: bool,
    progress_config: ProgressConfig,
    stats: Arc<Mutex<LoadStats>>,
) -> Result<()> {
    if verbose || dry_run {
        if dry_run {
            println!("\n[DRY RUN] Would load files from snapshot");
        } else {
            eprintln!("Loading files from snapshot");
        }
    }

    let existing_files = scan_existing_files_in_manifest(root, manifest, progress_config)?;

    let total_bytes = manifest.total_size;
    let progress = ByteProgress::new(total_bytes, progress_config);

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
                dry_run,
                &progress,
            )
        })
        .collect();

    for result in results {
        result?;
    }

    progress.finish();

    Ok(())
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
            // Use symlink_metadata to check existence without following symlinks
            if path.symlink_metadata().is_err() {
                return None;
            }
            
            // Check if it's a symlink and hash accordingly
            let hash = if path.symlink_metadata().ok()?.is_symlink() {
                file_hash::hash_symlink(&path).ok()?
            } else {
                file_hash::hash_file(&path).ok()?
            };
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
    dry_run: bool,
    progress: &ByteProgress,
) -> Result<()> {
    let dest_path = root.join(relative_path);

    if !dry_run {
        if let Some(parent) = dest_path.parent() {
            fs::create_dir_all(parent)?;
        }
    }

    if entry.is_symlink {
        if let Some(ref _target) = entry.symlink_target {
            // Check if symlink already exists with correct target
            let needs_restore = if let Some(existing_hash) = existing_files.get(relative_path) {
                if existing_hash == &entry.hash {
                    if verbose || dry_run {
                        if dry_run {
                            println!("  [DRY RUN] Symlink unchanged: {}", relative_path);
                        } else {
                            eprintln!("  Symlink unchanged: {}", relative_path);
                        }
                    }
                    
                    let mut s = stats.lock().unwrap();
                    s.unchanged += 1;
                    s.files_loaded += 1;
                    s.unchanged_files.push(relative_path.to_string());
                    
                    progress.inc(entry.size);
                    
                    false
                } else {
                    true
                }
            } else {
                true
            };
            
            if needs_restore {
                if dry_run {
                    if verbose {
                        println!("  [DRY RUN] Would restore symlink: {}", relative_path);
                    }
                } else {
                    if dest_path.exists() || dest_path.symlink_metadata().is_ok() {
                        fs::remove_file(&dest_path)?;
                    }

                    let target_path = store.retrieve_symlink_target(&entry.hash)?;
                    fs_utils::create_symlink(&target_path, &dest_path)?;
                    
                    if verbose {
                        eprintln!("  Symlink restored: {}", relative_path);
                    }
                }
                
                let mut s = stats.lock().unwrap();
                s.symlinks += 1;
                s.files_loaded += 1;
                s.symlink_files.push(relative_path.to_string());
                
                progress.inc(entry.size);
            }
        }
    }
    else {
        let needs_copy = if let Some(existing_hash) = existing_files.get(relative_path) {
            if existing_hash == &entry.hash {
                if verbose || dry_run {
                    if dry_run {
                        println!("  [DRY RUN] Unchanged: {}", relative_path);
                    } else {
                        eprintln!("  Unchanged: {}", relative_path);
                    }
                }
                
                let mut s = stats.lock().unwrap();
                s.unchanged += 1;
                s.files_loaded += 1;
                s.unchanged_files.push(relative_path.to_string());
                
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
            if dry_run {
                if verbose {
                    println!("  [DRY RUN] Would load: {}", relative_path);
                }
            } else {
                store.copy_blob_to_file(&entry.hash, &dest_path)
                    .with_context(|| format!("Failed to copy blob for: {}", relative_path))?;
                
                if verbose {
                    eprintln!("  Loaded: {}", relative_path);
                }
            }
            
            let mut s = stats.lock().unwrap();
            s.copies += 1;
            s.files_loaded += 1;
            s.copied_files.push(relative_path.to_string());
            
            progress.inc(entry.size);
        }

        if !dry_run {
            #[cfg(unix)]
            {
                fs_utils::set_file_mode(&dest_path, entry.mode)?;
            }
            
            fs_utils::set_file_mtime(&dest_path, entry.mtime_secs, entry.mtime_nanos)?;
        }
    }

    Ok(())
}

/// Restore directories from manifest with proper metadata
fn restore_directories(
    root: &Path,
    manifest: &Manifest,
    verbose: bool,
    dry_run: bool,
) -> Result<()> {
    if manifest.directories.is_empty() {
        return Ok(());
    }
    
    if verbose || dry_run {
        if dry_run {
            println!("\n[DRY RUN] Would restore {} directories", manifest.directories.len());
        } else {
            eprintln!("Restoring {} directories", manifest.directories.len());
        }
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
            if dry_run {
                if verbose {
                    println!("  [DRY RUN] Would create directory: {}", relative_path);
                }
            } else {
                fs::create_dir_all(&dir_path)
                    .with_context(|| format!("Failed to create directory: {}", dir_path.display()))?;
                
                if verbose {
                    eprintln!("  Created directory: {}", relative_path);
                }
            }
        }
        
        if !dry_run {
            #[cfg(unix)]
            {
                fs_utils::set_file_mode(&dir_path, entry.mode)?;
            }
            
            fs_utils::set_file_mtime(&dir_path, entry.mtime_secs, entry.mtime_nanos)?;
        }
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
    pub removed: usize,
    pub copied_files: Vec<String>,
    pub unchanged_files: Vec<String>,
    pub symlink_files: Vec<String>,
    pub removed_files: Vec<String>,
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

    #[test]
    fn test_cleanup_root_only_pattern_preserves_subdirectory_files() {
        let temp_dir = TempDir::new().unwrap();
        let root = temp_dir.path();
        
        let root_config = root.join("config.txt");
        File::create(&root_config).unwrap().write_all(b"root config").unwrap();
        
        fs::create_dir(root.join("subdir")).unwrap();
        let subdir_config = root.join("subdir/config.txt");
        File::create(&subdir_config).unwrap().write_all(b"subdir config").unwrap();
        
        let mut manifest = Manifest::new("test".to_string());
        manifest.set_tracked_paths(vec![], vec!["./config.txt".to_string()]);
        
        let hash = file_hash::hash_file(&root_config).unwrap();
        let entry = FileEntry {
            hash,
            size: 11,
            #[cfg(unix)]
            mode: 0o644,
            is_symlink: false,
            symlink_target: None,
            mtime_secs: 0,
            mtime_nanos: 0,
        };
        manifest.add_file("config.txt".to_string(), entry);
        
        // should NOT delete subdir/config.txt because ./ pattern only matches root
        let stats = Arc::new(Mutex::new(LoadStats::default()));
        cleanup_stale_files(root, &manifest, false, false, stats).unwrap();
        
        assert!(root_config.exists(), "Root config.txt should exist");
        
        assert!(subdir_config.exists(), "Subdirectory config.txt should be preserved");
    }

    #[test]
    fn test_cleanup_recursive_pattern_deletes_all_matches() {
        let temp_dir = TempDir::new().unwrap();
        let root = temp_dir.path();
        
        let root_config = root.join("config.txt");
        File::create(&root_config).unwrap().write_all(b"root config").unwrap();
        
        fs::create_dir(root.join("subdir")).unwrap();
        let subdir_config = root.join("subdir/config.txt");
        File::create(&subdir_config).unwrap().write_all(b"subdir config").unwrap();
        
        let mut manifest = Manifest::new("test".to_string());
        manifest.set_tracked_paths(vec![], vec!["config.txt".to_string()]);
        
        let hash = file_hash::hash_file(&root_config).unwrap();
        let entry = FileEntry {
            hash,
            size: 11,
            #[cfg(unix)]
            mode: 0o644,
            is_symlink: false,
            symlink_target: None,
            mtime_secs: 0,
            mtime_nanos: 0,
        };
        manifest.add_file("config.txt".to_string(), entry);
        
        // SHOULD delete subdir/config.txt because recursive pattern matches it
        let stats = Arc::new(Mutex::new(LoadStats::default()));
        cleanup_stale_files(root, &manifest, false, false, stats).unwrap();
        
        assert!(root_config.exists(), "Root config.txt should exist");
        
        assert!(!subdir_config.exists(), "Subdirectory config.txt should be deleted as stale");
    }

    #[test]
    fn test_cleanup_root_only_wildcard_pattern() {
        let temp_dir = TempDir::new().unwrap();
        let root = temp_dir.path();
        
        let root_data1 = root.join("data1.bin");
        let root_data2 = root.join("data2.bin");
        File::create(&root_data1).unwrap().write_all(b"data1").unwrap();
        File::create(&root_data2).unwrap().write_all(b"data2").unwrap();
        
        fs::create_dir(root.join("nested")).unwrap();
        let nested_data = root.join("nested/data3.bin");
        File::create(&nested_data).unwrap().write_all(b"data3").unwrap();
        
        let mut manifest = Manifest::new("test".to_string());
        manifest.set_tracked_paths(vec![], vec!["./*.bin".to_string()]);
        
        let hash = file_hash::hash_file(&root_data1).unwrap();
        let entry = FileEntry {
            hash,
            size: 5,
            #[cfg(unix)]
            mode: 0o644,
            is_symlink: false,
            symlink_target: None,
            mtime_secs: 0,
            mtime_nanos: 0,
        };
        manifest.add_file("data1.bin".to_string(), entry);
        
        let stats = Arc::new(Mutex::new(LoadStats::default()));
        cleanup_stale_files(root, &manifest, false, false, stats).unwrap();
        
        // data1.bin should exist (in manifest)
        assert!(root_data1.exists(), "data1.bin should exist");
        
        // data2.bin should be deleted (matched by ./*.bin but not in manifest)
        assert!(!root_data2.exists(), "data2.bin should be deleted as stale");
        
        // nested/data3.bin should NOT be deleted (not matched by ./*.bin pattern)
        assert!(nested_data.exists(), "nested/data3.bin should be preserved");
    }

    #[test]
    fn test_cleanup_root_only_subdirectory_pattern() {
        let temp_dir = TempDir::new().unwrap();
        let root = temp_dir.path();
        
        fs::create_dir(root.join("data")).unwrap();
        let root_file1 = root.join("data/file1.txt");
        let root_file2 = root.join("data/file2.txt");
        File::create(&root_file1).unwrap().write_all(b"file1").unwrap();
        File::create(&root_file2).unwrap().write_all(b"file2").unwrap();
        
        fs::create_dir_all(root.join("project/data")).unwrap();
        let nested_file = root.join("project/data/file3.txt");
        File::create(&nested_file).unwrap().write_all(b"file3").unwrap();
        
        let mut manifest = Manifest::new("test".to_string());
        manifest.set_tracked_paths(vec![], vec!["./data/*.txt".to_string()]);
        
        let hash = file_hash::hash_file(&root_file1).unwrap();
        let entry = FileEntry {
            hash,
            size: 5,
            #[cfg(unix)]
            mode: 0o644,
            is_symlink: false,
            symlink_target: None,
            mtime_secs: 0,
            mtime_nanos: 0,
        };
        manifest.add_file("data/file1.txt".to_string(), entry);
        
        let stats = Arc::new(Mutex::new(LoadStats::default()));
        cleanup_stale_files(root, &manifest, false, false, stats).unwrap();
        
        // file1.txt should exist (in manifest)
        assert!(root_file1.exists(), "data/file1.txt should exist");
        
        // file2.txt should be deleted (matched by ./data/*.txt but not in manifest)
        assert!(!root_file2.exists(), "data/file2.txt should be deleted as stale");
        
        // project/data/file3.txt should NOT be deleted (not matched by ./data/*.txt)
        assert!(nested_file.exists(), "project/data/file3.txt should be preserved");
    }

    #[test]
    fn test_cleanup_respects_root_only_vs_recursive() {
        let temp_dir = TempDir::new().unwrap();
        let root = temp_dir.path();
        
        let root_makefile = root.join("Makefile");
        File::create(&root_makefile).unwrap().write_all(b"root").unwrap();
        
        fs::create_dir(root.join("sub1")).unwrap();
        let sub1_makefile = root.join("sub1/Makefile");
        File::create(&sub1_makefile).unwrap().write_all(b"sub1").unwrap();
        
        fs::create_dir(root.join("sub2")).unwrap();
        let sub2_makefile = root.join("sub2/Makefile");
        File::create(&sub2_makefile).unwrap().write_all(b"sub2").unwrap();
        
        let mut manifest_root_only = Manifest::new("test".to_string());
        manifest_root_only.set_tracked_paths(vec![], vec!["./Makefile".to_string()]);
        
        let hash = file_hash::hash_file(&root_makefile).unwrap();
        let entry = FileEntry {
            hash,
            size: 4,
            #[cfg(unix)]
            mode: 0o644,
            is_symlink: false,
            symlink_target: None,
            mtime_secs: 0,
            mtime_nanos: 0,
        };
        manifest_root_only.add_file("Makefile".to_string(), entry);
        
        let stats = Arc::new(Mutex::new(LoadStats::default()));
        cleanup_stale_files(root, &manifest_root_only, false, false, stats).unwrap();
        
        // With ./ pattern: subdirectory Makefiles should NOT be deleted
        assert!(root_makefile.exists(), "Root Makefile should exist");
        assert!(sub1_makefile.exists(), "sub1/Makefile should be preserved (not matched by ./)");
        assert!(sub2_makefile.exists(), "sub2/Makefile should be preserved (not matched by ./)");
    }

    #[test]
    #[cfg(unix)]
    fn test_scan_existing_files_includes_symlinks() {
        use std::os::unix::fs::symlink;
        
        let temp_dir = TempDir::new().unwrap();
        let root = temp_dir.path();
        
        // Create a regular file
        let regular_file = root.join("regular.txt");
        File::create(&regular_file).unwrap().write_all(b"regular").unwrap();
        
        // Create a symlink
        let target_file = root.join("target.txt");
        File::create(&target_file).unwrap().write_all(b"target").unwrap();
        let link_file = root.join("link.txt");
        symlink("target.txt", &link_file).unwrap();
        
        // Create manifest with both
        let mut manifest = Manifest::new("test".to_string());
        
        let regular_hash = file_hash::hash_file(&regular_file).unwrap();
        let regular_entry = FileEntry {
            hash: regular_hash.clone(),
            size: 7,
            #[cfg(unix)]
            mode: 0o644,
            is_symlink: false,
            symlink_target: None,
            mtime_secs: 0,
            mtime_nanos: 0,
        };
        manifest.add_file("regular.txt".to_string(), regular_entry);
        
        let symlink_hash = file_hash::hash_symlink(&link_file).unwrap();
        let symlink_entry = FileEntry {
            hash: symlink_hash.clone(),
            size: 0,
            #[cfg(unix)]
            mode: 0o644,
            is_symlink: true,
            symlink_target: Some("target.txt".to_string()),
            mtime_secs: 0,
            mtime_nanos: 0,
        };
        manifest.add_file("link.txt".to_string(), symlink_entry);
        
        let existing = scan_existing_files_in_manifest(root, &manifest, ProgressConfig::Auto).unwrap();
        
        // Both files should be in the map
        assert_eq!(existing.len(), 2, "Should detect both regular file and symlink");
        assert_eq!(existing.get("regular.txt"), Some(&regular_hash), "Regular file hash should match");
        assert_eq!(existing.get("link.txt"), Some(&symlink_hash), "Symlink hash should match");
    }

    #[test]
    #[cfg(unix)]
    fn test_symlink_unchanged_not_recreated() {
        use std::os::unix::fs::symlink;
        
        let temp_dir = TempDir::new().unwrap();
        let root = temp_dir.path();
        
        let store = Store::new(root);
        store.init().unwrap();
        
        // Create target file
        let target = root.join("target.txt");
        File::create(&target).unwrap().write_all(b"target").unwrap();
        
        // Create symlink
        let link = root.join("link.txt");
        symlink("target.txt", &link).unwrap();
        
        // Store the symlink target in the store
        let symlink_hash = file_hash::hash_symlink(&link).unwrap();
        store.store_symlink(&link, &symlink_hash).unwrap();
        
        // Get original metadata
        let original_metadata = link.symlink_metadata().unwrap();
        let original_mtime = original_metadata.modified().unwrap();
        
        // Sleep briefly to ensure time would change if recreated
        std::thread::sleep(std::time::Duration::from_millis(10));
        
        // Create manifest with symlink
        let mut manifest = Manifest::new("test".to_string());
        let entry = FileEntry {
            hash: symlink_hash,
            size: 0,
            #[cfg(unix)]
            mode: 0o755,
            is_symlink: true,
            symlink_target: Some("target.txt".to_string()),
            mtime_secs: 0,
            mtime_nanos: 0,
        };
        manifest.add_file("link.txt".to_string(), entry);
        
        // Create existing_files map with the symlink
        let existing_files: HashMap<String, String> = 
            scan_existing_files_in_manifest(root, &manifest, ProgressConfig::Auto).unwrap();
        
        // Load the file (should skip because unchanged)
        let stats = Arc::new(Mutex::new(LoadStats::default()));
        let progress = ByteProgress::new(0, ProgressConfig::ForceDisable);
        
        load_single_file(
            root,
            "link.txt",
            manifest.files.get("link.txt").unwrap(),
            &existing_files,
            &store,
            stats.clone(),
            false,
            false,
            &progress,
        ).unwrap();
        
        // Check that symlink was not recreated (mtime should be unchanged)
        let new_metadata = link.symlink_metadata().unwrap();
        let new_mtime = new_metadata.modified().unwrap();
        
        assert_eq!(original_mtime, new_mtime, "Symlink should not be recreated when unchanged");
        
        // Check stats show it as unchanged
        let final_stats = stats.lock().unwrap();
        assert_eq!(final_stats.unchanged, 1, "Should count as unchanged");
        assert_eq!(final_stats.symlinks, 0, "Should not count as new symlink");
        assert_eq!(final_stats.unchanged_files.len(), 1, "Should be in unchanged list");
    }

    #[test]
    #[cfg(unix)]
    fn test_symlink_changed_is_recreated() {
        use std::os::unix::fs::symlink;
        
        let temp_dir = TempDir::new().unwrap();
        let root = temp_dir.path();
        
        let store = Store::new(root);
        store.init().unwrap();
        
        // Create initial target and symlink
        let old_target = root.join("old_target.txt");
        File::create(&old_target).unwrap().write_all(b"old").unwrap();
        let link = root.join("link.txt");
        symlink("old_target.txt", &link).unwrap();
        
        // Create NEW target and store its symlink target in the store
        let new_target = root.join("new_target.txt");
        File::create(&new_target).unwrap().write_all(b"new").unwrap();
        
        // Hash what the new symlink target would be (hash of the string "new_target.txt")
        let new_symlink_hash = blake3::hash(b"new_target.txt").to_hex().to_string();
        
        // Store the symlink target (the string "new_target.txt")
        store.store_symlink(Path::new("new_target.txt"), &new_symlink_hash).unwrap();
        
        // Create manifest with NEW target
        let mut manifest = Manifest::new("test".to_string());
        let entry = FileEntry {
            hash: new_symlink_hash,
            size: 0,
            #[cfg(unix)]
            mode: 0o755,
            is_symlink: true,
            symlink_target: Some("new_target.txt".to_string()),
            mtime_secs: 0,
            mtime_nanos: 0,
        };
        manifest.add_file("link.txt".to_string(), entry);
        
        // existing_files will have the OLD hash
        let existing_files: HashMap<String, String> = 
            scan_existing_files_in_manifest(root, &manifest, ProgressConfig::Auto).unwrap();
        
        // Load the file (should recreate because changed)
        let stats = Arc::new(Mutex::new(LoadStats::default()));
        let progress = ByteProgress::new(0, ProgressConfig::ForceDisable);
        
        load_single_file(
            root,
            "link.txt",
            manifest.files.get("link.txt").unwrap(),
            &existing_files,
            &store,
            stats.clone(),
            false,
            false,
            &progress,
        ).unwrap();
        
        // Check that symlink now points to new target
        let target = fs::read_link(&link).unwrap();
        assert_eq!(target.to_string_lossy(), "new_target.txt", "Symlink should point to new target");
        
        // Check stats show it as new symlink
        let final_stats = stats.lock().unwrap();
        assert_eq!(final_stats.symlinks, 1, "Should count as restored symlink");
        assert_eq!(final_stats.unchanged, 0, "Should not count as unchanged");
        assert_eq!(final_stats.symlink_files.len(), 1, "Should be in symlink list");
    }
}

