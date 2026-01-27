use anyhow::{Context, Result};
use std::fs::{self, File, Permissions};
use std::io::Write;
use std::path::Path;

#[cfg(unix)]
use std::os::unix::fs::{PermissionsExt};

/// Atomically write data to a file using temp file + rename
pub fn atomic_write(path: &Path, data: &[u8]) -> Result<()> {
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)
            .with_context(|| format!("Failed to create parent directory: {}", parent.display()))?;
    }

    let parent = path.parent().unwrap_or(Path::new("."));
    let temp_path = parent.join(format!(
        ".kibo_tmp_{}_{}",
        std::process::id(),
        rand_suffix()
    ));

    let mut file = File::create(&temp_path)
        .with_context(|| format!("Failed to create temp file: {}", temp_path.display()))?;
    
    file.write_all(data)
        .with_context(|| format!("Failed to write temp file: {}", temp_path.display()))?;
    
    file.sync_all()
        .with_context(|| format!("Failed to sync temp file: {}", temp_path.display()))?;
    
    drop(file);

    fs::rename(&temp_path, path).with_context(|| {
        let _ = fs::remove_file(&temp_path);
        format!("Failed to rename temp file to: {}", path.display())
    })?;

    Ok(())
}

/// Generate a random suffix for temp files
fn rand_suffix() -> u64 {
    use std::time::{SystemTime, UNIX_EPOCH};
    
    let duration = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default();
    
    duration.as_nanos() as u64
}

/// Create a symlink
#[cfg(unix)]
pub fn create_symlink(target: &Path, link: &Path) -> Result<()> {
    use std::os::unix::fs::symlink;
    
    if let Some(parent) = link.parent() {
        fs::create_dir_all(parent)?;
    }

    if link.exists() || link.symlink_metadata().is_ok() {
        fs::remove_file(link)
            .with_context(|| format!("Failed to remove existing file: {}", link.display()))?;
    }

    symlink(target, link)
        .with_context(|| format!("Failed to create symlink: {} -> {}", link.display(), target.display()))?;

    Ok(())
}

#[cfg(not(unix))]
pub fn create_symlink(target: &Path, link: &Path) -> Result<()> {
    use std::os::windows::fs::symlink_file;
    
    if let Some(parent) = link.parent() {
        fs::create_dir_all(parent)?;
    }

    if link.exists() {
        fs::remove_file(link)?;
    }

    symlink_file(target, link)
        .with_context(|| format!("Failed to create symlink: {} -> {}", link.display(), target.display()))?;

    Ok(())
}

/// Set file permissions from mode bits
#[cfg(unix)]
pub fn set_file_mode(path: &Path, mode: u32) -> Result<()> {
    let permissions = Permissions::from_mode(mode);
    fs::set_permissions(path, permissions)
        .with_context(|| format!("Failed to set permissions: {}", path.display()))?;
    Ok(())
}

#[cfg(not(unix))]
pub fn set_file_mode(_path: &Path, _mode: u32) -> Result<()> {
    // No-op on Windows
    Ok(())
}

/// Set file modification time from seconds and nanoseconds since Unix epoch
pub fn set_file_mtime(path: &Path, mtime_secs: i64, mtime_nanos: u32) -> Result<()> {
    use filetime::{FileTime, set_file_mtime};
    
    let file_time = FileTime::from_unix_time(mtime_secs, mtime_nanos);
    set_file_mtime(path, file_time)
        .with_context(|| format!("Failed to set mtime: {}", path.display()))?;
    Ok(())
}


/// Ensure a directory exists, creating it if necessary
pub fn ensure_dir(path: &Path) -> Result<()> {
    if !path.exists() {
        fs::create_dir_all(path)
            .with_context(|| format!("Failed to create directory: {}", path.display()))?;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;
    use tempfile::TempDir;

    #[test]
    fn test_atomic_write_creates_file() {
        let temp_dir = TempDir::new().unwrap();
        let file_path = temp_dir.path().join("test_file.txt");
        let data = b"Hello, World!";

        atomic_write(&file_path, data).unwrap();

        assert!(file_path.exists());
        let content = fs::read(&file_path).unwrap();
        assert_eq!(content, data);
    }

    #[test]
    fn test_atomic_write_overwrites_existing_file() {
        let temp_dir = TempDir::new().unwrap();
        let file_path = temp_dir.path().join("test_file.txt");
        
        // Write initial content
        fs::write(&file_path, b"Old content").unwrap();
        
        // Atomic write new content
        let new_data = b"New content";
        atomic_write(&file_path, new_data).unwrap();

        let content = fs::read(&file_path).unwrap();
        assert_eq!(content, new_data);
    }

    #[test]
    fn test_atomic_write_creates_parent_dirs() {
        let temp_dir = TempDir::new().unwrap();
        let file_path = temp_dir.path().join("nested").join("dirs").join("test_file.txt");
        let data = b"Test data";

        atomic_write(&file_path, data).unwrap();

        assert!(file_path.exists());
        let content = fs::read(&file_path).unwrap();
        assert_eq!(content, data);
    }

    #[test]
    fn test_atomic_write_empty_data() {
        let temp_dir = TempDir::new().unwrap();
        let file_path = temp_dir.path().join("empty_file.txt");
        let data = b"";

        atomic_write(&file_path, data).unwrap();

        assert!(file_path.exists());
        let content = fs::read(&file_path).unwrap();
        assert_eq!(content.len(), 0);
    }

    #[test]
    fn test_rand_suffix_generates_different_values() {
        let suffix1 = rand_suffix();
        std::thread::sleep(std::time::Duration::from_nanos(1));
        let suffix2 = rand_suffix();
        
        assert_ne!(suffix1, suffix2);
    }

    #[test]
    #[cfg(unix)]
    fn test_create_symlink_success() {
        let temp_dir = TempDir::new().unwrap();
        let target_path = temp_dir.path().join("target.txt");
        let link_path = temp_dir.path().join("link.txt");
        
        fs::write(&target_path, b"Target content").unwrap();
        create_symlink(&target_path, &link_path).unwrap();

        assert!(link_path.symlink_metadata().unwrap().is_symlink());
        let link_target = fs::read_link(&link_path).unwrap();
        assert_eq!(link_target, target_path);
    }

    #[test]
    #[cfg(unix)]
    fn test_create_symlink_replaces_existing() {
        let temp_dir = TempDir::new().unwrap();
        let target1 = temp_dir.path().join("target1.txt");
        let target2 = temp_dir.path().join("target2.txt");
        let link_path = temp_dir.path().join("link.txt");
        
        fs::write(&target1, b"Target 1").unwrap();
        fs::write(&target2, b"Target 2").unwrap();
        
        create_symlink(&target1, &link_path).unwrap();
        create_symlink(&target2, &link_path).unwrap();

        let link_target = fs::read_link(&link_path).unwrap();
        assert_eq!(link_target, target2);
    }

    #[test]
    #[cfg(unix)]
    fn test_create_symlink_creates_parent_dirs() {
        let temp_dir = TempDir::new().unwrap();
        let target_path = temp_dir.path().join("target.txt");
        let link_path = temp_dir.path().join("nested").join("dir").join("link.txt");
        
        fs::write(&target_path, b"Target").unwrap();
        create_symlink(&target_path, &link_path).unwrap();

        assert!(link_path.symlink_metadata().unwrap().is_symlink());
    }

    #[test]
    #[cfg(unix)]
    fn test_set_file_mode() {
        let temp_dir = TempDir::new().unwrap();
        let file_path = temp_dir.path().join("test_file.txt");
        fs::write(&file_path, b"Test").unwrap();

        // Set mode to 0o644 (rw-r--r--)
        set_file_mode(&file_path, 0o644).unwrap();

        let metadata = fs::metadata(&file_path).unwrap();
        let permissions = metadata.permissions();
        assert_eq!(permissions.mode() & 0o777, 0o644);
    }

    #[test]
    #[cfg(unix)]
    fn test_set_file_mode_executable() {
        let temp_dir = TempDir::new().unwrap();
        let file_path = temp_dir.path().join("executable.sh");
        fs::write(&file_path, b"#!/bin/bash").unwrap();

        // Set mode to 0o755 (rwxr-xr-x)
        set_file_mode(&file_path, 0o755).unwrap();

        let metadata = fs::metadata(&file_path).unwrap();
        let permissions = metadata.permissions();
        assert_eq!(permissions.mode() & 0o777, 0o755);
    }

    #[test]
    #[cfg(not(unix))]
    fn test_set_file_mode_noop_on_windows() {
        let temp_dir = TempDir::new().unwrap();
        let file_path = temp_dir.path().join("test_file.txt");
        fs::write(&file_path, b"Test").unwrap();

        // Should not error on Windows
        let result = set_file_mode(&file_path, 0o644);
        assert!(result.is_ok());
    }

    #[test]
    fn test_set_file_mtime() {
        let temp_dir = TempDir::new().unwrap();
        let file_path = temp_dir.path().join("test_file.txt");
        fs::write(&file_path, b"Test").unwrap();

        let mtime_secs = 1234567890;
        let mtime_nanos = 123456789;
        
        set_file_mtime(&file_path, mtime_secs, mtime_nanos).unwrap();

        let metadata = fs::metadata(&file_path).unwrap();
        let mtime = metadata.modified().unwrap();
        let duration = mtime.duration_since(std::time::UNIX_EPOCH).unwrap();
        
        assert_eq!(duration.as_secs() as i64, mtime_secs);
    }

    #[test]
    fn test_ensure_dir_creates_directory() {
        let temp_dir = TempDir::new().unwrap();
        let dir_path = temp_dir.path().join("new_directory");

        assert!(!dir_path.exists());
        ensure_dir(&dir_path).unwrap();
        assert!(dir_path.exists());
        assert!(dir_path.is_dir());
    }

    #[test]
    fn test_ensure_dir_creates_nested_directories() {
        let temp_dir = TempDir::new().unwrap();
        let dir_path = temp_dir.path().join("nested").join("deep").join("directory");

        ensure_dir(&dir_path).unwrap();
        assert!(dir_path.exists());
        assert!(dir_path.is_dir());
    }

    #[test]
    fn test_ensure_dir_idempotent() {
        let temp_dir = TempDir::new().unwrap();
        let dir_path = temp_dir.path().join("existing_directory");

        ensure_dir(&dir_path).unwrap();
        ensure_dir(&dir_path).unwrap(); // Should not error

        assert!(dir_path.exists());
    }

    #[test]
    fn test_atomic_write_concurrent_writes() {
        use std::sync::Arc;
        use std::thread;

        let temp_dir = TempDir::new().unwrap();
        let temp_dir_path = Arc::new(temp_dir.path().to_path_buf());
        
        // Pre-create subdirectories to avoid race conditions
        for i in 0..10 {
            let subdir = temp_dir_path.join(format!("subdir_{}", i));
            fs::create_dir(&subdir).unwrap();
        }
        
        let handles: Vec<_> = (0..10)
            .map(|i| {
                let path = Arc::clone(&temp_dir_path);
                thread::spawn(move || {
                    // Each thread writes to its own subdirectory to avoid races
                    let file_path = path.join(format!("subdir_{}/file.txt", i));
                    let data = format!("Thread {} data", i);
                    atomic_write(&file_path, data.as_bytes())
                })
            })
            .collect();

        let mut results = Vec::new();
        for handle in handles {
            results.push(handle.join());
        }

        // Check all threads succeeded
        for (i, result) in results.iter().enumerate() {
            assert!(result.is_ok(), "Thread {} failed: {:?}", i, result);
            if let Ok(write_result) = result {
                assert!(write_result.is_ok(), "Thread {} write failed: {:?}", i, write_result);
            }
        }

        // Verify all files exist
        for i in 0..10 {
            let file_path = temp_dir_path.join(format!("subdir_{}/file.txt", i));
            assert!(file_path.exists(), "File for thread {} doesn't exist", i);
        }
    }
}