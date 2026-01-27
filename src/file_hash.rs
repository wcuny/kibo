use anyhow::{Context, Result};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::fs::{self, File};
use std::io::{BufReader, Read};
use std::path::Path;
use std::time::SystemTime;

use crate::config::get_hash_cache_path;
use crate::fs_utils;

/// Buffer size for reading files during hashing (64KB)
const HASH_BUFFER_SIZE: usize = 64 * 1024;

/// Hash cache to avoid re-hashing unchanged files
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct HashCache {
    entries: HashMap<String, CacheEntry>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct CacheEntry {
    size: u64,
    mtime_secs: i64,
    mtime_nanos: u32,
    hash: String,
}

impl HashCache {
    /// Create a new empty hash cache
    pub fn new() -> Self {
        Self {
            entries: HashMap::new(),
        }
    }

    /// Load the hash cache from disk
    pub fn load(root: &Path) -> Result<Self> {
        let cache_path = get_hash_cache_path(root);
        
        if !cache_path.exists() {
            return Ok(Self::new());
        }

        let content = fs::read_to_string(&cache_path)
            .with_context(|| format!("Failed to read hash cache: {}", cache_path.display()))?;

        let cache: HashCache = serde_json::from_str(&content)
            .with_context(|| "Failed to parse hash cache, starting fresh")?;

        Ok(cache)
    }

    /// Save the hash cache to disk
    pub fn save(&self, root: &Path) -> Result<()> {
        let cache_path = get_hash_cache_path(root);
        
        if let Some(parent) = cache_path.parent() {
            fs::create_dir_all(parent)?;
        }

        let content = serde_json::to_string(self)
            .context("Failed to serialize hash cache")?;

        fs_utils::atomic_write(&cache_path, content.as_bytes())
            .with_context(|| format!("Failed to write hash cache: {}", cache_path.display()))?;

        Ok(())
    }

    /// Look up a cached hash if the file hasn't changed
    pub fn get(&self, path: &Path, size: u64, mtime: SystemTime) -> Option<String> {
        let path_str = path.to_string_lossy().to_string();
        
        if let Some(entry) = self.entries.get(&path_str) {
            let (mtime_secs, mtime_nanos) = system_time_to_secs_nanos(mtime);
            
            if entry.size == size 
                && entry.mtime_secs == mtime_secs 
                && entry.mtime_nanos == mtime_nanos 
            {
                return Some(entry.hash.clone());
            }
        }
        
        None
    }

    /// Insert a new hash into the cache
    pub fn insert(&mut self, path: &Path, size: u64, mtime: SystemTime, hash: String) {
        let path_str = path.to_string_lossy().to_string();
        let (mtime_secs, mtime_nanos) = system_time_to_secs_nanos(mtime);
        
        self.entries.insert(path_str, CacheEntry {
            size,
            mtime_secs,
            mtime_nanos,
            hash,
        });
    }
}

/// Convert SystemTime to seconds and nanoseconds since UNIX_EPOCH
fn system_time_to_secs_nanos(time: SystemTime) -> (i64, u32) {
    match time.duration_since(SystemTime::UNIX_EPOCH) {
        Ok(duration) => (duration.as_secs() as i64, duration.subsec_nanos()),
        Err(e) => {
            // Time before UNIX_EPOCH
            let duration = e.duration();
            (-(duration.as_secs() as i64), duration.subsec_nanos())
        }
    }
}

/// Compute the blake3 hash of a file
pub fn hash_file(path: &Path) -> Result<String> {
    let file = File::open(path)
        .with_context(|| format!("Failed to open file for hashing: {}", path.display()))?;
    
    let mut reader = BufReader::with_capacity(HASH_BUFFER_SIZE, file);
    let mut hasher = blake3::Hasher::new();
    let mut buffer = vec![0u8; HASH_BUFFER_SIZE];

    loop {
        let bytes_read = reader.read(&mut buffer)
            .with_context(|| format!("Failed to read file for hashing: {}", path.display()))?;
        
        if bytes_read == 0 {
            break;
        }
        
        hasher.update(&buffer[..bytes_read]);
    }

    Ok(hasher.finalize().to_hex().to_string())
}

/// Hash the content of a symlink (the target path, not the file it points to)
pub fn hash_symlink(path: &Path) -> Result<String> {
    let target = fs::read_link(path)
        .with_context(|| format!("Failed to read symlink: {}", path.display()))?;
    
    let target_str = target.to_string_lossy();
    let hash = blake3::hash(target_str.as_bytes());
    
    Ok(hash.to_hex().to_string())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;
    use std::io::Write;
    use tempfile::TempDir;
    use std::time::{Duration, UNIX_EPOCH};

    #[test]
    fn test_hash_cache_new() {
        let cache = HashCache::new();
        assert_eq!(cache.entries.len(), 0);
    }

    #[test]
    fn test_hash_cache_insert_and_get() {
        let mut cache = HashCache::new();
        let temp_dir = TempDir::new().unwrap();
        let file_path = temp_dir.path().join("test.txt");
        
        let size = 100;
        let mtime = UNIX_EPOCH + Duration::from_secs(1234567890);
        let hash = "abc123".to_string();

        cache.insert(&file_path, size, mtime, hash.clone());
        
        let retrieved = cache.get(&file_path, size, mtime);
        assert_eq!(retrieved, Some(hash));
    }

    #[test]
    fn test_hash_cache_get_miss_different_size() {
        let mut cache = HashCache::new();
        let temp_dir = TempDir::new().unwrap();
        let file_path = temp_dir.path().join("test.txt");
        
        let size = 100;
        let mtime = UNIX_EPOCH + Duration::from_secs(1234567890);
        let hash = "abc123".to_string();

        cache.insert(&file_path, size, mtime, hash);
        
        let retrieved = cache.get(&file_path, size + 1, mtime);
        assert_eq!(retrieved, None);
    }

    #[test]
    fn test_hash_cache_get_miss_different_mtime() {
        let mut cache = HashCache::new();
        let temp_dir = TempDir::new().unwrap();
        let file_path = temp_dir.path().join("test.txt");
        
        let size = 100;
        let mtime1 = UNIX_EPOCH + Duration::from_secs(1234567890);
        let mtime2 = UNIX_EPOCH + Duration::from_secs(1234567891);
        let hash = "abc123".to_string();

        cache.insert(&file_path, size, mtime1, hash);
        
        let retrieved = cache.get(&file_path, size, mtime2);
        assert_eq!(retrieved, None);
    }

    #[test]
    fn test_hash_cache_get_miss_no_entry() {
        let cache = HashCache::new();
        let temp_dir = TempDir::new().unwrap();
        let file_path = temp_dir.path().join("nonexistent.txt");
        
        let retrieved = cache.get(&file_path, 100, UNIX_EPOCH);
        assert_eq!(retrieved, None);
    }

    #[test]
    fn test_hash_cache_save_and_load() {
        let temp_dir = TempDir::new().unwrap();
        let root = temp_dir.path();
        
        let mut cache = HashCache::new();
        let file_path = root.join("test.txt");
        let size = 100;
        let mtime = UNIX_EPOCH + Duration::from_secs(1234567890);
        let hash = "abc123".to_string();
        
        cache.insert(&file_path, size, mtime, hash.clone());
        cache.save(root).unwrap();

        let loaded_cache = HashCache::load(root).unwrap();
        let retrieved = loaded_cache.get(&file_path, size, mtime);
        assert_eq!(retrieved, Some(hash));
    }

    #[test]
    fn test_hash_cache_load_nonexistent_returns_empty() {
        let temp_dir = TempDir::new().unwrap();
        let root = temp_dir.path();
        
        let cache = HashCache::load(root).unwrap();
        assert_eq!(cache.entries.len(), 0);
    }

    #[test]
    fn test_hash_cache_multiple_entries() {
        let mut cache = HashCache::new();
        let temp_dir = TempDir::new().unwrap();
        
        for i in 0..10 {
            let file_path = temp_dir.path().join(format!("file{}.txt", i));
            let size = 100 + i as u64;
            let mtime = UNIX_EPOCH + Duration::from_secs(1234567890 + i);
            let hash = format!("hash{}", i);
            
            cache.insert(&file_path, size, mtime, hash);
        }
        
        assert_eq!(cache.entries.len(), 10);
    }

    #[test]
    fn test_system_time_to_secs_nanos_positive() {
        let time = UNIX_EPOCH + Duration::from_secs(1234567890) + Duration::from_nanos(123456789);
        let (secs, nanos) = system_time_to_secs_nanos(time);
        
        assert_eq!(secs, 1234567890);
        assert_eq!(nanos, 123456789);
    }

    #[test]
    fn test_system_time_to_secs_nanos_epoch() {
        let time = UNIX_EPOCH;
        let (secs, nanos) = system_time_to_secs_nanos(time);
        
        assert_eq!(secs, 0);
        assert_eq!(nanos, 0);
    }

    #[test]
    fn test_hash_file_empty() {
        let temp_dir = TempDir::new().unwrap();
        let file_path = temp_dir.path().join("empty.txt");
        fs::write(&file_path, b"").unwrap();

        let hash = hash_file(&file_path).unwrap();
        
        // Empty file has a known blake3 hash
        assert!(!hash.is_empty());
        assert_eq!(hash.len(), 64); // blake3 produces 32-byte = 64 hex chars
    }

    #[test]
    fn test_hash_file_with_content() {
        let temp_dir = TempDir::new().unwrap();
        let file_path = temp_dir.path().join("content.txt");
        fs::write(&file_path, b"Hello, World!").unwrap();

        let hash1 = hash_file(&file_path).unwrap();
        let hash2 = hash_file(&file_path).unwrap();
        
        assert_eq!(hash1, hash2); // Same file should produce same hash
        assert_eq!(hash1.len(), 64);
    }

    #[test]
    fn test_hash_file_different_content_different_hash() {
        let temp_dir = TempDir::new().unwrap();
        let file1 = temp_dir.path().join("file1.txt");
        let file2 = temp_dir.path().join("file2.txt");
        
        fs::write(&file1, b"Content 1").unwrap();
        fs::write(&file2, b"Content 2").unwrap();

        let hash1 = hash_file(&file1).unwrap();
        let hash2 = hash_file(&file2).unwrap();
        
        assert_ne!(hash1, hash2);
    }

    #[test]
    fn test_hash_file_large_file() {
        let temp_dir = TempDir::new().unwrap();
        let file_path = temp_dir.path().join("large.txt");
        
        // Create a file larger than HASH_BUFFER_SIZE
        let mut file = File::create(&file_path).unwrap();
        let large_data = vec![b'x'; HASH_BUFFER_SIZE * 3];
        file.write_all(&large_data).unwrap();
        drop(file);

        let hash = hash_file(&file_path).unwrap();
        assert_eq!(hash.len(), 64);
    }

    #[test]
    fn test_hash_file_nonexistent_returns_error() {
        let temp_dir = TempDir::new().unwrap();
        let file_path = temp_dir.path().join("nonexistent.txt");

        let result = hash_file(&file_path);
        assert!(result.is_err());
    }

    #[test]
    #[cfg(unix)]
    fn test_hash_symlink() {
        let temp_dir = TempDir::new().unwrap();
        let target_path = temp_dir.path().join("target.txt");
        let link_path = temp_dir.path().join("link.txt");
        
        fs::write(&target_path, b"Target content").unwrap();
        std::os::unix::fs::symlink(&target_path, &link_path).unwrap();

        let hash = hash_symlink(&link_path).unwrap();
        
        assert_eq!(hash.len(), 64);
        
        // Hash should be consistent for same target
        let hash2 = hash_symlink(&link_path).unwrap();
        assert_eq!(hash, hash2);
    }

    #[test]
    #[cfg(unix)]
    fn test_hash_symlink_different_targets() {
        let temp_dir = TempDir::new().unwrap();
        let target1 = temp_dir.path().join("target1.txt");
        let target2 = temp_dir.path().join("target2.txt");
        let link1 = temp_dir.path().join("link1.txt");
        let link2 = temp_dir.path().join("link2.txt");
        
        fs::write(&target1, b"Target 1").unwrap();
        fs::write(&target2, b"Target 2").unwrap();
        std::os::unix::fs::symlink(&target1, &link1).unwrap();
        std::os::unix::fs::symlink(&target2, &link2).unwrap();

        let hash1 = hash_symlink(&link1).unwrap();
        let hash2 = hash_symlink(&link2).unwrap();
        
        assert_ne!(hash1, hash2);
    }

    #[test]
    fn test_hash_symlink_non_symlink_returns_error() {
        let temp_dir = TempDir::new().unwrap();
        let file_path = temp_dir.path().join("regular_file.txt");
        fs::write(&file_path, b"Regular file").unwrap();

        let result = hash_symlink(&file_path);
        assert!(result.is_err());
    }

    #[test]
    fn test_hash_cache_update_entry() {
        let mut cache = HashCache::new();
        let temp_dir = TempDir::new().unwrap();
        let file_path = temp_dir.path().join("test.txt");
        
        let size = 100;
        let mtime = UNIX_EPOCH + Duration::from_secs(1234567890);
        let hash1 = "hash1".to_string();
        let hash2 = "hash2".to_string();

        cache.insert(&file_path, size, mtime, hash1);
        cache.insert(&file_path, size, mtime, hash2.clone());
        
        let retrieved = cache.get(&file_path, size, mtime);
        assert_eq!(retrieved, Some(hash2));
    }
}