use anyhow::{Context, Result, bail};
use std::fs::{self, File};
use std::io::{BufReader, BufWriter, Read, Write};
use std::path::{Path, PathBuf};

use crate::config::get_store_dir;
use crate::fs_utils;

const BUFFER_SIZE: usize = 64 * 1024;
const COMPRESSION_MAGIC: &[u8; 4] = b"KBCP"; // "KBCP" = KiBo ComPressed
const DEFAULT_COMPRESSION_LEVEL: i32 = 3;

/// Content-addressed store for file blobs
pub struct Store {
    /// Root directory of the store
    store_dir: PathBuf,
    /// Compression level (0 = no compression, 1-10 = zstd levels)
    compression_level: u32,
}

impl Store {
    /// Create a new store instance
    pub fn new(repo_root: &Path) -> Self {
        Self {
            store_dir: get_store_dir(repo_root),
            compression_level: 0,
        }
    }

    /// Create a new store instance with compression
    pub fn with_compression(repo_root: &Path, compression_level: u32) -> Self {
        Self {
            store_dir: get_store_dir(repo_root),
            compression_level,
        }
    }

    /// Ensure the store directory exists
    pub fn init(&self) -> Result<()> {
        fs_utils::ensure_dir(&self.store_dir)
            .context("Failed to initialize content store")?;
        Ok(())
    }

    /// Get the path where a blob with the given hash would be stored
    /// Uses a two level directory structure to avoid too many files in one directory
    pub fn blob_path(&self, hash: &str) -> PathBuf {
        let (prefix, rest) = hash.split_at(2.min(hash.len()));
        self.store_dir.join(prefix).join(rest)
    }

    /// Check if a blob exists in the store
    pub fn has_blob(&self, hash: &str) -> bool {
        self.blob_path(hash).exists()
    }

    /// Store a file in the content addressed store
    /// Returns true if the file was newly stored, false if it already existed
    pub fn store_file(&self, src_path: &Path, hash: &str) -> Result<bool> {
        let blob_path = self.blob_path(hash);

        if blob_path.exists() {
            return Ok(false);
        }

        if let Some(parent) = blob_path.parent() {
            fs::create_dir_all(parent)
                .with_context(|| format!("Failed to create store subdirectory: {}", parent.display()))?;
        }

        let temp_path = blob_path.with_extension("tmp");
        
        if self.compression_level > 0 {
            self.compress_file_to_blob(src_path, &temp_path)?;
        }
        else {
            copy_file(src_path, &temp_path)
                .with_context(|| format!("Failed to copy file to store: {}", src_path.display()))?;
        }

        fs::rename(&temp_path, &blob_path).with_context(|| {
            let _ = fs::remove_file(&temp_path);
            format!("Failed to finalize blob in store: {}", hash)
        })?;

        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            let permissions = fs::Permissions::from_mode(0o444);
            let _ = fs::set_permissions(&blob_path, permissions);
        }

        Ok(true)
    }

    /// Store symlink target in the content addressed store
    pub fn store_symlink(&self, target: &Path, hash: &str) -> Result<bool> {
        let blob_path = self.blob_path(hash);

        if blob_path.exists() {
            return Ok(false);
        }

        if let Some(parent) = blob_path.parent() {
            fs::create_dir_all(parent)?;
        }

        let target_str = target.to_string_lossy();
        fs_utils::atomic_write(&blob_path, target_str.as_bytes())
            .with_context(|| format!("Failed to store symlink target: {}", hash))?;

        Ok(true)
    }

    /// Retrieve a symlink target from the store
    pub fn retrieve_symlink_target(&self, hash: &str) -> Result<PathBuf> {
        let blob_path = self.blob_path(hash);

        if !blob_path.exists() {
            bail!("Symlink blob not found in store: {}", hash);
        }

        let target_str = fs::read_to_string(&blob_path)
            .with_context(|| format!("Failed to read symlink target from store: {}", hash))?;

        Ok(PathBuf::from(target_str))
    }

    /// Get total size of the store in bytes
    pub fn total_size(&self) -> Result<u64> {
        if !self.store_dir.exists() {
            return Ok(0);
        }

        let mut total = 0u64;

        for entry in walkdir::WalkDir::new(&self.store_dir)
            .into_iter()
            .filter_map(|e| e.ok())
        {
            if entry.file_type().is_file() {
                if let Ok(metadata) = entry.metadata() {
                    total += metadata.len();
                }
            }
        }

        Ok(total)
    }

    /// Get the number of blobs in the store
    pub fn blob_count(&self) -> Result<usize> {
        if !self.store_dir.exists() {
            return Ok(0);
        }

        let count = walkdir::WalkDir::new(&self.store_dir)
            .into_iter()
            .filter_map(|e| e.ok())
            .filter(|e| e.file_type().is_file())
            .count();

        Ok(count)
    }

    /// Remove blobs not referenced by any manifest
    /// Returns the number of blobs removed and bytes freed
    pub fn garbage_collect(&self, referenced_hashes: &std::collections::HashSet<String>, show_progress: bool) -> Result<(usize, u64)> {
        if !self.store_dir.exists() {
            return Ok((0, 0));
        }

        let spinner = if show_progress {
            Some(crate::progress::Spinner::new(
                crate::progress::ProgressConfig::Auto,
                "Garbage collecting unreferenced blobs"
            ))
        }
        else {
            None
        };

        let mut removed_count = 0;
        let mut freed_bytes = 0u64;

        for prefix_entry in fs::read_dir(&self.store_dir)? {
            let prefix_entry = prefix_entry?;
            if !prefix_entry.file_type()?.is_dir() {
                continue;
            }

            let prefix = prefix_entry.file_name().to_string_lossy().to_string();

            for blob_entry in fs::read_dir(prefix_entry.path())? {
                let blob_entry = blob_entry?;
                if !blob_entry.file_type()?.is_file() {
                    continue;
                }

                let blob_name = blob_entry.file_name().to_string_lossy().to_string();
                let hash = format!("{}{}", prefix, blob_name);

                if !referenced_hashes.contains(&hash) {
                    let metadata = blob_entry.metadata()?;
                    freed_bytes += metadata.len();
                    
                    #[cfg(unix)]
                    {
                        use std::os::unix::fs::PermissionsExt;
                        let _ = fs::set_permissions(
                            blob_entry.path(),
                            fs::Permissions::from_mode(0o644),
                        );
                    }
                    
                    fs::remove_file(blob_entry.path())?;
                    removed_count += 1;
                }
            }

            if fs::read_dir(prefix_entry.path())?.next().is_none() {
                let _ = fs::remove_dir(prefix_entry.path());
            }
        }

        if let Some(sp) = spinner {
            sp.finish();
        }

        Ok((removed_count, freed_bytes))
    }

    /// Compress a file and write it to blob storage
    fn compress_file_to_blob(&self, src: &Path, dst: &Path) -> Result<()> {
        if let Some(parent) = dst.parent() {
            fs::create_dir_all(parent)?;
        }

        let src_file = File::open(src)
            .with_context(|| format!("Failed to open source file: {}", src.display()))?;
        
        let dst_file = File::create(dst)
            .with_context(|| format!("Failed to create destination file: {}", dst.display()))?;

        let mut reader = BufReader::with_capacity(BUFFER_SIZE, src_file);
        let mut writer = BufWriter::with_capacity(BUFFER_SIZE, dst_file);

        writer.write_all(COMPRESSION_MAGIC)?;

        let compression_level = if self.compression_level > 22 {
            22 // Max zstd level
        }
        else if self.compression_level == 0 {
            DEFAULT_COMPRESSION_LEVEL
        }
        else {
            self.compression_level as i32
        };

        let mut encoder = zstd::Encoder::new(&mut writer, compression_level)?;
        std::io::copy(&mut reader, &mut encoder)?;
        encoder.finish()?;

        Ok(())
    }

    /// Check if a blob is compressed by reading magic bytes
    pub fn is_blob_compressed(&self, hash: &str) -> Result<bool> {
        let blob_path = self.blob_path(hash);
        if !blob_path.exists() {
            return Ok(false);
        }

        let mut file = File::open(&blob_path)?;
        let mut magic = [0u8; 4];
        
        if file.read_exact(&mut magic).is_err() {
            return Ok(false);
        }

        Ok(&magic == COMPRESSION_MAGIC)
    }

    /// Decompress a blob to a destination file
    pub fn decompress_blob_to_file(&self, blob_path: &Path, dst: &Path) -> Result<()> {
        if let Some(parent) = dst.parent() {
            fs::create_dir_all(parent)?;
        }

        let src_file = File::open(blob_path)
            .with_context(|| format!("Failed to open blob: {}", blob_path.display()))?;
        
        let dst_file = File::create(dst)
            .with_context(|| format!("Failed to create destination file: {}", dst.display()))?;

        let mut reader = BufReader::with_capacity(BUFFER_SIZE, src_file);
        let mut writer = BufWriter::with_capacity(BUFFER_SIZE, dst_file);

        let mut magic = [0u8; 4];
        reader.read_exact(&mut magic)?;
        
        if &magic != COMPRESSION_MAGIC {
            bail!("Blob is not compressed (missing magic bytes)");
        }

        let mut decoder = zstd::Decoder::new(reader)?;
        std::io::copy(&mut decoder, &mut writer)?;
        writer.flush()?;

        Ok(())
    }

    /// Copy a blob to destination, automatically handling compression
    pub fn copy_blob_to_file(&self, hash: &str, dst: &Path) -> Result<()> {
        let blob_path = self.blob_path(hash);
        
        if !blob_path.exists() {
            bail!("Blob not found: {}", hash);
        }

        if self.is_blob_compressed(hash)? {
            self.decompress_blob_to_file(&blob_path, dst)
        }
        else {
            copy_file(&blob_path, dst)
        }
    }
}

/// Copy a file efficiently using buffered I/O
fn copy_file(src: &Path, dst: &Path) -> Result<()> {
    if let Some(parent) = dst.parent() {
        fs::create_dir_all(parent)?;
    }

    let src_file = File::open(src)
        .with_context(|| format!("Failed to open source file: {}", src.display()))?;
    
    let dst_file = File::create(dst)
        .with_context(|| format!("Failed to create destination file: {}", dst.display()))?;

    let mut reader = BufReader::with_capacity(BUFFER_SIZE, src_file);
    let mut writer = BufWriter::with_capacity(BUFFER_SIZE, dst_file);

    let mut buffer = vec![0u8; BUFFER_SIZE];

    loop {
        let bytes_read = reader.read(&mut buffer)?;
        if bytes_read == 0 {
            break;
        }
        writer.write_all(&buffer[..bytes_read])?;
    }

    writer.flush()?;

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;
    use std::collections::HashSet;

    #[test]
    fn test_store_new() {
        let temp_dir = TempDir::new().unwrap();
        let store = Store::new(temp_dir.path());
        
        assert_eq!(store.compression_level, 0);
        assert!(store.store_dir.ends_with(".kibo/store"));
    }

    #[test]
    fn test_store_with_compression() {
        let temp_dir = TempDir::new().unwrap();
        let store = Store::with_compression(temp_dir.path(), 5);
        
        assert_eq!(store.compression_level, 5);
    }

    #[test]
    fn test_store_init() {
        let temp_dir = TempDir::new().unwrap();
        let store = Store::new(temp_dir.path());
        
        store.init().unwrap();
        
        assert!(store.store_dir.exists());
        assert!(store.store_dir.is_dir());
    }

    #[test]
    fn test_blob_path() {
        let temp_dir = TempDir::new().unwrap();
        let store = Store::new(temp_dir.path());
        
        let hash = "abcdef123456";
        let path = store.blob_path(hash);
        
        assert!(path.to_string_lossy().contains("ab"));
        assert!(path.to_string_lossy().contains("cdef123456"));
    }

    #[test]
    fn test_blob_path_short_hash() {
        let temp_dir = TempDir::new().unwrap();
        let store = Store::new(temp_dir.path());
        
        let hash = "a";
        let path = store.blob_path(hash);
        
        assert!(path.to_string_lossy().contains("a"));
    }

    #[test]
    fn test_has_blob_false() {
        let temp_dir = TempDir::new().unwrap();
        let store = Store::new(temp_dir.path());
        store.init().unwrap();
        
        assert!(!store.has_blob("nonexistent"));
    }

    #[test]
    fn test_store_file() {
        let temp_dir = TempDir::new().unwrap();
        let store = Store::new(temp_dir.path());
        store.init().unwrap();
        
        let test_file = temp_dir.path().join("test.txt");
        fs::write(&test_file, b"Hello, World!").unwrap();
        
        let hash = "abc123";
        let was_new = store.store_file(&test_file, hash).unwrap();
        
        assert!(was_new);
        assert!(store.has_blob(hash));
    }

    #[test]
    fn test_store_file_already_exists() {
        let temp_dir = TempDir::new().unwrap();
        let store = Store::new(temp_dir.path());
        store.init().unwrap();
        
        let test_file = temp_dir.path().join("test.txt");
        fs::write(&test_file, b"Hello, World!").unwrap();
        
        let hash = "abc123";
        store.store_file(&test_file, hash).unwrap();
        let was_new = store.store_file(&test_file, hash).unwrap();
        
        assert!(!was_new);
    }

    #[test]
    fn test_store_symlink() {
        let temp_dir = TempDir::new().unwrap();
        let store = Store::new(temp_dir.path());
        store.init().unwrap();
        
        let target = Path::new("/path/to/target");
        let hash = "symlink123";
        
        let was_new = store.store_symlink(target, hash).unwrap();
        
        assert!(was_new);
        assert!(store.has_blob(hash));
    }

    #[test]
    fn test_store_symlink_already_exists() {
        let temp_dir = TempDir::new().unwrap();
        let store = Store::new(temp_dir.path());
        store.init().unwrap();
        
        let target = Path::new("/path/to/target");
        let hash = "symlink123";
        
        store.store_symlink(target, hash).unwrap();
        let was_new = store.store_symlink(target, hash).unwrap();
        
        assert!(!was_new);
    }

    #[test]
    fn test_retrieve_symlink_target() {
        let temp_dir = TempDir::new().unwrap();
        let store = Store::new(temp_dir.path());
        store.init().unwrap();
        
        let target = Path::new("/path/to/target");
        let hash = "symlink123";
        
        store.store_symlink(target, hash).unwrap();
        let retrieved = store.retrieve_symlink_target(hash).unwrap();
        
        assert_eq!(retrieved, target);
    }

    #[test]
    fn test_retrieve_symlink_target_not_found() {
        let temp_dir = TempDir::new().unwrap();
        let store = Store::new(temp_dir.path());
        store.init().unwrap();
        
        let result = store.retrieve_symlink_target("nonexistent");
        assert!(result.is_err());
    }

    #[test]
    fn test_total_size_empty() {
        let temp_dir = TempDir::new().unwrap();
        let store = Store::new(temp_dir.path());
        
        let size = store.total_size().unwrap();
        assert_eq!(size, 0);
    }

    #[test]
    fn test_total_size_with_blobs() {
        let temp_dir = TempDir::new().unwrap();
        let store = Store::new(temp_dir.path());
        store.init().unwrap();
        
        let test_file = temp_dir.path().join("test.txt");
        fs::write(&test_file, b"Hello, World!").unwrap();
        
        store.store_file(&test_file, "hash1").unwrap();
        
        let size = store.total_size().unwrap();
        assert!(size > 0);
    }

    #[test]
    fn test_blob_count_empty() {
        let temp_dir = TempDir::new().unwrap();
        let store = Store::new(temp_dir.path());
        
        let count = store.blob_count().unwrap();
        assert_eq!(count, 0);
    }

    #[test]
    fn test_blob_count_with_blobs() {
        let temp_dir = TempDir::new().unwrap();
        let store = Store::new(temp_dir.path());
        store.init().unwrap();
        
        let test_file = temp_dir.path().join("test.txt");
        fs::write(&test_file, b"Hello, World!").unwrap();
        
        store.store_file(&test_file, "hash1").unwrap();
        store.store_file(&test_file, "hash2").unwrap();
        
        let count = store.blob_count().unwrap();
        assert_eq!(count, 2);
    }

    #[test]
    fn test_garbage_collect_empty() {
        let temp_dir = TempDir::new().unwrap();
        let store = Store::new(temp_dir.path());
        store.init().unwrap();
        
        let referenced = HashSet::new();
        let (removed, freed) = store.garbage_collect(&referenced, false).unwrap();
        
        assert_eq!(removed, 0);
        assert_eq!(freed, 0);
    }

    #[test]
    fn test_garbage_collect_all_referenced() {
        let temp_dir = TempDir::new().unwrap();
        let store = Store::new(temp_dir.path());
        store.init().unwrap();
        
        let test_file = temp_dir.path().join("test.txt");
        fs::write(&test_file, b"Hello, World!").unwrap();
        
        let hash = "hash1";
        store.store_file(&test_file, hash).unwrap();
        
        let mut referenced = HashSet::new();
        referenced.insert(hash.to_string());
        
        let (removed, freed) = store.garbage_collect(&referenced, false).unwrap();
        
        assert_eq!(removed, 0);
        assert_eq!(freed, 0);
        assert!(store.has_blob(hash));
    }

    #[test]
    fn test_garbage_collect_unreferenced() {
        let temp_dir = TempDir::new().unwrap();
        let store = Store::new(temp_dir.path());
        store.init().unwrap();
        
        let test_file = temp_dir.path().join("test.txt");
        fs::write(&test_file, b"Hello, World!").unwrap();
        
        let hash = "hash1";
        store.store_file(&test_file, hash).unwrap();
        
        let referenced = HashSet::new();
        let (removed, freed) = store.garbage_collect(&referenced, false).unwrap();
        
        assert_eq!(removed, 1);
        assert!(freed > 0);
        assert!(!store.has_blob(hash));
    }

    #[test]
    fn test_store_file_with_compression() {
        let temp_dir = TempDir::new().unwrap();
        let store = Store::with_compression(temp_dir.path(), 3);
        store.init().unwrap();
        
        let test_file = temp_dir.path().join("test.txt");
        fs::write(&test_file, b"Hello, World! ".repeat(100)).unwrap();
        
        let hash = "compressed123";
        store.store_file(&test_file, hash).unwrap();
        
        assert!(store.has_blob(hash));
        assert!(store.is_blob_compressed(hash).unwrap());
    }

    #[test]
    fn test_is_blob_compressed_false() {
        let temp_dir = TempDir::new().unwrap();
        let store = Store::new(temp_dir.path());
        store.init().unwrap();
        
        let test_file = temp_dir.path().join("test.txt");
        fs::write(&test_file, b"Hello, World!").unwrap();
        
        let hash = "uncompressed123";
        store.store_file(&test_file, hash).unwrap();
        
        assert!(!store.is_blob_compressed(hash).unwrap());
    }

    #[test]
    fn test_copy_blob_to_file_uncompressed() {
        let temp_dir = TempDir::new().unwrap();
        let store = Store::new(temp_dir.path());
        store.init().unwrap();
        
        let test_file = temp_dir.path().join("test.txt");
        let content = b"Hello, World!";
        fs::write(&test_file, content).unwrap();
        
        let hash = "hash123";
        store.store_file(&test_file, hash).unwrap();
        
        let dst = temp_dir.path().join("restored.txt");
        store.copy_blob_to_file(hash, &dst).unwrap();
        
        let restored_content = fs::read(&dst).unwrap();
        assert_eq!(restored_content, content);
    }

    #[test]
    fn test_copy_blob_to_file_compressed() {
        let temp_dir = TempDir::new().unwrap();
        let store = Store::with_compression(temp_dir.path(), 3);
        store.init().unwrap();
        
        let test_file = temp_dir.path().join("test.txt");
        let content = b"Hello, World! ".repeat(100);
        fs::write(&test_file, &content).unwrap();
        
        let hash = "compressed123";
        store.store_file(&test_file, hash).unwrap();
        
        let dst = temp_dir.path().join("restored.txt");
        store.copy_blob_to_file(hash, &dst).unwrap();
        
        let restored_content = fs::read(&dst).unwrap();
        assert_eq!(restored_content, content.as_slice());
    }

    #[test]
    fn test_copy_blob_to_file_not_found() {
        let temp_dir = TempDir::new().unwrap();
        let store = Store::new(temp_dir.path());
        store.init().unwrap();
        
        let dst = temp_dir.path().join("restored.txt");
        let result = store.copy_blob_to_file("nonexistent", &dst);
        
        assert!(result.is_err());
    }

    #[test]
    fn test_copy_file_function() {
        let temp_dir = TempDir::new().unwrap();
        
        let src = temp_dir.path().join("src.txt");
        let content = b"Test content";
        fs::write(&src, content).unwrap();
        
        let dst = temp_dir.path().join("dst.txt");
        copy_file(&src, &dst).unwrap();
        
        let dst_content = fs::read(&dst).unwrap();
        assert_eq!(dst_content, content);
    }

    #[test]
    fn test_copy_file_creates_parent_dirs() {
        let temp_dir = TempDir::new().unwrap();
        
        let src = temp_dir.path().join("src.txt");
        fs::write(&src, b"Test").unwrap();
        
        let dst = temp_dir.path().join("subdir/nested/dst.txt");
        copy_file(&src, &dst).unwrap();
        
        assert!(dst.exists());
    }
}