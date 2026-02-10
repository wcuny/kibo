use anyhow::{Context, Result, bail};
use serde::{Deserialize, Serialize};
use std::path::{Path, PathBuf};

pub const CONFIG_FILENAME: &str = ".kibo.toml";
pub const KIBO_DIR: &str = ".kibo";
pub const STORE_DIR: &str = "store";
pub const MANIFESTS_DIR: &str = "manifests";
pub const HASH_CACHE_FILE: &str = "hash_cache.json";
pub const HISTORY_LOG_FILE: &str = "history.log";

/// Database configuration for MySQL snapshots
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DatabaseConfig {
    /// Database type (only "mysql" supported)
    #[serde(default = "default_db_type")]
    pub db_type: String,

    /// Database user
    #[serde(default = "default_db_user")]
    pub user: String,

    /// Database password (stored in plain text)
    #[serde(default)]
    pub password: String,

    /// Database host
    #[serde(default = "default_db_host")]
    pub host: String,

    /// Database port
    #[serde(default = "default_db_port")]
    pub port: u16,

    /// Database name to snapshot (can be overridden by --include-db flag)
    #[serde(default = "default_db_name")]
    pub name: String,

    /// Tables to snapshot ("*" means all tables)
    #[serde(default = "default_db_tables")]
    pub tables: Vec<String>,

    /// Use single transaction for consistent snapshots
    #[serde(default = "default_db_single_transaction")]
    pub single_transaction: bool,
}

fn default_db_type() -> String { "mysql".to_string() }
fn default_db_user() -> String { "root".to_string() }
fn default_db_host() -> String { "localhost".to_string() }
fn default_db_port() -> u16 { 3306 }
fn default_db_name() -> String { "mydb".to_string() }
fn default_db_tables() -> Vec<String> { vec!["*".to_string()] }
fn default_db_single_transaction() -> bool { true }

impl Default for DatabaseConfig {
    fn default() -> Self {
        Self {
            db_type: default_db_type(),
            user: default_db_user(),
            password: String::new(),
            host: default_db_host(),
            port: default_db_port(),
            name: default_db_name(),
            tables: default_db_tables(),
            single_transaction: default_db_single_transaction(),
        }
    }
}

/// Configuration loaded from .kibo.toml
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Config {
    /// List of directories to snapshot (optional)
    #[serde(default)]
    pub directories: Vec<String>,

    /// List of file patterns to snapshot (optional, uses glob patterns)
    #[serde(default)]
    pub files: Vec<String>,

    /// List of paths/patterns to ignore inside tracked directories (optional)
    #[serde(default)]
    pub ignore: Vec<String>,

    /// Optional warning threshold for snapshot size in GB
    #[serde(default)]
    pub max_snapshot_size_gb: Option<f64>,

    /// Compression level (0 = no compression, MVP default)
    #[serde(default)]
    pub compression_level: u32,

    /// Show progress bars (default: auto-detect TTY)
    #[serde(default)]
    pub progress: Option<bool>,

    /// Database configuration (optional)
    #[serde(default)]
    pub database: Option<DatabaseConfig>,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            directories: Vec::new(),
            files: Vec::new(),
            ignore: Vec::new(),
            max_snapshot_size_gb: None,
            compression_level: 0,
            progress: None,
            database: None,
        }
    }
}

impl Config {
    /// Load configuration from a TOML file
    pub fn load(config_path: &Path) -> Result<Self> {
        if !config_path.exists() {
            bail!(
                "Configuration file not found: {}\n\
                 Create a {} file with at least a 'directories' list to snapshot.",
                config_path.display(),
                CONFIG_FILENAME
            );
        }

        let content = std::fs::read_to_string(config_path)
            .with_context(|| format!("Failed to read config file: {}", config_path.display()))?;

        let config: Config = toml::from_str(&content)
            .with_context(|| format!("Failed to parse config file: {}", config_path.display()))?;

        config.validate()?;
        Ok(config)
    }

    /// Validate the configuration
    pub fn validate(&self) -> Result<()> {
        if self.directories.is_empty() && self.files.is_empty() {
            bail!(
                "Configuration error: both 'directories' and 'files' lists are empty.\n\
                 You must specify at least one directory or file pattern to snapshot."
            );
        }

        for path in &self.directories {
            if path.trim().is_empty() {
                bail!("Configuration error: empty path in 'directories' list");
            }
            if path.contains("..") {
                bail!(
                    "Configuration error: path '{}' contains '..', which is not allowed",
                    path
                );
            }
        }


        Ok(())
    }

    /// Validate and warn about compression level
    pub fn validate_compression_level(&self) {
        if self.compression_level > 10 {
            eprintln!(
                "Warning: compression_level {} exceeds maximum allowed (10). Will use level 10.",
                self.compression_level
            );
        } else if self.compression_level > 6 {
            eprintln!(
                "Warning: compression_level {} may be very slow. Recommended maximum is 6.",
                self.compression_level
            );
        }
    }

    /// Get the effective compression level (capped at 10)
    pub fn effective_compression_level(&self) -> u32 {
        std::cmp::min(self.compression_level, 10)
    }

    /// Check if a path should be ignored
    pub fn should_ignore(&self, relative_path: &Path) -> bool {
        let path_str = relative_path.to_string_lossy();
        
        for pattern in &self.ignore {
            if let Ok(glob_pattern) = glob::Pattern::new(pattern) {
                if glob_pattern.matches(&path_str) {
                    return true;
                }
            }
            
            if path_str.starts_with(pattern) {
                return true;
            }
            
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

/// Find the repository root by looking for .kibo.toml
pub fn find_repo_root() -> Result<PathBuf> {
    let current_dir = std::env::current_dir()
        .context("Failed to get current directory")?;
    
    let mut dir = current_dir.as_path();
    
    loop {
        let config_path = dir.join(CONFIG_FILENAME);
        if config_path.exists() {
            return Ok(dir.to_path_buf());
        }
        
        match dir.parent() {
            Some(parent) => dir = parent,
            None => bail!(
                "Could not find {} in current directory or any parent directory.\n\
                 Please create a configuration file using 'kibo init' or run from within a kibo-enabled repository.",
                CONFIG_FILENAME
            ),
        }
    }
}

/// Get the kibo directory path (.kibo/)
pub fn get_kibo_dir(root: &Path) -> PathBuf {
    root.join(KIBO_DIR)
}

/// Get the store directory path (.kibo/store/)
pub fn get_store_dir(root: &Path) -> PathBuf {
    get_kibo_dir(root).join(STORE_DIR)
}

/// Get the manifests directory path (.kibo/manifests/)
pub fn get_manifests_dir(root: &Path) -> PathBuf {
    get_kibo_dir(root).join(MANIFESTS_DIR)
}

/// Get the hash cache file path
pub fn get_hash_cache_path(root: &Path) -> PathBuf {
    get_kibo_dir(root).join(HASH_CACHE_FILE)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;
    use tempfile::TempDir;

    #[test]
    fn test_database_config_defaults() {
        let db_config = DatabaseConfig::default();
        assert_eq!(db_config.db_type, "mysql");
        assert_eq!(db_config.user, "root");
        assert_eq!(db_config.password, "");
        assert_eq!(db_config.host, "localhost");
        assert_eq!(db_config.port, 3306);
        assert_eq!(db_config.name, "mydb");
        assert_eq!(db_config.tables, vec!["*"]);
        assert!(db_config.single_transaction);
    }

    #[test]
    fn test_config_defaults() {
        let config = Config::default();
        assert!(config.directories.is_empty());
        assert!(config.files.is_empty());
        assert!(config.ignore.is_empty());
        assert_eq!(config.max_snapshot_size_gb, None);
        assert_eq!(config.compression_level, 0);
        assert_eq!(config.progress, None);
        assert!(config.database.is_none());
    }

    #[test]
    fn test_config_load_success() {
        let temp_dir = TempDir::new().unwrap();
        let config_path = temp_dir.path().join(".kibo.toml");
        
        let config_content = r#"
            directories = ["src", "tests"]
            files = ["*.txt"]
            ignore = ["target", "node_modules"]
            max_snapshot_size_gb = 10.5
            compression_level = 3
        "#;
        
        fs::write(&config_path, config_content).unwrap();
        
        let config = Config::load(&config_path).unwrap();
        assert_eq!(config.directories, vec!["src", "tests"]);
        assert_eq!(config.files, vec!["*.txt"]);
        assert_eq!(config.ignore, vec!["target", "node_modules"]);
        assert_eq!(config.max_snapshot_size_gb, Some(10.5));
        assert_eq!(config.compression_level, 3);
    }

    #[test]
    fn test_config_load_nonexistent_file() {
        let temp_dir = TempDir::new().unwrap();
        let config_path = temp_dir.path().join(".kibo.toml");
        
        let result = Config::load(&config_path);
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("not found"));
    }

    #[test]
    fn test_config_load_invalid_toml() {
        let temp_dir = TempDir::new().unwrap();
        let config_path = temp_dir.path().join(".kibo.toml");
        
        fs::write(&config_path, "invalid toml content {{{").unwrap();
        
        let result = Config::load(&config_path);
        assert!(result.is_err());
    }

    #[test]
    fn test_config_validate_empty_lists_fails() {
        let config = Config {
            directories: vec![],
            files: vec![],
            ..Default::default()
        };
        
        let result = config.validate();
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("empty"));
    }

    #[test]
    fn test_config_validate_with_directories_succeeds() {
        let config = Config {
            directories: vec!["src".to_string()],
            ..Default::default()
        };
        
        let result = config.validate();
        assert!(result.is_ok());
    }

    #[test]
    fn test_config_validate_with_files_succeeds() {
        let config = Config {
            files: vec!["*.txt".to_string()],
            ..Default::default()
        };
        
        let result = config.validate();
        assert!(result.is_ok());
    }

    #[test]
    fn test_config_validate_empty_path_fails() {
        let config = Config {
            directories: vec!["".to_string()],
            ..Default::default()
        };
        
        let result = config.validate();
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("empty path"));
    }

    #[test]
    fn test_config_validate_dotdot_path_fails() {
        let config = Config {
            directories: vec!["../etc".to_string()],
            ..Default::default()
        };
        
        let result = config.validate();
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains(".."));
    }

    #[test]
    fn test_effective_compression_level_capped_at_10() {
        let config = Config {
            compression_level: 15,
            ..Default::default()
        };
        
        assert_eq!(config.effective_compression_level(), 10);
    }

    #[test]
    fn test_effective_compression_level_under_cap() {
        let config = Config {
            compression_level: 5,
            ..Default::default()
        };
        
        assert_eq!(config.effective_compression_level(), 5);
    }

    #[test]
    fn test_should_ignore_exact_match() {
        let config = Config {
            ignore: vec!["target".to_string()],
            ..Default::default()
        };
        
        assert!(config.should_ignore(Path::new("target")));
        assert!(config.should_ignore(Path::new("target/debug")));
    }

    #[test]
    fn test_should_ignore_glob_pattern() {
        let config = Config {
            ignore: vec!["*.log".to_string()],
            ..Default::default()
        };
        
        assert!(config.should_ignore(Path::new("test.log")));
        assert!(!config.should_ignore(Path::new("test.txt")));
    }

    #[test]
    fn test_should_ignore_component_match() {
        let config = Config {
            ignore: vec!["node_modules".to_string()],
            ..Default::default()
        };
        
        assert!(config.should_ignore(Path::new("node_modules")));
        assert!(config.should_ignore(Path::new("project/node_modules")));
        assert!(config.should_ignore(Path::new("project/node_modules/package")));
    }

    #[test]
    fn test_should_ignore_no_match() {
        let config = Config {
            ignore: vec!["target".to_string()],
            ..Default::default()
        };
        
        assert!(!config.should_ignore(Path::new("src")));
        assert!(!config.should_ignore(Path::new("tests")));
    }

    #[test]
    fn test_find_repo_root_in_current_dir() {
        let temp_dir = TempDir::new().unwrap();
        let config_path = temp_dir.path().join(CONFIG_FILENAME);
        fs::write(&config_path, "directories = [\"src\"]").unwrap();
        
        std::env::set_current_dir(temp_dir.path()).unwrap();
        
        let root = find_repo_root().unwrap();
        let expected = temp_dir.path().canonicalize().unwrap();
        assert_eq!(root.canonicalize().unwrap(), expected);
    }

    #[test]
    fn test_find_repo_root_in_parent_dir() {
        let temp_dir = TempDir::new().unwrap();
        let config_path = temp_dir.path().join(CONFIG_FILENAME);
        fs::write(&config_path, "directories = [\"src\"]").unwrap();
        
        let subdir = temp_dir.path().join("subdir");
        fs::create_dir(&subdir).unwrap();
        
        std::env::set_current_dir(&subdir).unwrap();
        
        let root = find_repo_root().unwrap();
        let expected = temp_dir.path().canonicalize().unwrap();
        assert_eq!(root.canonicalize().unwrap(), expected);
    }

    #[test]
    fn test_get_kibo_dir() {
        let root = Path::new("/project");
        let kibo_dir = get_kibo_dir(root);
        assert_eq!(kibo_dir, PathBuf::from("/project/.kibo"));
    }

    #[test]
    fn test_get_store_dir() {
        let root = Path::new("/project");
        let store_dir = get_store_dir(root);
        assert_eq!(store_dir, PathBuf::from("/project/.kibo/store"));
    }

    #[test]
    fn test_get_manifests_dir() {
        let root = Path::new("/project");
        let manifests_dir = get_manifests_dir(root);
        assert_eq!(manifests_dir, PathBuf::from("/project/.kibo/manifests"));
    }

    #[test]
    fn test_get_hash_cache_path() {
        let root = Path::new("/project");
        let cache_path = get_hash_cache_path(root);
        assert_eq!(cache_path, PathBuf::from("/project/.kibo/hash_cache.json"));
    }

    #[test]
    fn test_database_config_custom_values() {
        let db_config = DatabaseConfig {
            db_type: "mysql".to_string(),
            user: "admin".to_string(),
            password: "secret".to_string(),
            host: "db.example.com".to_string(),
            port: 3307,
            name: "production".to_string(),
            tables: vec!["users".to_string(), "posts".to_string()],
            single_transaction: false,
        };
        
        assert_eq!(db_config.db_type, "mysql");
        assert_eq!(db_config.user, "admin");
        assert_eq!(db_config.password, "secret");
        assert_eq!(db_config.host, "db.example.com");
        assert_eq!(db_config.port, 3307);
        assert_eq!(db_config.name, "production");
        assert_eq!(db_config.tables.len(), 2);
        assert!(!db_config.single_transaction);
    }

    #[test]
    fn test_config_with_database() {
        let temp_dir = TempDir::new().unwrap();
        let config_path = temp_dir.path().join(".kibo.toml");
        
        let config_content = r#"
            directories = ["src"]
                
            [database]
            user = "testuser"
            password = "testpass"
            name = "testdb"
        "#;
        
        fs::write(&config_path, config_content).unwrap();
        
        let config = Config::load(&config_path).unwrap();
        assert!(config.database.is_some());
        
        let db = config.database.unwrap();
        assert_eq!(db.user, "testuser");
        assert_eq!(db.password, "testpass");
        assert_eq!(db.name, "testdb");
    }
}