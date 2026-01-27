use anyhow::{Context, Result};
use chrono::Utc;
use serde::{Deserialize, Serialize};
use std::fs::{File, OpenOptions};
use std::io::{BufRead, BufReader, Write};
use std::path::Path;

use crate::config;

/// History entry representing a single operation
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HistoryEntry {
    /// Timestamp in UTC (ISO 8601)
    pub timestamp: String,
    /// Command executed (SAVE, LOAD, RM)
    pub command: String,
    /// Snapshot name (if applicable)
    pub snapshot: Option<String>,
    /// Flags used (e.g., "--include-db", "--yes")
    pub flags: Vec<String>,
}

impl HistoryEntry {
    /// Create a new history entry with current timestamp
    pub fn new(command: &str, snapshot: Option<&str>, flags: Vec<String>) -> Self {
        let timestamp = Utc::now().to_rfc3339();
        Self {
            timestamp,
            command: command.to_uppercase(),
            snapshot: snapshot.map(|s| s.to_string()),
            flags,
        }
    }

    /// Format entry as a human-readable line
    pub fn to_line(&self) -> String {
        let mut parts = vec![self.timestamp.clone(), self.command.clone()];
        
        if let Some(ref snap) = self.snapshot {
            parts.push(snap.clone());
        }
        
        for flag in &self.flags {
            parts.push(flag.clone());
        }
        
        parts.join(" ")
    }

    /// Parse a line into a history entry
    pub fn from_line(line: &str) -> Option<Self> {
        let parts: Vec<&str> = line.split_whitespace().collect();
        if parts.len() < 2 {
            return None;
        }

        let timestamp = parts[0].to_string();
        let command = parts[1].to_string();
        
        let (snapshot, flags_start) = if parts.len() > 2 && !parts[2].starts_with("--") {
            (Some(parts[2].to_string()), 3)
        }
        else {
            (None, 2)
        };

        let flags = parts[flags_start..].iter().map(|s| s.to_string()).collect();

        Some(Self {
            timestamp,
            command,
            snapshot,
            flags,
        })
    }

    /// Format entry for display
    pub fn display(&self) -> String {
        let mut result = format!("{} {:8}", self.timestamp, self.command);
        
        if let Some(ref snap) = self.snapshot {
            result.push_str(&format!(" {:<20}", snap));
        }
        else {
            result.push_str(&format!(" {:<20}", ""));
        }
        
        if !self.flags.is_empty() {
            result.push_str(&format!(" {}", self.flags.join(" ")));
        }
        
        result
    }
}

/// Log a history entry to the history file
pub fn log_entry(root: &Path, entry: &HistoryEntry) {
    let history_path = root.join(config::KIBO_DIR).join(config::HISTORY_LOG_FILE);
    
    if let Some(parent) = history_path.parent() {
        let _ = std::fs::create_dir_all(parent);
    }

    let result = OpenOptions::new()
        .create(true)
        .append(true)
        .open(&history_path)
        .and_then(|mut file| {
            writeln!(file, "{}", entry.to_line())?;
            file.flush()
        });

    if let Err(e) = result {
        eprintln!("Warning: Failed to write to history log: {}", e);
    }
}

/// Read all history entries from the log file
pub fn read_history(root: &Path) -> Result<Vec<HistoryEntry>> {
    let history_path = root.join(config::KIBO_DIR).join(config::HISTORY_LOG_FILE);
    
    if !history_path.exists() {
        return Ok(Vec::new());
    }

    let file = File::open(&history_path)
        .with_context(|| format!("Failed to open history file: {}", history_path.display()))?;
    
    let reader = BufReader::new(file);
    let mut entries = Vec::new();

    for line in reader.lines() {
        let line = line?;
        if let Some(entry) = HistoryEntry::from_line(&line) {
            entries.push(entry);
        }
    }

    Ok(entries)
}

/// Filter history entries by snapshot name
pub fn filter_by_snapshot(entries: Vec<HistoryEntry>, snapshot: &str) -> Vec<HistoryEntry> {
    entries
        .into_iter()
        .filter(|e| {
            if let Some(ref snap) = e.snapshot {
                snap == snapshot
            }
            else {
                false
            }
        })
        .collect()
}

/// Get last N entries
pub fn take_last(entries: Vec<HistoryEntry>, n: usize) -> Vec<HistoryEntry> {
    let len = entries.len();
    if len <= n {
        entries
    }
    else {
        entries[len - n..].to_vec()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    #[test]
    fn test_history_entry_new() {
        let entry = HistoryEntry::new("save", Some("snapshot1"), vec!["--include-db".to_string()]);
        
        assert_eq!(entry.command, "SAVE");
        assert_eq!(entry.snapshot, Some("snapshot1".to_string()));
        assert_eq!(entry.flags, vec!["--include-db"]);
        assert!(!entry.timestamp.is_empty());
    }

    #[test]
    fn test_history_entry_new_without_snapshot() {
        let entry = HistoryEntry::new("list", None, vec![]);
        
        assert_eq!(entry.command, "LIST");
        assert_eq!(entry.snapshot, None);
        assert!(entry.flags.is_empty());
    }

    #[test]
    fn test_history_entry_to_line_with_snapshot() {
        let entry = HistoryEntry {
            timestamp: "2026-01-01T12:00:00Z".to_string(),
            command: "SAVE".to_string(),
            snapshot: Some("snapshot1".to_string()),
            flags: vec!["--include-db".to_string()],
        };
        
        let line = entry.to_line();
        assert_eq!(line, "2026-01-01T12:00:00Z SAVE snapshot1 --include-db");
    }

    #[test]
    fn test_history_entry_to_line_without_snapshot() {
        let entry = HistoryEntry {
            timestamp: "2026-01-01T12:00:00Z".to_string(),
            command: "LIST".to_string(),
            snapshot: None,
            flags: vec![],
        };
        
        let line = entry.to_line();
        assert_eq!(line, "2026-01-01T12:00:00Z LIST");
    }

    #[test]
    fn test_history_entry_from_line_with_snapshot() {
        let line = "2026-01-01T12:00:00Z SAVE snapshot1 --include-db";
        let entry = HistoryEntry::from_line(line).unwrap();
        
        assert_eq!(entry.timestamp, "2026-01-01T12:00:00Z");
        assert_eq!(entry.command, "SAVE");
        assert_eq!(entry.snapshot, Some("snapshot1".to_string()));
        assert_eq!(entry.flags, vec!["--include-db"]);
    }

    #[test]
    fn test_history_entry_from_line_without_snapshot() {
        let line = "2026-01-01T12:00:00Z LIST --verbose";
        let entry = HistoryEntry::from_line(line).unwrap();
        
        assert_eq!(entry.timestamp, "2026-01-01T12:00:00Z");
        assert_eq!(entry.command, "LIST");
        assert_eq!(entry.snapshot, None);
        assert_eq!(entry.flags, vec!["--verbose"]);
    }

    #[test]
    fn test_history_entry_from_line_minimal() {
        let line = "2026-01-01T12:00:00Z LOAD";
        let entry = HistoryEntry::from_line(line).unwrap();
        
        assert_eq!(entry.timestamp, "2026-01-01T12:00:00Z");
        assert_eq!(entry.command, "LOAD");
        assert_eq!(entry.snapshot, None);
        assert!(entry.flags.is_empty());
    }

    #[test]
    fn test_history_entry_from_line_invalid() {
        let line = "invalid";
        let entry = HistoryEntry::from_line(line);
        
        assert!(entry.is_none());
    }

    #[test]
    fn test_history_entry_display_with_snapshot() {
        let entry = HistoryEntry {
            timestamp: "2026-01-01T12:00:00Z".to_string(),
            command: "SAVE".to_string(),
            snapshot: Some("snapshot1".to_string()),
            flags: vec!["--include-db".to_string()],
        };
        
        let display = entry.display();
        assert!(display.contains("2026-01-01T12:00:00Z"));
        assert!(display.contains("SAVE"));
        assert!(display.contains("snapshot1"));
        assert!(display.contains("--include-db"));
    }

    #[test]
    fn test_history_entry_display_without_snapshot() {
        let entry = HistoryEntry {
            timestamp: "2026-01-01T12:00:00Z".to_string(),
            command: "LIST".to_string(),
            snapshot: None,
            flags: vec![],
        };
        
        let display = entry.display();
        assert!(display.contains("2026-01-01T12:00:00Z"));
        assert!(display.contains("LIST"));
    }

    #[test]
    fn test_log_entry_creates_file() {
        let temp_dir = TempDir::new().unwrap();
        let root = temp_dir.path();
        
        let entry = HistoryEntry::new("save", Some("snapshot1"), vec![]);
        log_entry(root, &entry);
        
        let history_path = root.join(config::KIBO_DIR).join(config::HISTORY_LOG_FILE);
        assert!(history_path.exists());
    }

    #[test]
    fn test_log_entry_appends_to_file() {
        let temp_dir = TempDir::new().unwrap();
        let root = temp_dir.path();
        
        let entry1 = HistoryEntry::new("save", Some("snapshot1"), vec![]);
        let entry2 = HistoryEntry::new("load", Some("snapshot1"), vec![]);
        
        log_entry(root, &entry1);
        log_entry(root, &entry2);
        
        let entries = read_history(root).unwrap();
        assert_eq!(entries.len(), 2);
    }

    #[test]
    fn test_read_history_empty() {
        let temp_dir = TempDir::new().unwrap();
        let root = temp_dir.path();
        
        let entries = read_history(root).unwrap();
        assert!(entries.is_empty());
    }

    #[test]
    fn test_read_history_with_entries() {
        let temp_dir = TempDir::new().unwrap();
        let root = temp_dir.path();
        
        let entry1 = HistoryEntry::new("save", Some("snapshot1"), vec![]);
        let entry2 = HistoryEntry::new("load", Some("snapshot2"), vec![]);
        
        log_entry(root, &entry1);
        log_entry(root, &entry2);
        
        let entries = read_history(root).unwrap();
        assert_eq!(entries.len(), 2);
        assert_eq!(entries[0].command, "SAVE");
        assert_eq!(entries[1].command, "LOAD");
    }

    #[test]
    fn test_filter_by_snapshot() {
        let entries = vec![
            HistoryEntry {
                timestamp: "2026-01-01T12:00:00Z".to_string(),
                command: "SAVE".to_string(),
                snapshot: Some("snapshot1".to_string()),
                flags: vec![],
            },
            HistoryEntry {
                timestamp: "2026-01-01T13:00:00Z".to_string(),
                command: "SAVE".to_string(),
                snapshot: Some("snapshot2".to_string()),
                flags: vec![],
            },
            HistoryEntry {
                timestamp: "2026-01-01T14:00:00Z".to_string(),
                command: "LOAD".to_string(),
                snapshot: Some("snapshot1".to_string()),
                flags: vec![],
            },
        ];
        
        let filtered = filter_by_snapshot(entries, "snapshot1");
        assert_eq!(filtered.len(), 2);
        assert_eq!(filtered[0].command, "SAVE");
        assert_eq!(filtered[1].command, "LOAD");
    }

    #[test]
    fn test_filter_by_snapshot_no_matches() {
        let entries = vec![
            HistoryEntry {
                timestamp: "2026-01-01T12:00:00Z".to_string(),
                command: "SAVE".to_string(),
                snapshot: Some("snapshot1".to_string()),
                flags: vec![],
            },
        ];
        
        let filtered = filter_by_snapshot(entries, "nonexistent");
        assert!(filtered.is_empty());
    }

    #[test]
    fn test_filter_by_snapshot_excludes_none() {
        let entries = vec![
            HistoryEntry {
                timestamp: "2026-01-01T12:00:00Z".to_string(),
                command: "LIST".to_string(),
                snapshot: None,
                flags: vec![],
            },
        ];
        
        let filtered = filter_by_snapshot(entries, "snapshot1");
        assert!(filtered.is_empty());
    }

    #[test]
    fn test_take_last_all_entries() {
        let entries = vec![
            HistoryEntry::new("save", Some("s1"), vec![]),
            HistoryEntry::new("save", Some("s2"), vec![]),
            HistoryEntry::new("save", Some("s3"), vec![]),
        ];
        
        let result = take_last(entries.clone(), 5);
        assert_eq!(result.len(), 3);
    }

    #[test]
    fn test_take_last_subset() {
        let entries = vec![
            HistoryEntry::new("save", Some("s1"), vec![]),
            HistoryEntry::new("save", Some("s2"), vec![]),
            HistoryEntry::new("save", Some("s3"), vec![]),
            HistoryEntry::new("save", Some("s4"), vec![]),
        ];
        
        let result = take_last(entries, 2);
        assert_eq!(result.len(), 2);
        assert_eq!(result[0].snapshot, Some("s3".to_string()));
        assert_eq!(result[1].snapshot, Some("s4".to_string()));
    }

    #[test]
    fn test_take_last_empty() {
        let entries: Vec<HistoryEntry> = vec![];
        let result = take_last(entries, 5);
        assert!(result.is_empty());
    }
}
