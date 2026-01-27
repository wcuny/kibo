use indicatif::{ProgressBar, ProgressStyle, ProgressDrawTarget};
use std::time::{Duration, Instant};

/// Progress configuration for determining whether to show progress bars
#[derive(Debug, Clone, Copy)]
pub enum ProgressConfig {
    /// Auto-detect based on TTY
    Auto,
    /// Force enable progress bars
    ForceEnable,
    /// Force disable progress bars
    ForceDisable,
}

impl ProgressConfig {
    /// Create a progress config from CLI flags and config file
    pub fn from_flags(progress_flag: bool, no_progress_flag: bool, config_value: Option<bool>) -> Self {
        if progress_flag {
            ProgressConfig::ForceEnable
        } 
        else if no_progress_flag {
            ProgressConfig::ForceDisable
        }
        else if let Some(config_val) = config_value {
            if config_val {
                ProgressConfig::ForceEnable
            }
            else {
                ProgressConfig::ForceDisable
            }
        }
        else {
            ProgressConfig::Auto
        }
    }

    /// Determine if progress should be shown based on configuration
    pub fn should_show_progress(&self) -> bool {
        match self {
            ProgressConfig::Auto => atty::is(atty::Stream::Stderr),
            ProgressConfig::ForceEnable => true,
            ProgressConfig::ForceDisable => false,
        }
    }
}

/// Timer for tracking operation duration
#[derive(Debug, Clone)]
pub struct Timer {
    start: Instant,
}

impl Timer {
    /// Create and start a new timer
    pub fn new() -> Self {
        Self {
            start: Instant::now(),
        }
    }

    /// Get elapsed time in seconds as a formatted string
    pub fn elapsed_string(&self) -> String {
        let elapsed = self.start.elapsed();
        format!("{:.2}s", elapsed.as_secs_f64())
    }
}

/// Progress tracker for byte-based operations
#[derive(Debug, Clone)]
pub struct ByteProgress {
    bar: Option<ProgressBar>,
    timer: Timer,
    enabled: bool,
}

impl ByteProgress {
    /// Create a new progress tracker for byte-based operations
    pub fn new(total_bytes: u64, config: ProgressConfig) -> Self {
        let enabled = config.should_show_progress();
        
        let bar = if enabled {
            let pb = ProgressBar::new(total_bytes);
            pb.set_draw_target(ProgressDrawTarget::stderr());
            pb.set_style(
                ProgressStyle::default_bar()
                    .template("{spinner:.green} [{elapsed_precise}] [{bar:40.cyan/blue}] {bytes}/{total_bytes} ({eta})")
                    .expect("Invalid progress template")
                    .progress_chars("#>-")
            );
            Some(pb)
        }
        else {
            None
        };

        Self {
            bar,
            timer: Timer::new(),
            enabled,
        }
    }

    /// Update progress by adding bytes processed
    pub fn inc(&self, bytes: u64) {
        if let Some(ref bar) = self.bar {
            bar.inc(bytes);
        }
    }

    /// Finish the progress bar and return elapsed time
    pub fn finish(self) -> String {
        if let Some(bar) = self.bar {
            bar.finish_and_clear();
        }
        self.timer.elapsed_string()
    }

    /// Check if progress is enabled
    pub fn is_enabled(&self) -> bool {
        self.enabled
    }
}

/// Progress tracker for item-based operations (e.g., file count)
pub struct ItemProgress {
    bar: Option<ProgressBar>,
    timer: Timer,
    enabled: bool,
}

impl ItemProgress {
    /// Create a new progress tracker for item-based operations
    pub fn new(total_items: u64, config: ProgressConfig, item_name: &str) -> Self {
        let enabled = config.should_show_progress();
        
        let bar = if enabled {
            let pb = ProgressBar::new(total_items);
            pb.set_draw_target(ProgressDrawTarget::stderr());
            let template = format!(
                "{{spinner:.green}} [{{elapsed_precise}}] [{{bar:40.cyan/blue}}] {{pos}}/{{len}} {} ({{eta}})",
                item_name
            );
            pb.set_style(
                ProgressStyle::default_bar()
                    .template(&template)
                    .expect("Invalid progress template")
                    .progress_chars("#>-")
            );
            Some(pb)
        }
        else {
            None
        };

        Self {
            bar,
            timer: Timer::new(),
            enabled,
        }
    }

    /// Increment progress by one item
    pub fn inc(&self, count: u64) {
        if let Some(ref bar) = self.bar {
            bar.inc(count);
        }
    }

    /// Set a message on the progress bar
    pub fn set_message(&self, msg: String) {
        if let Some(ref bar) = self.bar {
            bar.set_message(msg);
        }
    }

    /// Finish the progress bar and return elapsed time
    pub fn finish(self) -> String {
        if let Some(bar) = self.bar {
            bar.finish_and_clear();
        }
        self.timer.elapsed_string()
    }

    /// Check if progress is enabled
    pub fn is_enabled(&self) -> bool {
        self.enabled
    }
}

/// Spinner for indeterminate operations
pub struct Spinner {
    bar: Option<ProgressBar>,
    enabled: bool,
}

impl Spinner {
    /// Create a new spinner with a message
    pub fn new(config: ProgressConfig, message: &str) -> Self {
        let enabled = config.should_show_progress();
        
        let bar = if enabled {
            let pb = ProgressBar::new_spinner();
            pb.set_draw_target(ProgressDrawTarget::stderr());
            pb.set_style(
                ProgressStyle::default_spinner()
                    .template("{spinner:.green} {msg}")
                    .expect("Invalid spinner template")
            );
            pb.set_message(message.to_string());
            pb.enable_steady_tick(Duration::from_millis(100));
            Some(pb)
        }
        else {
            None
        };

        Self { bar, enabled }
    }

    /// Finish and clear the spinner
    pub fn finish(self) {
        if let Some(bar) = self.bar {
            bar.finish_and_clear();
        }
    }

    /// Check if spinner is enabled
    pub fn is_enabled(&self) -> bool {
        self.enabled
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_progress_config_auto() {
        let config = ProgressConfig::from_flags(false, false, None);
        matches!(config, ProgressConfig::Auto);
    }

    #[test]
    fn test_progress_config_force_enable() {
        let config = ProgressConfig::from_flags(true, false, None);
        matches!(config, ProgressConfig::ForceEnable);
        assert!(config.should_show_progress());
    }

    #[test]
    fn test_progress_config_force_disable() {
        let config = ProgressConfig::from_flags(false, true, None);
        matches!(config, ProgressConfig::ForceDisable);
        assert!(!config.should_show_progress());
    }

    #[test]
    fn test_progress_config_from_config_file() {
        let config = ProgressConfig::from_flags(false, false, Some(true));
        assert!(config.should_show_progress());

        let config = ProgressConfig::from_flags(false, false, Some(false));
        assert!(!config.should_show_progress());
    }

    #[test]
    fn test_progress_config_cli_flag_overrides_config() {
        let config = ProgressConfig::from_flags(true, false, Some(false));
        assert!(config.should_show_progress());

        let config = ProgressConfig::from_flags(false, true, Some(true));
        assert!(!config.should_show_progress());
    }

    #[test]
    fn test_timer_new() {
        let timer = Timer::new();
        std::thread::sleep(Duration::from_millis(10));
        let elapsed = timer.elapsed_string();
        assert!(elapsed.ends_with('s'));
    }

    #[test]
    fn test_timer_elapsed_string_format() {
        let timer = Timer::new();
        std::thread::sleep(Duration::from_millis(100));
        let elapsed = timer.elapsed_string();
        assert!(elapsed.contains("0.1") || elapsed.contains("0.0"));
        assert!(elapsed.ends_with('s'));
    }

    #[test]
    fn test_byte_progress_disabled() {
        let progress = ByteProgress::new(1000, ProgressConfig::ForceDisable);
        assert!(!progress.is_enabled());
        assert!(progress.bar.is_none());
    }

    #[test]
    fn test_byte_progress_enabled() {
        let progress = ByteProgress::new(1000, ProgressConfig::ForceEnable);
        assert!(progress.is_enabled());
        assert!(progress.bar.is_some());
    }

    #[test]
    fn test_byte_progress_inc() {
        let progress = ByteProgress::new(1000, ProgressConfig::ForceDisable);
        progress.inc(100);
        // Should not panic even when disabled
    }

    #[test]
    fn test_byte_progress_finish() {
        let progress = ByteProgress::new(1000, ProgressConfig::ForceDisable);
        let elapsed = progress.finish();
        assert!(elapsed.ends_with('s'));
    }

    #[test]
    fn test_item_progress_disabled() {
        let progress = ItemProgress::new(100, ProgressConfig::ForceDisable, "files");
        assert!(!progress.is_enabled());
        assert!(progress.bar.is_none());
    }

    #[test]
    fn test_item_progress_enabled() {
        let progress = ItemProgress::new(100, ProgressConfig::ForceEnable, "files");
        assert!(progress.is_enabled());
        assert!(progress.bar.is_some());
    }

    #[test]
    fn test_item_progress_inc() {
        let progress = ItemProgress::new(100, ProgressConfig::ForceDisable, "files");
        progress.inc(1);
        progress.inc(5);
        // Should not panic even when disabled
    }

    #[test]
    fn test_item_progress_set_message() {
        let progress = ItemProgress::new(100, ProgressConfig::ForceDisable, "files");
        progress.set_message("Processing file.txt".to_string());
        // Should not panic even when disabled
    }

    #[test]
    fn test_item_progress_finish() {
        let progress = ItemProgress::new(100, ProgressConfig::ForceDisable, "files");
        let elapsed = progress.finish();
        assert!(elapsed.ends_with('s'));
    }

    #[test]
    fn test_spinner_disabled() {
        let spinner = Spinner::new(ProgressConfig::ForceDisable, "Loading...");
        assert!(!spinner.is_enabled());
        assert!(spinner.bar.is_none());
    }

    #[test]
    fn test_spinner_enabled() {
        let spinner = Spinner::new(ProgressConfig::ForceEnable, "Loading...");
        assert!(spinner.is_enabled());
        assert!(spinner.bar.is_some());
    }

    #[test]
    fn test_spinner_finish() {
        let spinner = Spinner::new(ProgressConfig::ForceDisable, "Loading...");
        spinner.finish();
        // Should not panic even when disabled
    }

    #[test]
    fn test_progress_config_precedence() {
        // Progress flag has highest priority
        let config = ProgressConfig::from_flags(true, true, Some(false));
        assert!(config.should_show_progress());
    }

    #[test]
    fn test_byte_progress_zero_total() {
        let progress = ByteProgress::new(0, ProgressConfig::ForceEnable);
        assert!(progress.is_enabled());
        progress.inc(0);
        let _ = progress.finish();
    }

    #[test]
    fn test_item_progress_zero_total() {
        let progress = ItemProgress::new(0, ProgressConfig::ForceEnable, "items");
        assert!(progress.is_enabled());
        progress.inc(0);
        let _ = progress.finish();
    }

    #[test]
    fn test_item_progress_custom_name() {
        let progress1 = ItemProgress::new(10, ProgressConfig::ForceEnable, "files");
        let progress2 = ItemProgress::new(10, ProgressConfig::ForceEnable, "directories");
        
        assert!(progress1.is_enabled());
        assert!(progress2.is_enabled());
    }
}

