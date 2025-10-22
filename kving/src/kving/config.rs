use std::path::PathBuf;

#[derive(Debug, Clone)]
pub enum StoreModel {
    Bitcask,
    // todo more model
}

impl StoreModel {
    /// Returns the file extension associated with the storage model.
    ///
    /// # Returns
    ///
    /// A string representing the file extension for this storage model
    pub fn extension(&self) -> String {
        match self {
            StoreModel::Bitcask => String::from("bsk"),
        }
    }

    /// Creates a StoreModel instance from an integer index.
    /// Currently only Bitcask is supported, so any index will return Bitcask.
    ///
    /// # Arguments
    ///
    /// * `index` - The integer index representing the storage model
    ///
    /// # Returns
    ///
    /// The corresponding StoreModel variant (currently always Bitcask)
    pub fn with_index(index: i32) -> StoreModel {
        match index {
            0 => Self::Bitcask,
            _ => Self::Bitcask,
        }
    }
}

#[derive(Debug, Clone)]
pub struct Config {
    data_dir: PathBuf,
    name: String,
    max_file_size: u64,
    max_file_handle_caches: u32,
    max_historical_files: u32,
    strict_crc_validation: bool,
    store_model: StoreModel,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            data_dir: PathBuf::from("data"),
            name: String::from("bitcask"),
            max_file_size: 8 * 1024 * 1024,
            max_file_handle_caches: 30,
            max_historical_files: 5,
            strict_crc_validation: false,
            store_model: StoreModel::Bitcask,
        }
    }
}

impl Config {
    /// Get the data directory path.
    pub fn data_dir(&self) -> &PathBuf {
        &self.data_dir
    }

    /// Get the configuration name.
    pub fn name(&self) -> &String {
        &self.name
    }

    /// Get the full database path by joining data directory with name.
    /// Internal crate visibility only.
    pub(crate) fn database_path(&self) -> PathBuf {
        (&self.data_dir).join(&self.name)
    }

    /// Get the maximum file size in bytes.
    pub fn max_file_size(&self) -> u64 {
        self.max_file_size
    }

    /// Get the maximum number of file handle caches.
    pub fn max_file_handle_caches(&self) -> u32 {
        self.max_file_handle_caches
    }

    /// Get the maximum number of historical files to retain.
    pub fn max_historical_files(&self) -> u32 {
        self.max_historical_files
    }

    /// Check if strict CRC validation is enabled.
    /// When enabled, performs more rigorous data integrity checks.
    pub fn strict_crc_validation(&self) -> bool {
        self.strict_crc_validation
    }

    /// Get the storage model configuration.
    pub fn store_model(&self) -> &StoreModel {
        &self.store_model
    }

    /// Create a new builder for Config.
    pub fn builder() -> Builder {
        Builder::new()
    }
}

pub struct Builder {
    config: Config,
}

impl Builder {
    /// Creates a new Builder instance with default configuration values.
    /// This method is only visible within the current crate.
    pub(crate) fn new() -> Builder {
        Builder {
            config: Config::default(),
        }
    }

    /// Consumes the builder and returns the final Config instance.
    pub fn build(self) -> Config {
        self.config
    }

    /// Sets the data directory path and returns the builder for method chaining.
    ///
    /// # Arguments
    ///
    /// * `dir` - The path to the data directory where files will be stored
    pub fn set_data_dir(mut self, dir: PathBuf) -> Builder {
        self.config.data_dir = dir;
        self
    }

    /// Sets the name identifier and returns the builder for method chaining.
    ///
    /// # Arguments
    ///
    /// * `name` - The name identifier for this configuration (any type convertible to String)
    pub fn set_name<S>(mut self, name: S) -> Builder
    where
        S: Into<String>,
    {
        self.config.name = name.into();
        self
    }

    /// Sets the maximum file size in bytes and returns the builder for method chaining.
    ///
    /// # Arguments
    ///
    /// * `size` - The maximum allowed size for individual files in bytes
    pub fn set_max_file_size(mut self, size: u64) -> Builder {
        self.config.max_file_size = size;
        self
    }

    /// Sets the maximum number of file handle caches and returns the builder for method chaining.
    ///
    /// # Arguments
    ///
    /// * `caches` - The maximum number of file handle caches to maintain
    pub fn set_max_file_handle_caches(mut self, caches: u32) -> Builder {
        self.config.max_file_handle_caches = caches;
        self
    }

    /// Sets the maximum number of historical files to retain and returns the builder for method chaining.
    ///
    /// # Arguments
    ///
    /// * `files` - The maximum number of historical files to retain
    pub fn set_max_historical_files(mut self, files: u32) -> Builder {
        self.config.max_historical_files = files;
        self
    }

    /// Enables or disables strict CRC validation and returns the builder for method chaining.
    ///
    /// # Arguments
    ///
    /// * `validation` - Whether to enable strict CRC validation
    pub fn set_strict_crc_validation(mut self, validation: bool) -> Builder {
        self.config.strict_crc_validation = validation;
        self
    }

    /// Sets the storage model and returns the builder for method chaining.
    ///
    /// # Arguments
    ///
    /// * `model` - The storage model configuration to use
    pub fn set_store_model(mut self, model: StoreModel) -> Builder {
        self.config.store_model = model;
        self
    }
}
