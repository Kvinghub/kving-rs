use std::path::PathBuf;

#[derive(Debug, Clone)]
pub enum StoreModel {
    Bitcask,
    // todo more model
}
impl StoreModel {
    pub fn extension(&self) -> String {
        match self {
            StoreModel::Bitcask => String::from("bsk"),
        }
    }
}

#[derive(Debug, Clone)]
pub struct Config {
    data_dir: PathBuf,
    name: String,
    max_file_size: u64,
    max_file_cache_handles: u32,
    max_historical_files: u32,
    strict_crc_validation: bool,
    store_model: StoreModel,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            data_dir: PathBuf::from("data"),
            name: String::from("bitcask"),
            max_file_size: 1024 * 1024 * 1024 * 1024,
            max_file_cache_handles: 100,
            max_historical_files: 10,
            strict_crc_validation: false,
            store_model: StoreModel::Bitcask,
        }
    }
}

impl Config {
    pub fn data_dir(&self) -> &PathBuf {
        &self.data_dir
    }

    pub fn name(&self) -> &String {
        &self.name
    }

    pub(crate) fn database_path(&self) -> PathBuf {
        (&self.data_dir).join(&self.name)
    }

    pub fn max_file_size(&self) -> u64 {
        self.max_file_size
    }

    pub fn max_file_cache_handles(&self) -> u32 {
        self.max_file_cache_handles
    }

    pub fn max_historical_files(&self) -> u32 {
        self.max_historical_files
    }

    pub fn strict_crc_validation(&self) -> bool {
        self.strict_crc_validation
    }

    pub fn store_model(&self) -> &StoreModel {
        &self.store_model
    }

    pub fn builder() -> Builder {
        Builder::new()
    }
}

pub struct Builder {
    config: Config,
}

impl Builder {
    pub(crate) fn new() -> Builder {
        Builder {
            config: Config::default(),
        }
    }

    pub fn build(self) -> Config {
        self.config
    }

    pub fn set_data_dir(mut self, dir: PathBuf) -> Builder {
        self.config.data_dir = dir;
        self
    }

    pub fn set_name<S>(mut self, name: S) -> Builder
    where
        S: Into<String>,
    {
        self.config.name = name.into();
        self
    }

    pub fn set_max_file_size(mut self, size: u64) -> Builder {
        self.config.max_file_size = size;
        self
    }

    pub fn set_max_file_handles(mut self, handles: u32) -> Builder {
        self.config.max_file_cache_handles = handles;
        self
    }

    pub fn set_max_historical_files(mut self, value: u32) -> Builder {
        self.config.max_historical_files = value;
        self
    }

    pub fn set_strict_crc_validation(mut self, validation: bool) -> Builder {
        self.config.strict_crc_validation = validation;
        self
    }

    pub fn set_store_model(mut self, model: StoreModel) -> Builder {
        self.config.store_model = model;
        self
    }
}
