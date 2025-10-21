mod kving {
    pub mod config;
    pub mod errors;
    pub mod kv_store;
    pub mod kving;
}

mod bitcask {
    pub mod bitcask;
}

pub type Result<T> = core::result::Result<T, Error>;
pub use kving::config::*;
pub use kving::errors::*;
pub use kving::kving::*;
