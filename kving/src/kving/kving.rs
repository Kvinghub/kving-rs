use crate::bitcask::bitcask::Bitcask;
use crate::kving::config::Config;
use crate::kving::kv_store::KvStore;
use core::f32;
use std::{
    f64, isize,
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
};

pub struct Kving {
    store: Arc<Box<dyn KvStore>>,
    is_merging: Arc<AtomicBool>,
}

unsafe impl Send for Kving {}

unsafe impl Sync for Kving {}

impl Kving {
    pub fn with_config(config: Config) -> crate::Result<Self> {
        let kving = Self {
            store: Arc::new(Box::new(Bitcask::with_config(config)?)),
            is_merging: Arc::new(AtomicBool::new(false)),
        };
        // kving.merge_transactions()?;
        Ok(kving)
    }

    pub fn get_isize<K>(&self, key: K) -> Option<isize>
    where
        K: AsRef<str>,
    {
        let key = key.as_ref();
        let result = self.get(key.as_bytes()).ok()?;
        if let Some(value) = result {
            match value.try_into() {
                Ok(bytes) => Some(isize::from_be_bytes(bytes)),
                Err(_) => None,
            }
        } else {
            None
        }
    }

    pub fn get_usize<K>(&self, key: K) -> Option<usize>
    where
        K: AsRef<str>,
    {
        let key = key.as_ref();
        let result = self.get(key.as_bytes()).ok()?;
        if let Some(value) = result {
            match value.try_into() {
                Ok(bytes) => Some(usize::from_be_bytes(bytes)),
                Err(_) => None,
            }
        } else {
            None
        }
    }

    pub fn get_f32<K>(&self, key: K) -> Option<f32>
    where
        K: AsRef<str>,
    {
        let key = key.as_ref();
        let result = self.get(key.as_bytes()).ok()?;
        if let Some(value) = result {
            match value.try_into() {
                Ok(bytes) => Some(f32::from_be_bytes(bytes)),
                Err(_) => None,
            }
        } else {
            None
        }
    }

    pub fn get_f64<K>(&self, key: K) -> Option<f64>
    where
        K: AsRef<str>,
    {
        let key = key.as_ref();
        let result = self.get(key.as_bytes()).ok()?;
        if let Some(value) = result {
            match value.try_into() {
                Ok(bytes) => Some(f64::from_be_bytes(bytes)),
                Err(_) => None,
            }
        } else {
            None
        }
    }

    pub fn get_bool<K>(&self, key: K) -> Option<bool>
    where
        K: AsRef<str>,
    {
        let key = key.as_ref();
        let result = self.get(key.as_bytes()).ok()?;
        if let Some(value) = result {
            if value.len() == 1 {
                Some(if value[0] == 1 { true } else { false })
            } else {
                None
            }
        } else {
            None
        }
    }

    pub fn get_string<K>(&self, key: K) -> Option<String>
    where
        K: AsRef<str>,
    {
        let key = key.as_ref();
        let result = self.get(key.as_bytes()).ok()?;
        if let Some(value) = result {
            match String::from_utf8(value) {
                Ok(str) => Some(str),
                Err(_) => None,
            }
        } else {
            None
        }
    }

    pub fn get_blob<K>(&self, key: K) -> Option<Vec<u8>>
    where
        K: AsRef<str>,
    {
        let key = key.as_ref();
        self.get(key.as_bytes()).ok()?
    }

    pub fn put_isize<K>(&self, key: K, value: isize) -> crate::Result<()>
    where
        K: AsRef<str>,
    {
        let key = key.as_ref();
        let value = value.to_be_bytes();
        self.put(key.as_bytes(), &value.as_slice())
    }

    pub fn put_usize<K>(&self, key: K, value: usize) -> crate::Result<()>
    where
        K: AsRef<str>,
    {
        let key = key.as_ref();
        let value = value.to_be_bytes();
        self.put(key.as_bytes(), &value.as_slice())
    }

    pub fn put_f32<K>(&self, key: K, value: f32) -> crate::Result<()>
    where
        K: AsRef<str>,
    {
        let key = key.as_ref();
        let value = value.to_be_bytes();
        self.put(key.as_bytes(), &value.as_slice())
    }

    pub fn put_f64<K>(&self, key: K, value: f64) -> crate::Result<()>
    where
        K: AsRef<str>,
    {
        let key = key.as_ref();
        let value = value.to_be_bytes();
        self.put(key.as_bytes(), &value.as_slice())
    }

    pub fn put_bool<K>(&self, key: K, value: bool) -> crate::Result<()>
    where
        K: AsRef<str>,
    {
        let key = key.as_ref();
        let value = if value { [1] } else { [0] };
        self.put(key.as_bytes(), &value)
    }

    pub fn put_string<K, V>(&self, key: K, value: V) -> crate::Result<()>
    where
        K: AsRef<str>,
        V: AsRef<str>,
    {
        let key = key.as_ref();
        let value = value.as_ref();
        self.put(key.as_bytes(), value.as_bytes())
    }

    pub fn put_blob<K>(&self, key: K, value: &[u8]) -> crate::Result<()>
    where
        K: AsRef<str>,
    {
        let key = key.as_ref();
        self.put(key.as_bytes(), value)
    }

    pub fn delete<K>(&self, key: K) -> crate::Result<()>
    where
        K: AsRef<str>,
    {
        (self as &dyn KvStore).delete(key.as_ref().as_bytes())
    }

    pub fn contains<K>(&self, key: K) -> crate::Result<bool>
    where
        K: AsRef<str>,
    {
        (self as &dyn KvStore).contains(key.as_ref().as_bytes())
    }

    pub fn sync<K>(&self) -> crate::Result<()> {
        (self as &dyn KvStore).sync()
    }

    pub fn list_keys(&self) -> crate::Result<Vec<String>> {
        let keys = (self as &dyn KvStore)
            .list_keys()?
            .iter()
            .map(|k| String::from_utf8(k.to_vec()).unwrap())
            .collect::<Vec<String>>();
        Ok(keys)
    }

    pub fn close(&self) -> crate::Result<()> {
        (self as &dyn KvStore).close()
    }

    fn merge_transactions(&self) -> crate::Result<()> {
        if !self.can_merge()? {
            return Ok(());
        }

        if self
            .is_merging
            .compare_exchange(false, true, Ordering::Acquire, Ordering::Relaxed)
            .is_err()
        {
            return Ok(());
        }

        let store_clone = Arc::clone(&self.store);
        let is_merging_clone = Arc::clone(&self.is_merging);

        std::thread::spawn(move || {
            if let Err(e) = store_clone.merge() {
                eprintln!("{:?}", e)
            }
            is_merging_clone.store(false, Ordering::Release);
        });

        Ok(())
    }
}

impl KvStore for Kving {
    fn get(&self, key: &[u8]) -> crate::Result<Option<Vec<u8>>> {
        self.store.get(key)
    }

    fn put(&self, key: &[u8], value: &[u8]) -> crate::Result<()> {
        self.store.put(key, value)?;
        self.merge_transactions()
    }

    fn delete(&self, key: &[u8]) -> crate::Result<()> {
        self.store.delete(key)
        // self.merge_transactions()
    }

    fn contains(&self, key: &[u8]) -> crate::Result<bool> {
        self.store.contains(key)
    }

    fn list_keys(&self) -> crate::Result<Vec<Vec<u8>>> {
        self.store.list_keys()
    }

    fn sync(&self) -> crate::Result<()> {
        self.store.sync()
    }

    fn can_merge(&self) -> crate::Result<bool> {
        self.store.can_merge()
    }

    fn merge(&self) -> crate::Result<()> {
        self.store.merge()
    }

    fn close(&self) -> crate::Result<()> {
        self.store.close()
    }
}
