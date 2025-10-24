pub trait KvStore: Send + Sync {
    fn get(&self, key: &[u8]) -> crate::Result<Option<Vec<u8>>>;

    fn put(&self, key: &[u8], value: &[u8]) -> crate::Result<()>;

    fn delete(&self, key: &[u8]) -> crate::Result<()>;

    fn contains(&self, key: &[u8]) -> crate::Result<bool>;

    fn list_keys(&self) -> crate::Result<Vec<Vec<u8>>>;

    fn clear(&self) -> crate::Result<()>;

    fn sync(&self) -> crate::Result<()>;

    fn can_merge(&self) -> crate::Result<bool>;

    fn merge(&self) -> crate::Result<()>;

    fn close(&self) -> crate::Result<()>;
}
