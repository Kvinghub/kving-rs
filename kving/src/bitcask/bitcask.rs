use crate::kving::config::Config;
use crate::kving::kv_store::KvStore;
use byteorder::{ReadBytesExt, WriteBytesExt, BE};
use crc32fast::Hasher;
use dashmap::DashMap;
use lru::LruCache;
use std::fs::{File, OpenOptions};
use std::io::{BufReader, BufWriter, ErrorKind, Read, Seek, SeekFrom, Write};
use std::num::NonZeroUsize;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Mutex, RwLock};
use std::time::{SystemTime, UNIX_EPOCH};

type FileHandleCache = Mutex<LruCache<u64, BufReader<File>>>;

/// KeyDir is a hash table in memory that maps keys to their positions in a data file
type KeyDir = DashMap<Vec<u8>, RecordPos>;

/// The specific location of the RecordPos value in the data file
#[allow(unused)]
struct RecordPos {
    file_id: u64,
    value_size: u64,
    value_pos: u64,
    timestamp: u64,
}

/// The data structure of RecordData stored in a file
struct RecordData {
    crc: u32,
    timestamp: u64,
    key_size: u64,
    value_size: u64,
    key: Vec<u8>,
    value: Vec<u8>,
}

impl RecordData {
    /// RecordData header size: `crc(4) + timestamp(8) + key_size(8) + value_size(8)` bytes len.
    const HEADER_SIZE: u64 = 4 + 8 + 8 + 8;

    /// Tombstone value, indicating deletion
    const TOMBSTONE: &'static [u8] = &[0];

    /// Create a new RecordData instance
    fn new(key: Vec<u8>, value: Vec<u8>) -> Self {
        let timestamp = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("Time went backwards")
            .as_secs();

        Self {
            crc: 0,
            timestamp,
            key_size: key.len() as u64,
            value_size: value.len() as u64,
            key,
            value,
        }
    }

    /// Create a tombstone record for deletion
    fn tombstone(key: Vec<u8>) -> Self {
        Self::new(key, Self::TOMBSTONE.to_vec())
    }

    /// Check if this record is a tombstone
    fn is_tombstone(&self) -> bool {
        self.value_size as usize == Self::TOMBSTONE.len() && self.value == Self::TOMBSTONE
    }

    /// Calculate total record size
    fn total_size(&self) -> u64 {
        Self::HEADER_SIZE + self.key_size + self.value_size
    }

    /// Encode RecordData into a byte array
    fn encode(&self) -> crate::Result<Vec<u8>> {
        let mut buf = Vec::with_capacity(self.total_size() as usize);

        // Reserve CRC position
        buf.write_u32::<BE>(0)?;
        buf.write_u64::<BE>(self.timestamp)?;
        buf.write_u64::<BE>(self.key_size)?;
        buf.write_u64::<BE>(self.value_size)?;
        buf.write_all(&self.key)?;
        buf.write_all(&self.value)?;

        // Calculate CRC and fill in
        let mut hasher = Hasher::new();
        hasher.update(&buf[4..]); // Skip 4 bytes of CRC
        let crc = hasher.finalize();
        (&mut buf[0..4]).write_u32::<BE>(crc)?;

        Ok(buf)
    }
}

pub struct Bitcask {
    config: Config,
    keydir: KeyDir,
    active_file: RwLock<BufWriter<File>>,
    active_file_id: AtomicU64,
    next_file_id: AtomicU64,
    file_ids: RwLock<Vec<u64>>,
    opened_data_file_handles: FileHandleCache,
}

impl Bitcask {
    /// Open bitcask storage engine
    pub fn with_config(config: Config) -> crate::Result<Self> {
        std::fs::create_dir_all(&config.database_path())?;

        let file_ids = Self::get_file_ids(&config)?;
        let (active_file_id, keydir) = Self::load_existing_files(&config, &file_ids)?;
        let active_file = Self::open_append_data_file(&config, active_file_id)?;
        let cap = NonZeroUsize::new(config.max_file_cache_handles() as usize).unwrap();
        let lri_cache = FileHandleCache::new(LruCache::new(cap));

        Ok(Bitcask {
            config,
            keydir,
            active_file: RwLock::new(active_file),
            active_file_id: AtomicU64::new(active_file_id),
            next_file_id: AtomicU64::new(Self::get_timestamp()),
            file_ids: RwLock::new(file_ids),
            opened_data_file_handles: lri_cache,
        })
    }

    /// Load existing files into memory
    fn load_existing_files(config: &Config, file_ids: &Vec<u64>) -> crate::Result<(u64, KeyDir)> {
        if file_ids.is_empty() {
            return Ok((Self::get_timestamp(), DashMap::new()));
        }

        let keydir = DashMap::new();
        for file_id in file_ids {
            Self::process_data_file(config, *file_id, &keydir)?;
        }

        // Every time it is opened, a new active file is generated
        let next_file_id = file_ids.last().map_or(Self::get_timestamp(), |id| *id);
        Ok((next_file_id, keydir))
    }

    /// Process a single data file and populate keydir
    fn process_data_file(config: &Config, file_id: u64, keydir: &KeyDir) -> crate::Result<()> {
        let mut file = Self::open_read_only_data_file(config, file_id)?;
        let mut offset = 0;

        while let Some(record_result) =
            Self::read_next_record(&mut file, offset, config.strict_crc_validation())?
        {
            match record_result {
                Ok((record, record_start_pos)) => {
                    if record.is_tombstone() {
                        keydir.remove(&record.key);
                    } else {
                        let record_pos = RecordPos {
                            file_id,
                            value_size: record.value_size,
                            value_pos: record_start_pos + RecordData::HEADER_SIZE + record.key_size,
                            timestamp: record.timestamp,
                        };
                        keydir.insert(record.key.to_vec(), record_pos);
                    }
                    offset = record_start_pos + record.total_size();
                }
                Err(skip_size) => {
                    offset += skip_size;
                }
            }
            file.seek(SeekFrom::Start(offset))?;
        }

        Ok(())
    }

    /// Read the next record from file, returning either the record or skip size on CRC failure
    fn read_next_record(
        file: &mut BufReader<File>,
        start_offset: u64,
        strict_crc: bool,
    ) -> crate::Result<Option<Result<(RecordData, u64), u64>>> {
        file.seek(SeekFrom::Start(start_offset))?;

        let record_start_pos = start_offset;
        let stored_crc = match file.read_u32::<BE>() {
            Ok(crc) => crc,
            Err(e) if e.kind() == ErrorKind::UnexpectedEof => return Ok(None),
            Err(e) => return Err(e.into()),
        };

        let record = match Self::read_record_data(file) {
            Ok(record) => record,
            Err(e) if e.kind() == ErrorKind::UnexpectedEof => return Ok(None),
            Err(e) => return Err(e.into()),
        };

        // Check CRC
        if stored_crc != record.crc {
            if strict_crc {
                return Err(crate::Error::CorruptedData);
            }

            eprintln!(
                "CRC check failed for record at offset {}, expected: {}, got: {}",
                record_start_pos, stored_crc, record.crc
            );
            return Ok(Some(Err(record.total_size())));
        }

        Ok(Some(Ok((record, record_start_pos))))
    }

    /// Compact existing files into a new file
    fn merge_existing_files(&self) -> crate::Result<()> {
        // Get old file IDs (excluding active file)
        let old_file_ids: Vec<u64> = Self::get_file_ids(&self.config)?
            .into_iter()
            .filter(|&id| id != self.active_file_id.load(Ordering::Relaxed))
            .collect();

        // Merge files
        let merge_file_id = self.next_file_id.load(Ordering::Relaxed);
        self.next_file_id
            .store(Self::get_timestamp(), Ordering::Relaxed);

        let mut merge_file = Self::open_merge_data_file(&self.config, merge_file_id)?;
        let mut new_file_offset = 0;

        let merge_keydir = Self::merge_data_files(
            &self.config,
            &old_file_ids,
            &self.keydir,
            merge_file_id,
            &mut merge_file,
            &mut new_file_offset,
        )?;

        // Finish merge data
        merge_file.flush()?;
        Self::finish_merge_data_file(&self.config, merge_file_id)?;

        // Update keydir and delete old files
        for (key, pos) in merge_keydir {
            self.keydir.insert(key, pos);
        }
        self.delete_data_files(&old_file_ids)?;

        // Refresh file_ids
        let mut file_ids = self
            .file_ids
            .write()
            .map_err(|_| crate::Error::PoisonError("Failed to write file_ids".to_string()))?;
        *file_ids = Self::get_file_ids(&self.config)?;

        Ok(())
    }

    /// Merge multiple data files into one
    fn merge_data_files(
        config: &Config,
        old_file_ids: &[u64],
        keydir: &KeyDir,
        merge_file_id: u64,
        merge_file: &mut BufWriter<File>,
        new_file_offset: &mut u64,
    ) -> crate::Result<KeyDir> {
        let merge_keydir = KeyDir::new();

        for &old_file_id in old_file_ids {
            Self::merge_single_file(
                config,
                old_file_id,
                keydir,
                merge_file_id,
                merge_file,
                new_file_offset,
                &merge_keydir,
            )?;
        }

        Ok(merge_keydir)
    }

    /// Merge a single data file
    fn merge_single_file(
        config: &Config,
        old_file_id: u64,
        keydir: &KeyDir,
        merge_file_id: u64,
        merge_file: &mut BufWriter<File>,
        new_file_offset: &mut u64,
        merge_keydir: &KeyDir,
    ) -> crate::Result<()> {
        let mut file = Self::open_read_only_data_file(config, old_file_id)?;
        let mut old_file_offset = 0;

        while let Some(record_result) =
            Self::read_next_record(&mut file, old_file_offset, config.strict_crc_validation())?
        {
            match record_result {
                Ok((record, record_start_pos)) => {
                    let total_size = record.total_size();
                    if Self::should_merge_record(&record, old_file_id, record_start_pos, keydir) {
                        file.seek(SeekFrom::Start(record_start_pos))?;

                        let mut record_bytes = vec![0; total_size as usize];
                        file.read_exact(&mut record_bytes)?;

                        let bytes_written =
                            merge_file.write_all(&record_bytes).map(|_| total_size)?;

                        let new_record_pos = RecordPos {
                            file_id: merge_file_id, // Note: This should point to the merged new file ID
                            value_size: record.value_size,
                            value_pos: *new_file_offset + RecordData::HEADER_SIZE + record.key_size,
                            timestamp: record.timestamp,
                        };

                        merge_keydir.insert(record.key, new_record_pos);
                        *new_file_offset += bytes_written;
                    }

                    old_file_offset = record_start_pos + total_size;
                }
                Err(skip_size) => {
                    old_file_offset += skip_size;
                }
            }
        }
        merge_file.flush()?;
        Ok(())
    }

    /// Check if a record should be merged (is still valid and current)
    fn should_merge_record(
        record: &RecordData,
        file_id: u64,
        record_start_pos: u64,
        keydir: &KeyDir,
    ) -> bool {
        if let Some(memory_record_pos) = keydir.get(&record.key) {
            memory_record_pos.file_id == file_id
                && memory_record_pos.value_pos
                    == record_start_pos + RecordData::HEADER_SIZE + record.key_size
                && memory_record_pos.timestamp >= record.timestamp
        } else {
            false
        }
    }

    /// Read record data from file (after CRC)
    fn read_record_data(file: &mut BufReader<File>) -> crate::Result<RecordData> {
        let timestamp = file.read_u64::<BE>()?;
        let key_size = file.read_u64::<BE>()?;
        let value_size = file.read_u64::<BE>()?;

        let mut key_buff = vec![0; key_size as usize];
        file.read_exact(&mut key_buff)?;

        let mut value_buf = vec![0; value_size as usize];
        file.read_exact(&mut value_buf)?;

        // Calculate CRC
        let mut hasher = Hasher::new();
        hasher.update(&timestamp.to_be_bytes());
        hasher.update(&key_size.to_be_bytes());
        hasher.update(&value_size.to_be_bytes());
        hasher.update(&key_buff);
        hasher.update(&value_buf);
        let computed_crc = hasher.finalize();

        Ok(RecordData {
            crc: computed_crc,
            timestamp,
            key_size,
            value_size,
            key: key_buff,
            value: value_buf,
        })
    }

    /// Get current timestamp
    fn get_timestamp() -> u64 {
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("Time went backwards")
            .as_secs()
    }

    /// Get all data file IDs in the data directory
    fn get_file_ids(config: &Config) -> crate::Result<Vec<u64>> {
        let mut file_ids = Vec::new();
        let extension = config.store_model().extension();

        for entry in std::fs::read_dir(&config.database_path())? {
            let path = entry?.path();

            if path.is_dir() {
                continue;
            }

            // Check file extension
            if path
                .extension()
                .map_or(true, |ext| ext != extension.as_str())
            {
                continue;
            }

            // Extract file ID from filename
            if let Some(stem) = path.file_stem().and_then(|s| s.to_str()) {
                if let Ok(file_id) = stem.parse::<u64>() {
                    file_ids.push(file_id);
                }
            }
        }

        file_ids.sort_unstable();
        Ok(file_ids)
    }

    /// Generate filename for a file ID
    fn get_file_name(config: &Config, file_id: u64) -> String {
        format!("{}.{}", file_id, &config.store_model().extension())
    }

    /// Open file for appending
    fn open_append_data_file(config: &Config, file_id: u64) -> crate::Result<BufWriter<File>> {
        let file_path = config
            .database_path()
            .join(Self::get_file_name(config, file_id));
        Ok(BufWriter::new(
            OpenOptions::new()
                .create(true)
                .append(true)
                .open(file_path)?,
        ))
    }

    /// Open file for reading
    fn open_read_only_data_file(config: &Config, file_id: u64) -> crate::Result<BufReader<File>> {
        let file_path = config
            .database_path()
            .join(Self::get_file_name(config, file_id));
        Ok(BufReader::new(
            OpenOptions::new().read(true).open(file_path)?,
        ))
    }

    /// Open merge file
    fn open_merge_data_file(config: &Config, file_id: u64) -> crate::Result<BufWriter<File>> {
        let file_name = Self::get_file_name(config, file_id);
        let file_path = config.database_path().join(format!("{}.merge", file_name));
        let file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(file_path)?;
        Ok(BufWriter::new(file))
    }

    /// Finalize merge file by renaming
    fn finish_merge_data_file(config: &Config, file_id: u64) -> crate::Result<()> {
        let file_name = Self::get_file_name(config, file_id);
        let merge_file_path = config.database_path().join(format!("{}.merge", file_name));
        let finish_file_path = config.database_path().join(file_name);
        std::fs::rename(merge_file_path, finish_file_path)?;
        Ok(())
    }

    /// Delete multiple data files
    fn delete_data_files(&self, file_ids: &[u64]) -> crate::Result<()> {
        for &file_id in file_ids {
            Self::delete_data_file(&self.config, file_id)?;
        }
        Ok(())
    }

    /// Delete a single data file
    fn delete_data_file(config: &Config, file_id: u64) -> crate::Result<()> {
        let file_path = config
            .database_path()
            .join(Self::get_file_name(config, file_id));
        std::fs::remove_file(file_path)?;
        Ok(())
    }

    /// Internal get method
    fn get_internal(&self, key: &[u8]) -> crate::Result<Option<Vec<u8>>> {
        let record_pos = match self.keydir.get(key) {
            Some(pos) => pos,
            None => return Ok(None),
        };

        let file_id = record_pos.file_id;
        let mut cache = self
            .opened_data_file_handles
            .lock()
            .map_err(|_| crate::Error::PoisonError("Failed to lock file cache".to_string()))?;

        let mut file = cache.get_or_insert_mut(file_id, || {
            Self::open_read_only_data_file(&self.config, file_id)
                .expect(&format!("Failed to open data file id: {}", file_id))
        });

        let start_offset = record_pos.value_pos - RecordData::HEADER_SIZE - key.len() as u64;
        let next_record = Self::read_next_record(&mut file, start_offset, true)?;
        match next_record {
            Some(next_record) => Ok(next_record.map_or(None, |data| Some(data.0.value))),
            None => Ok(None),
        }
    }

    /// Internal put method
    fn put_internal(&self, key: &[u8], value: &[u8]) -> crate::Result<()> {
        // Check if file rotation is needed
        let key_size = key.len() as u64;
        let value_size = value.len() as u64;
        let record_size = RecordData::HEADER_SIZE + key_size + value_size;
        let mut active_file = self.active_file.write().unwrap();
        self.maybe_rotate_file(&mut active_file, record_size)?;

        let record = RecordData::new(key.to_vec(), value.to_vec());
        let record_start_pos = active_file.seek(SeekFrom::End(0))?;

        active_file.write_all(&record.encode()?)?;
        active_file.flush()?;

        let record_pos = RecordPos {
            file_id: self.active_file_id.load(Ordering::Relaxed),
            value_size: record.value_size,
            value_pos: record_start_pos + RecordData::HEADER_SIZE + record.key_size,
            timestamp: record.timestamp,
        };

        self.keydir.insert(key.to_vec(), record_pos);
        Ok(())
    }

    /// Rotate file if current file exceeds size limit
    fn maybe_rotate_file(
        &self,
        active_file: &mut BufWriter<File>,
        record_size: u64,
    ) -> crate::Result<()> {
        let current_offset = active_file.seek(SeekFrom::End(0))?;
        if current_offset + record_size > self.config.max_file_size() {
            active_file.flush()?;
            active_file.get_ref().sync_all()?;

            let next_file_id = self.next_file_id.load(Ordering::Relaxed);

            self.active_file_id.store(next_file_id, Ordering::Relaxed);
            *active_file = Self::open_append_data_file(&self.config, next_file_id)?;

            self.next_file_id
                .store(Self::get_timestamp(), Ordering::Relaxed);

            let mut file_ids = self
                .file_ids
                .write()
                .map_err(|_| crate::Error::PoisonError("Failed to write file_ids".to_string()))?;
            file_ids.push(next_file_id);
        }

        Ok(())
    }

    /// Internal remove method
    fn delete_internal(&self, key: &[u8]) -> crate::Result<()> {
        if self.keydir.contains_key(key) {
            // Write tombstone record
            let tombstone = RecordData::tombstone(key.to_vec());
            let mut active_file = self.active_file.write().unwrap();
            let _record_start_pos = active_file.seek(SeekFrom::End(0))?;
            active_file.write_all(&tombstone.encode()?)?;

            // Remove from memory index
            self.keydir.remove(key);
        }

        Ok(())
    }

    /// Internal list_keys method
    fn list_keys_internal(&self) -> crate::Result<Vec<Vec<u8>>> {
        Ok(self.keydir.iter().map(|e| e.key().clone()).collect())
    }

    /// Internal contains method
    fn contains_internal(&self, key: &[u8]) -> crate::Result<bool> {
        Ok(self.keydir.contains_key(key))
    }

    /// Internal sync method
    fn sync_internal(&self) -> crate::Result<()> {
        let mut active_file = self.active_file.write().unwrap();
        active_file.flush()?;
        active_file.get_ref().sync_all()?;
        Ok(())
    }

    /// Internal can_merge method
    fn can_merge_internal(&self) -> crate::Result<bool> {
        // Get old file IDs (excluding active file)
        let file_ids = self
            .file_ids
            .read()
            .map_err(|_| crate::Error::PoisonError("Failed to read file_ids".to_string()))?;

        let old_file_ids: Vec<&u64> = file_ids
            .iter()
            .filter(|&id| *id != self.active_file_id.load(Ordering::Relaxed))
            .collect();

        // If the threshold is not exceeded
        if old_file_ids.len() >= self.config.max_historical_files() as usize {
            return Ok(true);
        }

        Ok(false)
    }

    /// Internal merge method
    fn merge_internal(&self) -> crate::Result<()> {
        self.merge_existing_files()
    }

    /// Internal close method
    fn close_internal(&self) -> crate::Result<()> {
        self.merge_existing_files()?;
        let mut active_file = self.active_file.write().unwrap();
        active_file.flush()?;
        active_file.get_ref().sync_all()?;
        self.opened_data_file_handles
            .lock()
            .map_err(|_| crate::Error::PoisonError("Failed to clear data file".to_string()))?
            .clear();
        Ok(())
    }
}

impl KvStore for Bitcask {
    fn get(&self, key: &[u8]) -> crate::Result<Option<Vec<u8>>> {
        self.get_internal(key)
    }

    fn put(&self, key: &[u8], value: &[u8]) -> crate::Result<()> {
        self.put_internal(key, value)
    }

    fn delete(&self, key: &[u8]) -> crate::Result<()> {
        self.delete_internal(key)
    }

    fn contains(&self, key: &[u8]) -> crate::Result<bool> {
        self.contains_internal(key)
    }

    fn list_keys(&self) -> crate::Result<Vec<Vec<u8>>> {
        self.list_keys_internal()
    }

    fn sync(&self) -> crate::Result<()> {
        self.sync_internal()
    }

    fn can_merge(&self) -> crate::Result<bool> {
        self.can_merge_internal()
    }

    fn merge(&self) -> crate::Result<()> {
        self.merge_internal()
    }

    fn close(&self) -> crate::Result<()> {
        self.close_internal()
    }
}

impl Drop for Bitcask {
    fn drop(&mut self) {
        if let Err(e) = self.close() {
            eprint!("bitcask close err: {}", e);
        }
    }
}
