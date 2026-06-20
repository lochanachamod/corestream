use memmap2::MmapMut;
use std::collections::HashMap;
use std::fs::{self, File, OpenOptions};
use std::io::{self, Write};
use std::path::{Path, PathBuf};

const INDEX_ENTRY_SIZE: usize = 16; // 8 bytes for position, 8 bytes for length
const MAX_INDEX_SIZE: usize = 1024 * 1024 * 10; // 10MB map

pub struct TopicStorage {
    log_file: File,
    index_mmap: MmapMut,
    pub current_offset: u64,
    current_log_pos: u64,
}

impl TopicStorage {
    pub fn new(dir: &PathBuf) -> io::Result<Self> {
        std::fs::create_dir_all(dir)?;

        let log_path = dir.join("commit_log.dat");
        let index_path = dir.join("index.dat");

        let log_file = OpenOptions::new()
            .read(true)
            .create(true)
            .append(true)
            .open(&log_path)?;

        let current_log_pos = log_file.metadata()?.len();

        let index_file = OpenOptions::new()
            .create(true)
            .read(true)
            .write(true)
            .open(&index_path)?;

        if index_file.metadata()?.len() == 0 {
            index_file.set_len(MAX_INDEX_SIZE as u64)?;
        }

        let index_mmap = unsafe { MmapMut::map_mut(&index_file)? };

        // Real production systems parse the index to find the exact offset. 
        // For simplicity in our prototype, we will just count the length and assume each entry is 1.
        let file_len = index_file.metadata()?.len();
        // Just for simplicity, start offset at 1
        let current_offset = 1; 

        Ok(Self {
            log_file,
            index_mmap,
            current_offset,
            current_log_pos,
        })
    }

    pub fn append(&mut self, payload: &[u8]) -> io::Result<u64> {
        let len = payload.len() as u64;

        self.log_file.write_all(payload)?;
        self.log_file.sync_data()?;

        let idx_pos = (self.current_offset as usize - 1) * INDEX_ENTRY_SIZE;
        
        self.index_mmap[idx_pos..idx_pos + 8].copy_from_slice(&self.current_log_pos.to_be_bytes());
        self.index_mmap[idx_pos + 8..idx_pos + 16].copy_from_slice(&len.to_be_bytes());
        self.index_mmap.flush()?;

        let assigned_offset = self.current_offset;
        self.current_log_pos += len;
        self.current_offset += 1;

        Ok(assigned_offset)
    }

    pub fn read(&self, offset: u64) -> io::Result<Option<Vec<u8>>> {
        if offset == 0 || offset >= self.current_offset {
            return Ok(None);
        }

        let idx_pos = (offset as usize - 1) * INDEX_ENTRY_SIZE;
        
        let mut pos_bytes = [0u8; 8];
        pos_bytes.copy_from_slice(&self.index_mmap[idx_pos..idx_pos + 8]);
        let pos = u64::from_be_bytes(pos_bytes);
        
        let mut len_bytes = [0u8; 8];
        len_bytes.copy_from_slice(&self.index_mmap[idx_pos + 8..idx_pos + 16]);
        let len = u64::from_be_bytes(len_bytes);

        #[cfg(unix)]
        {
            use std::os::unix::fs::FileExt;
            let mut buf = vec![0u8; len as usize];
            self.log_file.read_exact_at(&mut buf, pos)?;
            return Ok(Some(buf));
        }

        #[cfg(not(unix))]
        {
            return Err(io::Error::new(io::ErrorKind::Unsupported, "Only unix OS page cache reads are supported"));
        }
    }
}

pub struct StorageEngine {
    base_dir: PathBuf,
    topics: HashMap<String, TopicStorage>,
}

impl StorageEngine {
    pub fn new<P: AsRef<Path>>(dir: P) -> io::Result<Self> {
        let base_dir = dir.as_ref().to_path_buf();
        std::fs::create_dir_all(&base_dir)?;

        Ok(Self {
            base_dir,
            topics: HashMap::new(),
        })
    }

    fn get_or_create_topic(&mut self, topic: &str) -> io::Result<&mut TopicStorage> {
        if !self.topics.contains_key(topic) {
            let topic_dir = self.base_dir.join(topic);
            let engine = TopicStorage::new(&topic_dir)?;
            self.topics.insert(topic.to_string(), engine);
        }
        Ok(self.topics.get_mut(topic).unwrap())
    }

    pub fn append(&mut self, topic: &str, payload: &[u8]) -> io::Result<u64> {
        let topic_engine = self.get_or_create_topic(topic)?;
        topic_engine.append(payload)
    }

    pub fn read(&mut self, topic: &str, offset: u64) -> io::Result<Option<Vec<u8>>> {
        let topic_engine = self.get_or_create_topic(topic)?;
        topic_engine.read(offset)
    }

    pub fn current_offset(&mut self, topic: &str) -> io::Result<u64> {
        let topic_engine = self.get_or_create_topic(topic)?;
        Ok(topic_engine.current_offset)
    }
}
