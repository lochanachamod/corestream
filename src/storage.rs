use memmap2::MmapMut;
use std::collections::HashMap;
use std::fs::{self, File, OpenOptions};
use std::io::{self, Write};
use std::path::{Path, PathBuf};

const INDEX_ENTRY_SIZE: usize = 16;
const MAX_INDEX_SIZE: usize = 1024 * 1024; // 1MB per index segment
const MAX_SEGMENT_SIZE: u64 = 1024 * 100; // 100KB per log segment for testing

pub struct Segment {
    pub base_offset: u64,
    pub log_path: PathBuf,
    pub index_path: PathBuf,
    pub log_file: File,
    pub index_mmap: MmapMut,
    pub current_log_pos: u64,
}

impl Segment {
    pub fn new(dir: &PathBuf, base_offset: u64) -> io::Result<Self> {
        let log_path = dir.join(format!("{:020}.log", base_offset));
        let index_path = dir.join(format!("{:020}.index", base_offset));

        let log_file = OpenOptions::new().read(true).create(true).append(true).open(&log_path)?;
        let index_file = OpenOptions::new().create(true).read(true).write(true).open(&index_path)?;

        if index_file.metadata()?.len() == 0 {
            index_file.set_len(MAX_INDEX_SIZE as u64)?;
        }
        
        let index_mmap = unsafe { MmapMut::map_mut(&index_file)? };
        let current_log_pos = log_file.metadata()?.len();

        Ok(Self {
            base_offset,
            log_path,
            index_path,
            log_file,
            index_mmap,
            current_log_pos,
        })
    }
}

pub struct TopicStorage {
    dir: PathBuf,
    pub segments: Vec<Segment>,
    pub current_offset: u64,
}

impl TopicStorage {
    pub fn new(dir: &PathBuf) -> io::Result<Self> {
        std::fs::create_dir_all(dir)?;
        
        // Start fresh at offset 1
        let first_segment = Segment::new(dir, 1)?;
        
        Ok(Self {
            dir: dir.clone(),
            segments: vec![first_segment],
            current_offset: 1,
        })
    }

    pub fn append(&mut self, payload: &[u8]) -> io::Result<u64> {
        let len = payload.len() as u64;

        // Check if active segment is full
        if self.segments.last().unwrap().current_log_pos + len > MAX_SEGMENT_SIZE {
            // Close and create a new segment
            let new_segment = Segment::new(&self.dir, self.current_offset)?;
            self.segments.push(new_segment);
        }

        let active_segment = self.segments.last_mut().unwrap();
        
        active_segment.log_file.write_all(payload)?;
        active_segment.log_file.sync_data()?;

        let local_idx = (self.current_offset - active_segment.base_offset) as usize * INDEX_ENTRY_SIZE;
        
        active_segment.index_mmap[local_idx..local_idx + 8].copy_from_slice(&active_segment.current_log_pos.to_be_bytes());
        active_segment.index_mmap[local_idx + 8..local_idx + 16].copy_from_slice(&len.to_be_bytes());
        active_segment.index_mmap.flush()?;

        let assigned_offset = self.current_offset;
        active_segment.current_log_pos += len;
        self.current_offset += 1;

        Ok(assigned_offset)
    }

    pub fn read(&self, offset: u64) -> io::Result<Option<Vec<u8>>> {
        if offset == 0 || offset >= self.current_offset {
            return Ok(None);
        }

        // Find the segment containing this offset
        let segment = self.segments.iter().rev().find(|s| offset >= s.base_offset);
        if let Some(seg) = segment {
            let local_idx = (offset - seg.base_offset) as usize * INDEX_ENTRY_SIZE;
            
            let mut pos_bytes = [0u8; 8];
            pos_bytes.copy_from_slice(&seg.index_mmap[local_idx..local_idx + 8]);
            let pos = u64::from_be_bytes(pos_bytes);
            
            let mut len_bytes = [0u8; 8];
            len_bytes.copy_from_slice(&seg.index_mmap[local_idx + 8..local_idx + 16]);
            let len = u64::from_be_bytes(len_bytes);

            #[cfg(unix)]
            {
                use std::os::unix::fs::FileExt;
                let mut buf = vec![0u8; len as usize];
                seg.log_file.read_exact_at(&mut buf, pos)?;
                return Ok(Some(buf));
            }
        }

        Ok(None)
    }

    pub fn garbage_collect(&mut self, max_age_secs: u64) -> io::Result<usize> {
        let mut removed_count = 0;
        let now = std::time::SystemTime::now();

        // Keep at least the last segment (active segment)
        let mut retain_idx = 0;
        for (i, seg) in self.segments.iter().enumerate() {
            if i == self.segments.len() - 1 {
                break; // Don't delete the active segment
            }
            if let Ok(metadata) = seg.log_file.metadata() {
                if let Ok(modified) = metadata.modified() {
                    if let Ok(age) = now.duration_since(modified) {
                        if age.as_secs() > max_age_secs {
                            // Delete from disk
                            let _ = fs::remove_file(&seg.log_path);
                            let _ = fs::remove_file(&seg.index_path);
                            removed_count += 1;
                            retain_idx = i + 1;
                        }
                    }
                }
            }
        }

        if retain_idx > 0 {
            self.segments.drain(0..retain_idx);
        }
        
        Ok(removed_count)
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

    pub fn garbage_collect(&mut self, max_age_secs: u64) -> io::Result<usize> {
        let mut total_removed = 0;
        for topic_engine in self.topics.values_mut() {
            total_removed += topic_engine.garbage_collect(max_age_secs)?;
        }
        Ok(total_removed)
    }
}
