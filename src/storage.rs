use memmap2::MmapMut;
use std::fs::{File, OpenOptions};
use std::io::{self, Write};
use std::path::Path;

const INDEX_ENTRY_SIZE: usize = 16; // 8 bytes for position, 8 bytes for length
const MAX_INDEX_SIZE: usize = 1024 * 1024 * 10; // 10MB map (allows hundreds of thousands of messages)

pub struct StorageEngine {
    log_file: File,
    index_mmap: MmapMut,
    current_offset: u64,
    current_log_pos: u64,
}

impl StorageEngine {
    pub fn new<P: AsRef<Path>>(dir: P) -> io::Result<Self> {
        let dir = dir.as_ref();
        std::fs::create_dir_all(dir)?;

        let log_path = dir.join("commit_log.dat");
        let index_path = dir.join("index.dat");

        // Open the append-only commit log
        let log_file = OpenOptions::new()
            .read(true)
            .create(true)
            .append(true)
            .open(&log_path)?;

        let current_log_pos = log_file.metadata()?.len();

        // Open the high-speed index file
        let index_file = OpenOptions::new()
            .create(true)
            .read(true)
            .write(true)
            .open(&index_path)?;

        // Pre-allocate index file if it is empty so we can map it into RAM
        if index_file.metadata()?.len() == 0 {
            index_file.set_len(MAX_INDEX_SIZE as u64)?;
        }

        // Map the index file directly into OS RAM using mmap
        let index_mmap = unsafe { MmapMut::map_mut(&index_file)? };

        // For simplicity in Phase 2, start offset at 1
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

        // 1. Write the raw binary payload sequentially to the log file on disk
        self.log_file.write_all(payload)?;
        self.log_file.sync_data()?; // Ensure it hits the disk hardware safely

        // 2. Map the byte position and length directly into RAM via the index mmap
        let idx_pos = (self.current_offset as usize - 1) * INDEX_ENTRY_SIZE;
        
        // Write the 8-byte log position
        self.index_mmap[idx_pos..idx_pos + 8].copy_from_slice(&self.current_log_pos.to_be_bytes());
        // Write the 8-byte message length
        self.index_mmap[idx_pos + 8..idx_pos + 16].copy_from_slice(&len.to_be_bytes());
        
        // Flush the mmap buffer so the OS syncs it to the disk file asynchronously
        self.index_mmap.flush()?;

        let assigned_offset = self.current_offset;

        self.current_log_pos += len;
        self.current_offset += 1;

        Ok(assigned_offset)
    }

    pub fn current_offset(&self) -> u64 {
        self.current_offset
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

        // Read directly from the OS Page Cache using pread (read_exact_at)
        #[cfg(unix)]
        {
            use std::os::unix::fs::FileExt;
            let mut buf = vec![0u8; len as usize];
            self.log_file.read_exact_at(&mut buf, pos)?;
            return Ok(Some(buf));
        }

        #[cfg(not(unix))]
        {
            // Fallback for non-unix, though this project targets WSL/Linux
            return Err(io::Error::new(io::ErrorKind::Unsupported, "Only unix OS page cache reads are supported"));
        }
    }
}
