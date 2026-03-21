use std::{fs::{File, ReadDir}, io::{Read, Write, Seek, SeekFrom}, os::windows::fs::FileExt, path::PathBuf, sync::atomic::{AtomicUsize, Ordering}};
use crc32fast::Hasher;
use std::fs::{OpenOptions, rename};

pub struct CheckpointConfig { 
    pub write_offset: usize,
    pub read_offset: usize,
    pub writer_seg: PathBuf,
    pub reader_seg: PathBuf
}

impl CheckpointConfig {

    pub fn persist(&self, path: &PathBuf) -> std::io::Result<()> {

        let tmp = path.with_extension("tmp");

        let mut buf = Vec::new();

        buf.extend_from_slice(&(self.write_offset as u64).to_be_bytes());
        buf.extend_from_slice(&(self.read_offset as u64).to_be_bytes());

        let writer = self.writer_seg.to_string_lossy();
        buf.extend_from_slice(&(writer.len() as u32).to_be_bytes());
        buf.extend_from_slice(writer.as_bytes());

        let reader = self.reader_seg.to_string_lossy();
        buf.extend_from_slice(&(reader.len() as u32).to_be_bytes());
        buf.extend_from_slice(reader.as_bytes());

        let mut file = OpenOptions::new()
            .create(true)
            .write(true)
            .truncate(true)
            .open(&tmp)?;

        file.write_all(&buf)?;
        file.sync_all()?;

        rename(tmp, path)?;

        Ok(())
    }
    pub fn load(path: &PathBuf) -> std::io::Result<Self> {

        let mut file = File::open(path)?;

        let mut u64buf = [0u8; 8];
        let mut u32buf = [0u8; 4];

        file.read_exact(&mut u64buf)?;
        let write_offset = u64::from_be_bytes(u64buf) as usize;

        file.read_exact(&mut u64buf)?;
        let read_offset = u64::from_be_bytes(u64buf) as usize;

        file.read_exact(&mut u32buf)?;
        let writer_len = u32::from_be_bytes(u32buf) as usize;

        let mut writer_bytes = vec![0u8; writer_len];
        file.read_exact(&mut writer_bytes)?;

        file.read_exact(&mut u32buf)?;
        let reader_len = u32::from_be_bytes(u32buf) as usize;

        let mut reader_bytes = vec![0u8; reader_len];
        file.read_exact(&mut reader_bytes)?;

        Ok(Self {
            write_offset,
            read_offset,
            writer_seg: PathBuf::from(String::from_utf8(writer_bytes).unwrap()),
            reader_seg: PathBuf::from(String::from_utf8(reader_bytes).unwrap()),
        })
    }
}
#[derive(Debug)]
pub struct Mutation { 
    pub bucket: u32,
    pub key_len: u32,
    pub val_len: u32,
    pub key: Vec<u8>,
    pub value: Vec<u8>
}
pub struct WalWriter { 
    path: PathBuf,
    file: File,
    write_offset: AtomicUsize   
}

impl WalWriter { 
    pub fn new(path: PathBuf, lsn: usize) -> std::result::Result<Self, std::io::Error> { 
        let file = std::fs::OpenOptions::new().append(true).create(true).open(path.clone())?;
        Ok(Self { path, file, write_offset: AtomicUsize::new(lsn) })
    }

    #[inline]
    pub fn size(&self) -> std::io::Result<u64> { 
        Ok(self.file.metadata()?.len())
    }
    pub fn write(&mut self, mutation: Mutation) -> std::result::Result<usize, std::io::Error> { 
        let mut buf : Vec<u8> = Vec::new();
        let mut hasher = Hasher::new();
        buf.extend_from_slice(&mutation.bucket.to_be_bytes());
        hasher.update(&mutation.bucket.to_be_bytes());
        buf.extend_from_slice(&mutation.key_len.to_be_bytes());
        hasher.update(&mutation.key_len.to_be_bytes());
        buf.extend_from_slice(&mutation.val_len.to_be_bytes());
        hasher.update(&mutation.val_len.to_be_bytes());
        buf.extend_from_slice(&mutation.key);
        hasher.update(&mutation.key);
        buf.extend_from_slice(&mutation.value);
        hasher.update(&mutation.value);
        let checksum = hasher.finalize();
        buf.extend_from_slice(&checksum.to_be_bytes());
        let current_offset = self.write_offset.fetch_add(buf.len(), Ordering::SeqCst);
        let mut written = 0usize;
        while !buf.is_empty() { 
            match self.file.seek_write(&buf[written..], (current_offset + written) as u64) { 
                Ok(0) => break,
                Ok(n) => written = written + n,
                Err(_) => todo!()
            }
        }
        self.file.sync_data()?;
        Ok(self.write_offset.load(Ordering::SeqCst))
    }
}


pub struct WalReader {
    file: File,
}

impl WalReader {
    pub fn new(path: PathBuf) -> std::io::Result<Self> {
        let file = std::fs::OpenOptions::new()
            .read(true)
            .open(path).expect("here is hte error");
        Ok(Self { file })
    }
    pub fn seek(&mut self, offset: usize) -> std::io::Result<()> {
        self.file.seek(std::io::SeekFrom::Start(offset as u64))?;
        Ok(())
    }
    
    pub fn read_all(&mut self, offset: usize) -> std::io::Result<(Vec<Mutation>, usize)> {
        let start = offset;

        let mut mutations = Vec::new();
        self.file.seek(SeekFrom::Start(start as u64))?;
        loop {
            let pos = self.file.stream_position()? as usize;

            let mut header = [0u8; 12];

            match self.file.read_exact(&mut header) {
                Ok(_) => {}
                Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => {
                    self.file.seek(SeekFrom::Start(pos as u64))?;
                    break;
                }
                Err(e) => return Err(e),
            }

            let bucket = u32::from_be_bytes(header[0..4].try_into().unwrap());
            let key_len = u32::from_be_bytes(header[4..8].try_into().unwrap());
            let val_len = u32::from_be_bytes(header[8..12].try_into().unwrap());

            let mut key = vec![0u8; key_len as usize];
            let mut value = vec![0u8; val_len as usize];

            self.file.read_exact(&mut key)?;
            self.file.read_exact(&mut value)?;

            let mut checksum = [0u8; 4];
            self.file.read_exact(&mut checksum)?;

            mutations.push(Mutation {
                bucket,
                key_len,
                val_len,
                key,
                value,
            });
        }

        let end = self.file.stream_position()? as usize;

        Ok((mutations, end - start))
    }
}
