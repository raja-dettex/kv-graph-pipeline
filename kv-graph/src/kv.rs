use scylla::client::session::Session;
use std::{collections::HashMap, hash::{DefaultHasher, Hash, Hasher}, io::ErrorKind, path::PathBuf, sync::{Arc, Mutex}, time::Duration};
use crate::{cassandra::create_session, wal::{CheckpointConfig, Mutation, WalReader, WalWriter}};
use chrono::prelude::*;
const NUM_OF_BUCKET: u64 = 128;
const NUM_OF_SHARDS: u64 = 8;
const LOG_SEG_SIZE: u64 = 2048;
pub trait KVApi<T: Into<Vec<u8>> + Sized> { 
    async fn put(&self, key: String, value: T) -> Result<(), std::io::Error>;
    async fn get(&self, key: String) -> Result<Vec<Vec<u8>>, std::io::Error>;
}

pub struct KvMeta { 
    pub keyspace: String,
    pub table: String
}

pub struct KvStore { 
    session: Session,
    kv_meta: KvMeta,
    wal_writer: Arc<Mutex<WalWriter>>,
    wal_reader: Arc<Mutex<WalReader>>,
    checkpoint: Arc<Mutex<CheckpointConfig>>,
    wal_dir: PathBuf,
    checkpoint_path: PathBuf
}

impl KvStore { 
    pub async fn new(uri: String, kv_meta: KvMeta, wal_dir: PathBuf, checkpoint_path: PathBuf) -> std::result::Result<KvStore, std::io::Error>  { 
        let session: Session = create_session(uri).await?;
        std::fs::create_dir_all(wal_dir.clone()).expect("failing hre in kv");
        let now_utc: DateTime<Utc> = Utc::now();
        let custom_format_str = "%Y-%m-%d %H:%M:%S.%f";
        let formatted_utc_time = now_utc.format(custom_format_str).to_string();
        //let wal_path = PathBuf::from(format!("{:?}/wal.seg", wal_dir.to_str()));
        let wal_path = wal_dir.join("wal.seg");
        let checkpoint = if checkpoint_path.exists() { 
            println!("check point path exists");
            CheckpointConfig::load(&checkpoint_path)?
        } else { 
            CheckpointConfig { 
                write_offset: 0,
                read_offset: 0,
                writer_seg: wal_path.clone(),
                reader_seg: wal_path.clone()
            }
        };
        let wal_writer = WalWriter::new(wal_path, 0)?;
        let mut wal_reader = WalReader::new(checkpoint.reader_seg.clone())?;
        
        Ok(Self { session, kv_meta, wal_dir, checkpoint_path, wal_writer: Arc::new(Mutex::new(wal_writer)), wal_reader: Arc::new(Mutex::new(wal_reader)), checkpoint: Arc::new(Mutex::new(checkpoint))})
    }

    pub async fn migrate_if_allowed(&self) -> Result<(), std::io::Error> {

        let create_keyspace_query = format!(
            "
            CREATE KEYSPACE IF NOT EXISTS {}
            WITH REPLICATION = {{
                'class': 'SimpleStrategy',
                'replication_factor': 2
            }};
            ",
            self.kv_meta.keyspace
        );

        self.session
            .query_unpaged(create_keyspace_query, ())
            .await
            .map_err(|e| std::io::Error::new(ErrorKind::Other, e.to_string()))?;

        let create_table_query = format!(
            "
            CREATE TABLE IF NOT EXISTS {}.{} (
                bucket int,
                key text,
                value blob,
                PRIMARY KEY((bucket, key), value)
            );
            ",
            self.kv_meta.keyspace,
            self.kv_meta.table
        );

        self.session
            .query_unpaged(create_table_query, ())
            .await
            .map_err(|e| std::io::Error::new(ErrorKind::Other, e.to_string()))?;

        Ok(())
    }

    pub fn append(&self, key: String, value: Vec<u8>) -> std::result::Result<(), std::io::Error>{ 
        // append to log segments
        let bucket = get_bucket_from_key(key.clone()) as u32;
        let key_bytes = String::into_bytes(key);
        let key_len = key_bytes.clone().len() as u32;
        let val_len = value.clone().len() as u32;
        let mutation = Mutation { bucket, key_len, val_len, key: key_bytes, value };
        let mut wal_writer = self.wal_writer.lock().map_err(|e| std::io::Error::new(ErrorKind::Other, e.to_string()))?;
        let mut cp = self.checkpoint.lock().map_err(|e| std::io::Error::new(ErrorKind::Other, e.to_string()))?;
        let write_offset = wal_writer.write(mutation)?;
        cp.write_offset = write_offset;
        
        Ok(())
    }

    async fn flush_to_storage(&self) -> std::result::Result<(), std::io::Error>{ 
        let read_offset = { 
            let mut cp = self.checkpoint.lock()
            .map_err(|e| std::io::Error::new(ErrorKind::Other, e.to_string()))?;
            cp.read_offset 
        };
        println!("read offset {read_offset}");
        let (mutations, consumed_bytes) = {
            let mut wal_reader = self
                .wal_reader
                .lock()
                .map_err(|e| std::io::Error::new(ErrorKind::Other, e.to_string()))?;
            
            wal_reader.read_all(read_offset)?
        };
        println!("{:?}", mutations);
        let mut mutations_group: HashMap<(i32, Vec<u8>), Vec<Vec<u8>>> = HashMap::new();
        for mutation in mutations { 
            let bucket = mutation.bucket as i32;
            let key = mutation.key;
            let value = mutation.value;
            mutations_group
                .entry((bucket, key))
                .or_insert_with(Vec::new)
                .push(value);
        }
        if let Some(((bucket, key), values) ) = mutations_group.iter_mut().next() { 
            for value in values { 
                self.put(bucket.clone(), String::from_utf8(key.clone()).unwrap(), value.clone()).await?;
            }
        }
        {
            let mut cp = self
                .checkpoint
                .lock()
                .map_err(|e| std::io::Error::new(ErrorKind::Other, e.to_string()))?;

            cp.read_offset += consumed_bytes;
        }
        Ok(())
    }

    async fn put(&self, bucket: i32,  key: String, value: Vec<u8>) -> Result<(), std::io::Error> {
        
        let stmt = format!("INSERT INTO {}.{} (bucket, key, value) values(?,?,?)", self.kv_meta.keyspace, self.kv_meta.table);
        self.session.query_unpaged(stmt, (bucket, key, value)).await
            .map_err(|e| std::io::Error::new(ErrorKind::Other, e.to_string()))?;
        Ok(())
    }

    pub async fn get(&self, key: String) -> Result<Vec<Vec<u8>>, std::io::Error> {
        let mut values = Vec::new();
        let bucket = get_bucket_from_key(key.clone());
        let query = format!("SELECT value FROM {}.{} WHERE bucket = ? AND key = ?", self.kv_meta.keyspace, self.kv_meta.table);
        let result = self.session.query_unpaged(query, (bucket, key)).await
            .map_err(|e| std::io::Error::new(ErrorKind::Other, e.to_string()))?;
        let rows_result = result.into_rows_result()
            .map_err(|e| std::io::Error::new(ErrorKind::Other, e.to_string()))?;
        let mut rows = rows_result.rows::<(Vec<u8>,)>()
            .map_err(|e| std::io::Error::new(ErrorKind::Other, e.to_string()))?;
        while let Ok(Some((value,))) = rows.next().transpose() { 
            values.push(value)
        }
        Ok(values)
    }
    pub fn start_checkpoint_persister(&self) {

        let checkpoint = Arc::clone(&self.checkpoint);
        let checkpoint_path = self.checkpoint_path.clone();

        tokio::spawn(async move  {

            loop {

                tokio::time::sleep(Duration::from_secs(1)).await;

                let cp = checkpoint.lock();

                if let Ok(cp) = cp {

                    if let Err(e) = cp.persist(&checkpoint_path) {
                        eprintln!("checkpoint persist failed: {:?}", e);
                    }

                }

            }

        });

    }
    pub fn start_flush_worker(self: Arc<Self>) {

        tokio::spawn(async move {

            let mut ticker = tokio::time::interval(Duration::from_secs(10));

            loop {

                ticker.tick().await;

                let store = self.clone();

                if let Err(e) = store.flush_to_storage().await {
                    eprintln!("flush_to_storage failed: {:?}", e);
                }

            }

        });

    }
}





fn get_bucket_from_key(key: String) -> i32 { 
    let mut hasher = DefaultHasher::new();
    key.hash(&mut hasher);
    (hasher.finish() % NUM_OF_BUCKET) as i32
}

fn get_shard_from_value(value: Vec<u8>) -> i32 { 
    let mut hasher = DefaultHasher::new();
    value.hash(&mut hasher);
    (hasher.finish() % NUM_OF_SHARDS) as i32
}