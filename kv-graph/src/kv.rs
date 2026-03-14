use scylla::client::session::Session;
use std::{hash::{DefaultHasher, Hash, Hasher}, io::ErrorKind};
use crate::cassandra::create_session;
const NUM_OF_BUCKET: u64 = 128;
const NUM_OF_SHARDS: u64 = 8;
pub trait KVApi<T: Into<Vec<u8>> + Sized> { 
    async fn put(&self, key: String, value: T) -> Result<(), std::io::Error>;
    async fn get(&self, key: String) -> Result<Vec<Vec<u8>>, std::io::Error>;
}
pub struct KvMeta { 
    pub keyspace: String,
    pub table: String,
}

pub struct KvStore { 
    session: Session,
    kv_meta: KvMeta  
}

impl KvStore { 
    pub async fn new(uri: String, kv_meta: KvMeta) -> std::result::Result<KvStore, std::io::Error>  { 
        let session: Session = create_session(uri).await?;
        Ok(Self { session, kv_meta})
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
}

impl<T: Into<Vec<u8>> + Sized>  KVApi<T> for KvStore {
    async fn put(&self, key: String, value: T) -> Result<(), std::io::Error> {
        let bucket = get_bucket_from_key(key.clone());
        let value_vec = value.into();
        let stmt = format!("INSERT INTO {:?}.{:?} (bucket, key, value) values(?,?,?)", self.kv_meta.keyspace, self.kv_meta.table);
        self.session.query_unpaged(stmt, (bucket, key, value_vec)).await
            .map_err(|e| std::io::Error::new(ErrorKind::Other, e.to_string()))?;
        Ok(())
    }

    async fn get(&self, key: String) -> Result<Vec<Vec<u8>>, std::io::Error> {
        let mut values = Vec::new();
        let bucket = get_bucket_from_key(key.clone());
        let query = format!("SELECT value FROM {:?}.{:?} WHERE bucket = ? AND key = ?", self.kv_meta.keyspace, self.kv_meta.table);
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