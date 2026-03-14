mod cassandra;
mod kv;
use clap::{Parser, Subcommand};
use scylla::client::session::Session;

use crate::{cassandra::create_session, kv::{KVApi, KvMeta, KvStore}};
#[derive(Parser, Debug)]
#[command(name = "kv-graph", about = "kv graph utils", version="1.0")]
pub struct Args {
    #[command(subcommand)] 
    command: Command
}

#[derive(Debug, Subcommand)]
pub enum Command { 
    Start { keyspace: String, table: String}
}
#[tokio::main]
async fn main() -> std::result::Result<(), std::io::Error>{
    let args = Args::parse();
    
    println!("command {:?}", args);
    let Command::Start { keyspace, table } = args.command;
    let kv_store = KvStore::new("localhost:9042".to_string(), KvMeta { keyspace, table }).await?;
    kv_store.migrate_if_allowed().await?;
    let key = "user245:watched";
    let value = b"movie:123";
    let mut movie_id = 124;
    for _ in 0..50 { 
        let movie = format!("movie:{movie_id:?}");
        println!("putting movie {movie:?}");
        let movie_blob = String::into_bytes(movie);
        kv_store.put(key.to_string(), movie_blob).await?;
        movie_id += 1;

    }
    //kv_store.put(key.to_string(), value).await?;
    let values = <KvStore as KVApi<Vec<u8>>>::get(&kv_store, key.to_string()).await?;
    let values_str: Vec<String> = values.into_iter().map(|v| String::from_utf8(v).unwrap()).collect();
    println!("values {:?}", values_str);
    Ok(())
}
