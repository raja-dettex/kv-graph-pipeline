mod cassandra;
mod kv;
mod wal;
use std::{path::PathBuf, sync::Arc, time::Duration};

use clap::{Parser, Subcommand};


use crate::{ kv::{KVApi, KvMeta, KvStore}};
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
    let wal_dir = PathBuf::from("./segments");
    let checkpoint_path = wal_dir.join("checkpoint.log");
    let kv_store = Arc::new(KvStore::new("localhost:9042".to_string(), KvMeta { keyspace, table }, wal_dir, checkpoint_path).await
    .expect("failing herer"));
    kv_store.migrate_if_allowed().await?;
    kv_store.start_checkpoint_persister();
    kv_store.clone().start_flush_worker();
    let kv_clone = kv_store.clone();
    // tokio::spawn(async move { 
    //     let mut key = "user123:watched".to_string();
    //     let mut movie_id = 10;
    //     for i in 0..100 { 
    //         if i == 50 { 
    //             key = "user:130:watched".to_string();
    //             movie_id = 10;    
    //         }
    //         let mut value = format!("movie:{}", movie_id);
    //         kv_clone.clone().append(key.clone(), value.as_bytes().to_vec());
    //         movie_id += 1;
    //     }
    // });
    tokio::time::sleep(Duration::from_secs(18)).await;
    let values_1 = kv_store.clone().get("user123:watched".to_string()).await?;
    let values_2 = kv_store.clone().get("user:130:watched".to_string()).await?;

    let first_set_of_values: Vec<String> = values_1.into_iter().map(|v| String::from_utf8(v).unwrap()).collect();
    let second_set_of_values: Vec<String> = values_2.into_iter().map(|v| String::from_utf8(v).unwrap()).collect();
    println!("first set of values {first_set_of_values:?}");
    println!("second set of values {second_set_of_values:?}");

    tokio::signal::ctrl_c().await?;
    Ok(())
}
