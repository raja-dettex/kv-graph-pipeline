use scylla::client::{session::Session, session_builder::SessionBuilder};

pub async fn create_session(uri: String) -> std::result::Result<Session, std::io::Error>{ 
    let session: Session = SessionBuilder::new()
        .known_node(uri)
        .build().await.map_err(|e| std::io::Error::new(
            std::io::ErrorKind::ConnectionAborted, e.to_string()))?;
    Ok(session)
} 