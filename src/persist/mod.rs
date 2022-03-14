pub mod es;
pub mod mongo;

use self::mongo::get_mongo_client;
use crate::Result;
use elasticsearch::Elasticsearch;
use mongodb::Client;
use std::{
    fmt::Debug,
    sync::{Arc, Mutex},
};

#[derive(Debug)]
pub struct PersistenceContext {
    elastic_connections: Arc<Mutex<Vec<Elasticsearch>>>,
    mongo_connections: Arc<Mutex<Vec<Client>>>,
}

impl PersistenceContext {
    pub fn new() -> PersistenceContext {
        PersistenceContext {
            elastic_connections: Arc::new(Mutex::new(Vec::new())),
            mongo_connections: Arc::new(Mutex::new(Vec::new())),
        }
    }
    pub async fn init_mongo(&self) -> Result<()> {
        // TODO: temp sollution
        for _ in 1..10 {
            let conn = get_mongo_client().await?;
            self.close_connection(conn)?;
        }
        Ok(())
    }
}

pub trait DataSource<T> {
    fn get_connection(&self) -> Result<T>;
    fn close_connection(&self, conn: T) -> Result<()>;
}
