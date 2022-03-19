pub mod es;
pub mod mongo;

use self::mongo::get_mongo_client;
use crate::{vo::core::AppConfig, Result};
use elasticsearch::Elasticsearch;
use mongodb::Client;
use std::{
    fmt::Debug,
    sync::{Arc, Mutex},
};

#[derive(Debug)]
pub struct PersistenceContext {
    pub config: Arc<AppConfig>,
    elastic_connections: Arc<Mutex<Vec<Elasticsearch>>>,
    mongo_connections: Arc<Mutex<Vec<Client>>>,
}

impl PersistenceContext {
    pub fn new(config: Arc<AppConfig>) -> PersistenceContext {
        PersistenceContext {
            config,
            elastic_connections: Arc::new(Mutex::new(Vec::new())),
            mongo_connections: Arc::new(Mutex::new(Vec::new())),
        }
    }
    pub async fn init_mongo(&self, conifg: &AppConfig) -> Result<()> {
        // TODO: temp sollution
        for _ in 1..10 {
            let conn = get_mongo_client(&conifg.data_source.mongodb.uri).await?;
            self.close_connection(conn)?;
        }
        Ok(())
    }
}

pub trait DataSource<T> {
    fn get_connection(&self) -> Result<T>;
    fn close_connection(&self, conn: T) -> Result<()>;
}
