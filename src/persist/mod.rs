pub mod es;
pub mod mongo;

use crate::Result;
use elasticsearch::Elasticsearch;
use mongodb::Client;
use std::sync::{Arc, Mutex};

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
}

pub trait DataSource<T> {
    fn get_connection(&self) -> Result<T>;
    fn close_connection(&self, conn: T) -> Result<()>;
}
