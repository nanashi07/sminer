use crate::Result;
use elasticsearch::Elasticsearch;
use std::sync::{Arc, Mutex};

pub mod es;
pub mod mongo;

pub struct PersistenceContext {
    elastic_connections: Arc<Mutex<Vec<Elasticsearch>>>,
}

impl PersistenceContext {
    pub fn new() -> PersistenceContext {
        PersistenceContext {
            elastic_connections: Arc::new(Mutex::new(Vec::new())),
        }
    }
}

pub trait DataSource<T> {
    fn get_connection(&self) -> Result<T>;
    fn close_connection(&self, conn: T) -> Result<()>;
}
