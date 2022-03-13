pub mod es;
pub mod mongo;

use crate::Result;
use elasticsearch::Elasticsearch;
use futures::Future;
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
}

pub trait DataSource<T> {
    fn get_connection(&self) -> Result<T>;
    fn close_connection(&self, conn: T) -> Result<()>;
}

pub trait DataSource2<T> {
    type Output: Future<Output = Result<T>>;

    fn get_connection2(&self) -> Self::Output;
}

// #[async_trait]
// pub trait DataSource3<T> {
//     async fn get_connection3(&self) -> Result<T>;
// }
