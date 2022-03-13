use std::collections::{HashMap, LinkedList};

use super::biz::Ticker;

pub struct AppContext {
    pub tickers: HashMap<String, LinkedList<Ticker>>,
}

impl AppContext{
    
}