use super::biz::{Protfolio, Ticker, TimeUnit};
use crate::{
    analysis::init_dispatcher,
    persist::{es::ElasticTicker, PersistenceContext},
    proto::biz::TickerEvent,
    Result,
};
use config::Config;
use log::{debug, error};
use rayon::prelude::*;
use serde::{Deserialize, Serialize};
use std::{
    collections::{HashMap, LinkedList},
    sync::{Arc, RwLock},
};
use tokio::sync::broadcast::{channel, Sender};

#[derive(Debug)]
pub struct AppContext {
    pub config: Arc<AppConfig>,
    pub persistence: Arc<PersistenceContext>,
    pub tickers: Arc<HashMap<String, RwLock<LinkedList<Ticker>>>>,
    pub protfolios: Arc<HashMap<String, HashMap<TimeUnit, RwLock<LinkedList<Protfolio>>>>>,
    // Sender for persist data
    pub house_keeper: Sender<TickerEvent>,
    // Sender for cache source data
    pub preparatory: Sender<TickerEvent>,
    // Sender for calculation
    pub calculator: Arc<HashMap<String, Sender<i64>>>,
}

impl AppContext {
    pub fn new(config: AppConfig) -> AppContext {
        let (house_keeper, _) = channel::<TickerEvent>(2048);
        let (preparatory, _) = channel::<TickerEvent>(2048);

        let config_ref = Arc::new(config);
        let calculator = AppContext::init_sender(Arc::clone(&config_ref));
        let persistence = PersistenceContext::new(Arc::clone(&config_ref));
        let tickers = AppContext::init_tickers(Arc::clone(&config_ref));
        let protfolios = AppContext::init_protfolios(Arc::clone(&config_ref));

        AppContext {
            config: config_ref,
            persistence: Arc::new(persistence),
            tickers,
            protfolios,
            house_keeper,
            preparatory,
            calculator,
        }
    }

    fn init_tickers(config: Arc<AppConfig>) -> Arc<HashMap<String, RwLock<LinkedList<Ticker>>>> {
        let symbols = config.symbols();
        let mut map: HashMap<String, RwLock<LinkedList<Ticker>>> = HashMap::new();
        for symbol in symbols {
            map.insert(symbol, RwLock::new(LinkedList::new()));
        }
        Arc::new(map)
    }
    // Init container

    fn init_protfolios(
        config: Arc<AppConfig>,
    ) -> Arc<HashMap<String, HashMap<TimeUnit, RwLock<LinkedList<Protfolio>>>>> {
        let symbols = config.symbols();
        let units = TimeUnit::values();
        let mut map: HashMap<String, HashMap<TimeUnit, RwLock<LinkedList<Protfolio>>>> =
            HashMap::new();
        for symbol in symbols {
            let mut uniter: HashMap<TimeUnit, RwLock<LinkedList<Protfolio>>> = HashMap::new();
            for unit in &units {
                uniter.insert(*unit, RwLock::new(LinkedList::new()));
            }
            map.insert(symbol, uniter);
        }
        Arc::new(map)
    }

    fn init_sender(config: Arc<AppConfig>) -> Arc<HashMap<String, Sender<i64>>> {
        let symbols = config.symbols();
        let mut map: HashMap<String, Sender<i64>> = HashMap::new();
        for symbol in symbols {
            let (calculator, _) = channel::<i64>(2048);
            map.insert(symbol, calculator);
        }
        Arc::new(map)
    }

    pub async fn init(self) -> Result<Arc<AppContext>> {
        let me = Arc::new(self);
        init_dispatcher(&Arc::clone(&me)).await?;
        // FIXME: init mongo for temp solution
        Arc::clone(&me).persistence.init_mongo().await?;
        Ok(Arc::clone(&me))
    }

    pub async fn dispatch(&self, ticker: &Ticker) -> Result<()> {
        if self.config.data_source.mongodb.enabled || self.config.data_source.elasticsearch.enabled
        {
            self.house_keeper.send(ticker.into())?;
        }
        self.preparatory.send(ticker.into())?;
        Ok(())
    }

    pub async fn dispatch_direct(&self, ticker: &Ticker) -> Result<()> {
        // save data
        if self.config.data_source.mongodb.enabled {
            ticker.save_to_mongo(Arc::clone(&self.persistence)).await?;
        }
        if self.config.data_source.elasticsearch.enabled {
            let es_ticker: ElasticTicker = (*ticker).clone().into();
            es_ticker
                .save_to_elasticsearch(Arc::clone(&self.persistence))
                .await?;
        }

        // Add into source list
        let tickers = Arc::clone(&self.tickers);
        if let Some(lock) = tickers.get(&ticker.id) {
            let mut list = lock.write().unwrap();
            list.push_front(ticker.clone());
        } else {
            error!("No tickers container {} initialized", &ticker.id);
        }

        // calculate protfolios
        TimeUnit::values().par_iter_mut().for_each(|unit| {
            debug!("route calculation: {:?} of {}", unit, &ticker.id);
            self.route(&ticker.id, &unit).unwrap();
        });
        Ok(())
    }
}

#[derive(Debug, Deserialize, Serialize, Clone)]
pub struct AppConfig {
    #[serde(rename = "dataSource")]
    pub data_source: DataSource,
    pub analysis: AnalysisBehavior,
    pub tickers: TickerList,
}

impl AppConfig {
    pub fn load(file: &str) -> Result<AppConfig> {
        let settings = Config::builder()
            .add_source(config::File::with_name(file))
            .set_default("analysis.output.baseFolder", "tmp")?
            .set_default("dataSource.mongodb.target", "yahoo")?
            .build()?;

        let config: AppConfig = settings.try_deserialize::<AppConfig>()?;
        Ok(config)
    }
    pub fn symbols(&self) -> Vec<String> {
        self.tickers
            .symbols
            .iter()
            .flat_map(|g| [&g.bear.id, &g.bull.id])
            .map(|id| id.to_string())
            .collect::<Vec<String>>()
    }
}

#[derive(Debug, Deserialize, Serialize, Clone)]
pub struct DataSource {
    pub mongodb: DataSourceInfo,
    pub elasticsearch: DataSourceInfo,
}

#[derive(Debug, Deserialize, Serialize, Clone)]
pub struct DataSourceInfo {
    pub uri: String,
    pub enabled: bool,
    pub target: Option<String>,
}

#[derive(Debug, Deserialize, Serialize, Clone)]
pub struct AnalysisBehavior {
    pub output: Outputs,
}

#[derive(Debug, Deserialize, Serialize, Clone)]
pub struct Outputs {
    #[serde(rename = "baseFolder")]
    pub base_folder: String,
    pub file: OutputType,
    pub elasticsearch: OutputType,
}

#[derive(Debug, Deserialize, Serialize, Clone)]
pub struct OutputType {
    pub enabled: bool,
}

#[derive(Debug, Deserialize, Serialize, Clone)]
pub struct TickerList {
    pub symbols: Vec<TickerGroup>,
}

#[derive(Debug, Deserialize, Serialize, Clone)]
pub struct TickerGroup {
    pub bull: Symbol,
    pub bear: Symbol,
}

#[derive(Debug, Deserialize, Serialize, Clone)]
pub struct Symbol {
    pub id: String,
}
