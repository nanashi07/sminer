use super::biz::{Protfolio, SlopePoint, Ticker, TimeUnit};
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
    cmp::max,
    collections::{HashMap, LinkedList},
    sync::{Arc, RwLock},
};
use tokio::sync::broadcast::{channel, Sender};

pub const KEY_EXTRA_PRCOESS_IN_REPLAY: &str = "process_in_replay";
pub const KEY_EXTRA_ENABLE_DATA_TRUNCAT: &str = "enable_clean_data_before_operation";

pub type RefSlopePoint = Arc<RwLock<SlopePoint>>;
pub type LockListMap<T> = HashMap<String, RwLock<LinkedList<T>>>;

#[derive(Debug)]
pub struct AppContext {
    config: Arc<AppConfig>,
    persistence: Arc<PersistenceContext>,
    asset: Arc<AssetContext>,
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

        AppContext {
            config: Arc::clone(&config_ref),
            persistence: Arc::new(persistence),
            asset: AssetContext::new(Arc::clone(&config_ref)),
            house_keeper,
            preparatory,
            calculator,
        }
    }

    pub fn config(&self) -> Arc<AppConfig> {
        Arc::clone(&self.config)
    }

    pub fn persistence(&self) -> Arc<PersistenceContext> {
        Arc::clone(&self.persistence)
    }

    pub fn asset(&self) -> Arc<AssetContext> {
        Arc::clone(&self.asset)
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

    pub fn last_volume(&self, symbol: &str) -> i64 {
        let lock = self.asset.symbol_tickers(symbol).unwrap();
        let list = lock.read().unwrap();
        if let Some(ticker) = list.front() {
            ticker.day_volume
        } else {
            0
        }
    }

    pub async fn dispatch(&self, ticker: &Ticker) -> Result<()> {
        // calculate volume diff
        let volume_diff = max(0, ticker.day_volume - self.last_volume(&ticker.id));

        // send to persist
        if self.config.sync_mongo_enabled() || self.config.sync_elasticsearch_enabled() {
            let mut event: TickerEvent = ticker.into();
            // calculate volume
            event.volume = volume_diff;
            self.house_keeper.send(event)?;
        }

        // send to analysis
        let mut event: TickerEvent = ticker.into();
        event.volume = volume_diff;
        self.preparatory.send(event)?;

        Ok(())
    }

    // for test only
    pub async fn dispatch_direct(&self, ticker: &mut Ticker, message_id: i64) -> Result<()> {
        // calculate volume diff
        ticker.volume = Some(max(0, ticker.day_volume - self.last_volume(&ticker.id)));

        // save data
        if self.config.sync_mongo_enabled() {
            ticker.save_to_mongo(self.persistence()).await?;
        }
        if self.config.sync_elasticsearch_enabled() {
            let es_ticker: ElasticTicker = (*ticker).clone().into();
            es_ticker.save_to_elasticsearch(self.persistence()).await?;
        }

        // Add into source list
        if let Some(lock) = self.asset.symbol_tickers(&ticker.id) {
            let mut list = lock.write().unwrap();
            list.push_front(ticker.clone());
            debug!(
                "{} ticker size: {}, message_id: {}",
                ticker.id,
                list.len(),
                &message_id
            );
        } else {
            error!("No tickers container {} initialized", &ticker.id);
        }

        // Add ticker decision data first (id/time... with empty analysis data)
        let asset = self.asset();
        let slope = SlopePoint::from(ticker, message_id);
        asset.add_slope(&ticker.id, slope);

        // calculate protfolios, speed up using parallel loop (change to normal loop when debugging log order)
        TimeUnit::values().par_iter_mut().for_each(|unit| {
            self.route(message_id, &ticker.id, &unit).unwrap();

            // TODO: check all values finalized and push
            if asset.is_slope_closed(&ticker.id, message_id) {
                //
            }
        });

        Ok(())
    }
}

#[derive(Debug, Clone)]
pub struct AssetContext {
    tickers: Arc<LockListMap<Ticker>>,
    protfolios: Arc<HashMap<String, LockListMap<Protfolio>>>,
    slopes: Arc<LockListMap<RefSlopePoint>>,
}

impl AssetContext {
    pub fn new(config: Arc<AppConfig>) -> Arc<AssetContext> {
        let tickers = AssetContext::init_tickers(Arc::clone(&config));
        let slopes = AssetContext::init_slopes(Arc::clone(&config));
        let protfolios = AssetContext::init_protfolios(Arc::clone(&config));

        let asset = AssetContext {
            tickers,
            protfolios,
            slopes,
        };
        Arc::new(asset)
    }

    fn init_tickers(config: Arc<AppConfig>) -> Arc<LockListMap<Ticker>> {
        let symbols = config.symbols();
        let mut map: LockListMap<Ticker> = HashMap::new();
        for symbol in symbols {
            map.insert(symbol, RwLock::new(LinkedList::new()));
        }
        Arc::new(map)
    }

    fn init_protfolios(config: Arc<AppConfig>) -> Arc<HashMap<String, LockListMap<Protfolio>>> {
        let symbols = config.symbols();
        let units = TimeUnit::values();
        let mut map: HashMap<String, LockListMap<Protfolio>> = HashMap::new();
        for symbol in symbols {
            let mut uniter: LockListMap<Protfolio> = HashMap::new();
            for unit in &units {
                uniter.insert(unit.name.clone(), RwLock::new(LinkedList::new()));
            }
            map.insert(symbol, uniter);
        }
        Arc::new(map)
    }

    fn init_slopes(config: Arc<AppConfig>) -> Arc<LockListMap<RefSlopePoint>> {
        let symbols = config.symbols();
        let mut map: LockListMap<RefSlopePoint> = HashMap::new();
        for symbol in symbols {
            map.insert(symbol, RwLock::new(LinkedList::new()));
        }
        Arc::new(map)
    }

    pub fn tickers(&self) -> Arc<LockListMap<Ticker>> {
        Arc::clone(&self.tickers)
    }

    pub fn protfolios(&self) -> Arc<HashMap<String, LockListMap<Protfolio>>> {
        Arc::clone(&self.protfolios)
    }

    pub fn slopes(&self) -> Arc<LockListMap<RefSlopePoint>> {
        Arc::clone(&self.slopes)
    }

    pub fn symbol_tickers(&self, symbol: &str) -> Option<&RwLock<LinkedList<Ticker>>> {
        self.tickers.get(symbol)
    }

    pub fn symbol_protfolios(&self, symbol: &str) -> Option<&LockListMap<Protfolio>> {
        self.protfolios.get(symbol)
    }

    pub fn get_protfolios(
        &self,
        symbol: &str,
        unit: &str,
    ) -> Option<&RwLock<LinkedList<Protfolio>>> {
        if let Some(map) = self.protfolios.get(symbol) {
            map.get(unit)
        } else {
            None
        }
    }

    pub fn add_slope(&self, symbol: &str, slope: SlopePoint) {
        let lock = self.symbol_slopes(symbol).unwrap();
        let mut slope_list = lock.write().unwrap();
        slope_list.push_front(Arc::new(RwLock::new(slope)));
    }

    pub fn symbol_slopes(&self, symbol: &str) -> Option<&RwLock<LinkedList<RefSlopePoint>>> {
        self.slopes.get(symbol)
    }

    pub fn find_slope(&self, symbol: &str, message_id: i64) -> Option<RefSlopePoint> {
        let slopes_lock = self.symbol_slopes(symbol).unwrap();
        let slopes = slopes_lock.read().unwrap();
        let slope = slopes
            .iter()
            .find(|s| s.read().unwrap().message_id == message_id)
            .unwrap();
        Some(Arc::clone(slope))
    }

    // check if all slope added into ticker point
    pub fn is_slope_closed(&self, symbol: &str, message_id: i64) -> bool {
        if let Some(lock) = self.find_slope(symbol, message_id) {
            let slope = lock.read().unwrap();
            slope.finalized()
        } else {
            false
        }
    }

    pub fn clean(&self) -> Result<()> {
        self.tickers.iter().for_each(|(id, lock)| {
            let mut list_writer = lock.write().unwrap();
            list_writer.clear();
            debug!("Clean up cached data for ticker: {}", id)
        });
        self.protfolios.iter().for_each(|(id, map)| {
            map.iter().for_each(|(unit, lock)| {
                let mut list_writer = lock.write().unwrap();
                list_writer.clear();
                debug!("Clean up cached data for protfolio: {:?} of {}", unit, id)
            });
        });
        self.slopes.iter().for_each(|(id, lock)| {
            let mut list_writer = lock.write().unwrap();
            list_writer.clear();
            debug!("Clean up cached data for slopes: {}", id)
        });
        Ok(())
    }
}

#[derive(Debug, Deserialize, Serialize, Clone)]
pub struct AppConfig {
    #[serde(rename = "dataSource")]
    pub data_source: DataSource,
    pub platform: Platform,
    pub analysis: AnalysisBehavior,
    pub tickers: TickerList,
    #[serde(default = "empty_map")]
    runtime: HashMap<String, String>,
}

fn empty_map() -> HashMap<String, String> {
    HashMap::new()
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

    pub fn extra_put(&mut self, key: &str, value: &str) {
        self.runtime.insert(key.to_string(), value.to_string());
    }

    pub fn extra_get(&self, key: &str) -> Option<&String> {
        self.runtime.get(key)
    }

    pub fn extra_present(&self, key: &str) -> bool {
        self.runtime.contains_key(key)
    }

    pub fn symbols(&self) -> Vec<String> {
        self.tickers
            .symbols
            .iter()
            .flat_map(|g| [&g.bear.id, &g.bull.id])
            .map(|id| id.to_string())
            .collect::<Vec<String>>()
    }

    pub fn sync_mongo_enabled(&self) -> bool {
        self.data_source.mongodb.enabled && !self.extra_present(KEY_EXTRA_PRCOESS_IN_REPLAY)
    }

    pub fn sync_elasticsearch_enabled(&self) -> bool {
        self.data_source.elasticsearch.enabled && !self.extra_present(KEY_EXTRA_PRCOESS_IN_REPLAY)
    }

    pub fn truncat_enabled(&self) -> bool {
        self.extra_present(KEY_EXTRA_ENABLE_DATA_TRUNCAT)
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
pub struct Platform {
    pub yahoo: YahooFinance,
}

#[derive(Debug, Deserialize, Serialize, Clone)]
pub struct YahooFinance {
    pub uri: String,
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
