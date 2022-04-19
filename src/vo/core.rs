use super::biz::{
    MarketHoursType, Order, OrderStatus, Protfolio, Ticker, TimeUnit, TradeInfo, Trend,
};
use crate::{
    analysis::{init_dispatcher, trade::prepare_trade},
    persist::{es::ElasticTicker, mongo::get_start_time, PersistenceContext},
    proto::biz::TickerEvent,
    Result,
};
use chrono::{Duration, Utc};
use config::Config;
use log::*;
use rayon::prelude::*;
use serde::{Deserialize, Serialize};
use std::{
    cmp::max,
    collections::{BTreeMap, HashMap, HashSet, LinkedList},
    sync::{Arc, Mutex, RwLock},
};
use tokio::sync::broadcast::{channel, Receiver, Sender};

pub const KEY_EXTRA_PRCOESS_IN_ASYNC: &str = "process_in_async";
pub const KEY_EXTRA_ENABLE_DATA_TRUNCAT: &str = "enable_clean_data_before_operation";
pub const KEY_EXTRA_PRINT_TRADE_META_START_TIME: &str = "print_trade_meta_start_time";
pub const KEY_EXTRA_PRINT_TRADE_META_END_TIME: &str = "print_trade_meta_end_time";

pub type LockTradeInfo = Arc<RwLock<TradeInfo>>;
pub type LockListMap<T> = BTreeMap<String, RwLock<LinkedList<T>>>;

#[derive(Debug)]
pub struct AppContext {
    config: Arc<AppConfig>,
    persistence: Arc<PersistenceContext>,
    asset: Arc<AssetContext>,
    post_man: Arc<PostMan>,
}

impl AppContext {
    pub fn new(app_config: AppConfig) -> Self {
        let config = Arc::new(app_config);
        let persistence = PersistenceContext::new(Arc::clone(&config));
        let asset = AssetContext::new(Arc::clone(&config));
        let post_man = PostMan::new(Arc::clone(&config));

        Self {
            config: Arc::clone(&config),
            persistence: Arc::new(persistence),
            asset: Arc::new(asset),
            post_man: Arc::new(post_man),
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

    pub fn post_man(&self) -> Arc<PostMan> {
        Arc::clone(&self.post_man)
    }

    pub async fn init(self) -> Result<Arc<Self>> {
        let me = Arc::new(self);
        if me.config().async_process() {
            init_dispatcher(&Arc::clone(&me)).await?;
        }
        // FIXME: init mongo for temp solution
        let persistence = me.persistence();
        persistence.init_mongo().await?;
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
        if self.config().sync_mongo_enabled() || self.config().sync_elasticsearch_enabled() {
            let mut event: TickerEvent = ticker.into();
            // calculate volume
            event.volume = volume_diff;
            self.post_man().store(event)?;
        }

        // send to analysis
        let mut event: TickerEvent = ticker.into();
        event.volume = volume_diff;
        self.post_man().prepare(event)?;

        let asset = self.asset();

        match ticker.market_hours {
            MarketHoursType::PreMarket => {
                // update time of pre-market for getting regular market start time
                asset.set_regular_start_time(ticker.time);
            }
            MarketHoursType::RegularMarket => {
                // runtime broken and restarted while regular market period
                if asset.get_regular_start_time() == 0 {
                    let start_time = get_start_time(self.persistence(), self.config()).await;
                    asset.set_regular_start_time(start_time);
                }
            }
            _ => {}
        }

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

        let asset = self.asset();
        let config = self.config();
        let mut units = config.time_units();
        // only take moving data
        let unit_size = units.iter().filter(|u| u.period > 0).count();

        // Add ticker decision data first (id/time... with empty analysis data)
        let trade = TradeInfo::from(ticker, message_id, unit_size, true);
        asset.add_trade(&ticker.id, trade);

        // calculate protfolios, speed up using parallel loop (change to normal loop when debugging log order)
        units.par_iter_mut().for_each(|unit| {
            self.route(message_id, &ticker.id, &unit).unwrap();

            // check all values finalized and push
            if asset.is_trade_finalized(&ticker.id, message_id) {
                prepare_trade(self.asset(), self.config(), message_id).unwrap();
            }

            match ticker.market_hours {
                MarketHoursType::PreMarket => {
                    // update time of pre-market for getting regular market start time
                    asset.set_regular_start_time(ticker.time);
                }
                _ => {}
            }
        });

        Ok(())
    }
}

#[derive(Debug, Clone)]
pub struct AssetContext {
    config: Arc<AppConfig>,
    // source data
    tickers: Arc<LockListMap<Ticker>>,
    // aggregated trade data
    trades: Arc<LockListMap<LockTradeInfo>>,
    // computed trend info
    protfolios: Arc<HashMap<String, LockListMap<Protfolio>>>,
    // placed orders
    orders: Arc<RwLock<LinkedList<Order>>>,
    // number of generating ID
    sequence: Arc<Mutex<i64>>,
    // start time of regular market
    regular_start_time: Arc<Mutex<i64>>,
}

impl AssetContext {
    pub fn new(config: Arc<AppConfig>) -> Self {
        let tickers = Self::init_tickers(Arc::clone(&config));
        let protfolios = Self::init_protfolios(Arc::clone(&config));
        let trades = Self::init_trades(Arc::clone(&config));

        Self {
            config: Arc::clone(&config),
            tickers: Arc::new(tickers),
            protfolios: Arc::new(protfolios),
            trades: Arc::new(trades),
            orders: Arc::new(RwLock::new(LinkedList::new())),
            sequence: Arc::new(Mutex::new(
                Utc::now().timestamp_millis() % Duration::days(3).num_milliseconds(),
            )),
            regular_start_time: Arc::new(Mutex::new(0)),
        }
    }

    fn init_tickers(config: Arc<AppConfig>) -> LockListMap<Ticker> {
        let symbols = config.symbols();
        let mut map: LockListMap<Ticker> = BTreeMap::new();
        for symbol in symbols {
            map.insert(symbol, RwLock::new(LinkedList::new()));
        }
        map
    }

    fn init_protfolios(config: Arc<AppConfig>) -> HashMap<String, LockListMap<Protfolio>> {
        let symbols = config.symbols();
        let mut map: HashMap<String, LockListMap<Protfolio>> = HashMap::new();
        for symbol in symbols {
            let mut uniter: LockListMap<Protfolio> = BTreeMap::new();
            for unit in config.time_units() {
                uniter.insert(unit.name.clone(), RwLock::new(LinkedList::new()));
            }
            map.insert(symbol, uniter);
        }
        map
    }

    fn init_trades(config: Arc<AppConfig>) -> LockListMap<LockTradeInfo> {
        let symbols = config.symbols();
        let mut map: LockListMap<LockTradeInfo> = BTreeMap::new();
        for symbol in symbols {
            map.insert(symbol, RwLock::new(LinkedList::new()));
        }
        map
    }

    pub fn tickers(&self) -> Arc<LockListMap<Ticker>> {
        Arc::clone(&self.tickers)
    }

    pub fn protfolios(&self) -> Arc<HashMap<String, LockListMap<Protfolio>>> {
        Arc::clone(&self.protfolios)
    }

    pub fn trades(&self) -> Arc<LockListMap<LockTradeInfo>> {
        Arc::clone(&self.trades)
    }

    pub fn orders(&self) -> Arc<RwLock<LinkedList<Order>>> {
        Arc::clone(&self.orders)
    }

    pub fn symbol_tickers(&self, symbol: &str) -> Option<&RwLock<LinkedList<Ticker>>> {
        self.tickers.get(symbol)
    }

    pub fn get_current_market(&self, symbol: &str) -> Option<MarketHoursType> {
        let lock = self.tickers.get(symbol).unwrap();
        let reader = lock.read().unwrap();
        if let Some(ticker) = reader.front() {
            Some(ticker.market_hours)
        } else {
            None
        }
    }

    pub fn get_first_post_ticker(&self, symbol: &str) -> Option<Ticker> {
        let lock = self.tickers.get(symbol).unwrap();
        let reader = lock.read().unwrap();
        let first_trade = reader
            .iter()
            .filter(|ticker| matches!(ticker.market_hours, MarketHoursType::PostMarket))
            .last();

        if let Some(trade) = first_trade {
            Some(trade.clone())
        } else {
            None
        }
    }

    pub fn get_latest_ticker(&self, symbol: &str) -> Option<Ticker> {
        let lock = self.tickers.get(symbol).unwrap();
        let reader = lock.read().unwrap();
        if let Some(ticker) = reader.front() {
            Some(ticker.clone())
        } else {
            None
        }
    }

    pub fn get_latest_rival_ticker(&self, symbol: &str) -> Option<Ticker> {
        if let Some(rival_symbol) = self.find_rival_symbol(symbol) {
            self.get_latest_ticker(&rival_symbol)
        } else {
            None
        }
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

    pub fn next_message_id(&self) -> i64 {
        let mut guard = self.sequence.lock().unwrap();
        *guard += 1;
        let value = *guard;
        value
    }

    pub fn set_regular_start_time(&self, value: i64) {
        if let Ok(mut guard) = self.regular_start_time.lock() {
            *guard = value;
        }
    }

    pub fn get_regular_start_time(&self) -> i64 {
        if let Ok(guard) = self.regular_start_time.lock() {
            let value = *guard;
            return value;
        }
        0
    }

    pub fn regular_marketing_closing(&self, time: i64) -> bool {
        // regular market duration: 390 min, 2 min to prepare closing
        let duration = Duration::minutes(390 - 2).num_milliseconds();
        let start_time = self.get_regular_start_time();
        start_time > 0 && time > start_time + duration
    }

    pub fn consumer_closable(&self, time: i64) -> bool {
        // regular market duration: 390 min, 30 min to exit consuming after regular market
        let duration = Duration::minutes(390 + 30).num_milliseconds();
        let start_time = self.get_regular_start_time();
        start_time > 0 && time > start_time + duration
    }

    pub fn add_trade(&self, symbol: &str, trade: TradeInfo) {
        let lock = self.symbol_trades(symbol).unwrap();
        let mut trades = lock.write().unwrap();
        if log_enabled!(log::Level::Debug) {
            debug!("add_trade: {} - {:?}", symbol, &trade.clone());
        }
        // TODO: message might not be sequential, make sure message are in time sort
        trades.push_front(Arc::new(RwLock::new(trade)));
    }

    pub fn symbol_trades(&self, symbol: &str) -> Option<&RwLock<LinkedList<LockTradeInfo>>> {
        self.trades.get(symbol)
    }

    pub fn search_trade(&self, message_id: i64) -> Option<LockTradeInfo> {
        let trades = Arc::clone(&self.trades);
        for (_symbol, lock) in trades.as_ref() {
            let reader = lock.read().unwrap();
            let result = reader
                .iter()
                .take(5) // only take 5 to check, expect to be the first one
                .find(|s| s.read().unwrap().message_id == message_id);

            if let Some(value) = result {
                return Some(Arc::clone(value));
            }
        }
        None
    }

    pub fn find_trade(&self, symbol: &str, message_id: i64) -> Option<LockTradeInfo> {
        let trades_lock = self.symbol_trades(symbol).unwrap();
        let trades = trades_lock.read().unwrap();
        let trade = trades
            .iter()
            .find(|s| s.read().unwrap().message_id == message_id)
            .unwrap();
        Some(Arc::clone(trade))
    }

    // check if all trades added into ticker point
    pub fn is_trade_finalized(&self, symbol: &str, message_id: i64) -> bool {
        if let Some(lock) = self.find_trade(symbol, message_id) {
            let trade = lock.read().unwrap();
            trade.finalized()
        } else {
            false
        }
    }

    pub fn get_latest_trade(&self, symbol: &str) -> Option<TradeInfo> {
        let lock = self.trades.get(symbol).unwrap();
        let reader = lock.read().unwrap();
        if let Some(trade_lock) = reader.front() {
            let trade_reader = trade_lock.read().unwrap();
            Some(trade_reader.clone())
        } else {
            None
        }
    }

    pub fn add_order(&self, order: Order) -> bool {
        let lock = &self.orders;
        let mut writer = lock.write().unwrap();
        if let Some(exists_order) = writer
            .iter()
            .filter(|o| o.symbol == order.symbol)
            .filter(|o| matches!(o.status, OrderStatus::Init | OrderStatus::Accepted))
            .next()
        {
            debug!("find exists order {:?}", exists_order);
            false
        } else {
            debug!(
                "new order: [{}] {:<12} price: {:<7}, rival price: {:<7}, volume: {}",
                &order.symbol,
                format!("{:?}", &order.audit),
                order.created_price,
                order.created_rival_price,
                order.created_volume,
            );
            writer.push_front(order);
            true
        }
    }

    pub fn find_rival_symbol(&self, symbol: &str) -> Option<String> {
        let config = Arc::clone(&self.config);
        if let Some(ticker_group) = config
            .tickers
            .symbols
            .iter()
            .find(|group| group.bull.id == symbol)
        {
            return Some(ticker_group.bear.id.clone());
        }

        if let Some(ticker_group) = config
            .tickers
            .symbols
            .iter()
            .find(|group| group.bear.id == symbol)
        {
            return Some(ticker_group.bull.id.clone());
        }

        None
    }

    pub fn write_off(&self, order: &Order) {
        self.finalize_order(order, OrderStatus::WriteOff);
    }

    pub fn realized_loss(&self, order: &Order) {
        self.finalize_order(order, OrderStatus::LossPair);
    }

    fn finalize_order(&self, order: &Order, status: OrderStatus) {
        let symbol = &order.symbol;
        let rival_symbol = self.find_rival_symbol(symbol).unwrap();

        if let Some(rival_order) = self.find_running_order(&rival_symbol) {
            let constraint_id = format!("P{}", self.next_message_id());
            let lock = Arc::clone(&self.orders);
            let mut writer = lock.write().unwrap();

            let count = writer
                .iter()
                .filter(|o| o.id == rival_order.id || o.id == order.id)
                .count();

            if count == 2 {
                {
                    for o in writer.iter_mut().filter(|o| o.id == rival_order.id) {
                        o.write_off_time = order.accepted_time; // FIXME: accepted time to current time
                        o.status = status.clone();
                        o.constraint_id = Some(constraint_id.clone());
                    }
                }
                {
                    for o in writer.iter_mut().filter(|o| o.id == order.id) {
                        o.write_off_time = order.accepted_time; // FIXME: accepted time to current time
                        o.status = status.clone();
                        o.constraint_id = Some(constraint_id.clone());
                    }
                }
            }
        }
    }

    pub fn find_running_order(&self, symbol: &str) -> Option<Order> {
        let lock = &self.orders;
        let reader = lock.read().unwrap();
        if let Some(order) = reader
            .iter()
            .filter(|o| o.symbol == symbol)
            .filter(|o| matches!(o.status, OrderStatus::Init | OrderStatus::Accepted))
            .next()
        {
            Some(order.clone())
        } else {
            None
        }
    }

    pub fn find_last_flash_order(&self, symbol: &str) -> Option<Order> {
        let lock = &self.orders;
        let reader = lock.read().unwrap();
        if let Some(order) = reader
            .iter()
            .filter(|o| o.symbol == symbol)
            .filter(|o| {
                matches!(
                    o.status,
                    OrderStatus::Init
                        | OrderStatus::Accepted
                        | OrderStatus::WriteOff
                        | OrderStatus::LossPair
                )
            })
            .next()
        {
            Some(order.clone())
        } else {
            None
        }
    }

    pub fn find_orders_by_symbol(&self, symbols: &Vec<String>) -> Vec<Order> {
        let reader = self.orders.read().unwrap();
        reader
            .iter()
            .filter(|o| symbols.contains(&o.symbol))
            .map(|o| o.clone())
            .collect()
    }

    pub fn has_running_orders(&self, symbols: &Vec<String>) -> Option<String> {
        let running_symbols: HashSet<String> = self
            .find_orders_by_symbol(symbols)
            .iter()
            .filter(|o| matches!(o.constraint_id, None))
            .map(|o| o.symbol.clone())
            .collect();

        // assume that there must be only one symbols, not possible both bear and bulk exists
        match running_symbols.len() {
            0 => None,
            _ => Some(running_symbols.into_iter().next().unwrap()),
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
        self.trades.iter().for_each(|(id, lock)| {
            let mut list_writer = lock.write().unwrap();
            list_writer.clear();
            debug!("Clean up cached data for trades: {}", id)
        });
        Ok(())
    }
}

#[derive(Debug, Clone)]
pub struct PostMan {
    // Sender for persist data
    house_keeper: Sender<TickerEvent>,
    // Sender for cache source data
    preparatory: Sender<TickerEvent>,
    // Sender for calculation
    calculator: Arc<HashMap<String, Sender<i64>>>,
    // Sender for trading
    trader: Sender<i64>,
}

impl PostMan {
    pub fn new(config: Arc<AppConfig>) -> Self {
        let (house_keeper, _) = channel::<TickerEvent>(2048);
        let (preparatory, _) = channel::<TickerEvent>(2048);
        let calculator = Self::init_sender(Arc::clone(&config));
        let (trader, _) = channel::<i64>(128);

        let post_man = Self {
            house_keeper,
            preparatory,
            calculator: Arc::new(calculator),
            trader,
        };

        post_man
    }

    fn init_sender(config: Arc<AppConfig>) -> HashMap<String, Sender<i64>> {
        let symbols = config.symbols();
        let mut map: HashMap<String, Sender<i64>> = HashMap::new();
        for symbol in symbols {
            let (calculator, _) = channel::<i64>(2048);
            map.insert(symbol, calculator);
        }
        map
    }

    pub fn subscribe_store(&self) -> Receiver<TickerEvent> {
        self.house_keeper.subscribe()
    }

    pub fn subscribe_prepare(&self) -> Receiver<TickerEvent> {
        self.preparatory.subscribe()
    }

    pub fn subscribe_calculate(&self, symbol: &str) -> Receiver<i64> {
        self.calculator.get(symbol).unwrap().subscribe()
    }

    pub fn subscribe_trade(&self) -> Receiver<i64> {
        self.trader.subscribe()
    }

    pub fn store(&self, event: TickerEvent) -> Result<usize> {
        let result = self.house_keeper.send(event)?;
        Ok(result)
    }

    pub fn prepare(&self, event: TickerEvent) -> Result<usize> {
        let result = self.preparatory.send(event)?;
        Ok(result)
    }

    pub fn calculate(&self, symbol: &str, message_id: i64) -> Result<usize> {
        let sender = self.calculator.get(symbol).unwrap();
        let result = sender.send(message_id)?;
        Ok(result)
    }

    pub async fn watch_trade(&self, message_id: i64) -> Result<usize> {
        match self.trader.send(message_id) {
            Ok(result) => Ok(result),
            Err(err) => {
                error!("watch trade send message id failed: {:?}", err);
                Ok(0)
            }
        }
    }
}

#[derive(Debug, Deserialize, Serialize, Clone)]
pub struct AppConfig {
    #[serde(rename = "dataSource")]
    pub data_source: DataSource,
    pub platform: Platform,
    pub trade: TradeAudit,
    pub replay: ReplayBehavior,
    pub units: Vec<TimeUnit>,
    pub tickers: TickerList,
    #[serde(default = "empty_map", skip_serializing, skip_deserializing)]
    runtime: Arc<RwLock<HashMap<String, String>>>,
}

fn empty_map() -> Arc<RwLock<HashMap<String, String>>> {
    Arc::new(RwLock::new(HashMap::new()))
}

impl AppConfig {
    pub fn load(file: &str) -> Result<Self> {
        let settings = Config::builder()
            .add_source(config::File::with_name(file))
            .set_default("replay.outputs.baseFolder", "tmp")?
            .set_default("dataSource.mongodb.target", "yahoo")?
            .build()?;

        let config: Self = settings.try_deserialize::<Self>()?;
        Ok(config)
    }

    pub fn extra_put(&self, key: &str, value: &str) {
        if let Ok(mut lock) = self.runtime.write() {
            lock.insert(key.to_string(), value.to_string());
        }
    }

    pub fn extra_get(&self, key: &str) -> Option<String> {
        let lock = self.runtime.read().unwrap();
        if let Some(value) = lock.get(key) {
            Some(value.to_string())
        } else {
            None
        }
    }

    pub fn extra_present(&self, key: &str) -> bool {
        let lock = self.runtime.read().unwrap();
        lock.contains_key(key)
    }

    pub fn symbols(&self) -> Vec<String> {
        self.tickers
            .symbols
            .iter()
            .flat_map(|g| [&g.bear.id, &g.bull.id])
            .map(|id| id.to_string())
            .collect::<Vec<String>>()
    }

    pub fn time_units(&self) -> Vec<TimeUnit> {
        self.units
            .iter()
            .map(|u| u.clone())
            .collect::<Vec<TimeUnit>>()
    }

    pub fn find_unit(&self, name: &str) -> Option<TimeUnit> {
        self.time_units().into_iter().find(|u| u.name == name)
    }

    pub fn async_process(&self) -> bool {
        self.extra_present(KEY_EXTRA_PRCOESS_IN_ASYNC)
    }

    pub fn sync_mongo_enabled(&self) -> bool {
        self.data_source.mongodb.enabled && self.async_process()
    }

    pub fn sync_elasticsearch_enabled(&self) -> bool {
        self.data_source.elasticsearch.enabled && self.async_process()
    }

    pub fn truncat_enabled(&self) -> bool {
        self.extra_present(KEY_EXTRA_ENABLE_DATA_TRUNCAT)
    }
}

#[derive(Debug, Deserialize, Serialize, Clone)]
pub struct DataSource {
    pub mongodb: DataSourceInfo,
    pub elasticsearch: DataSourceInfo,
    pub grafana: DataSourceInfo,
}

#[derive(Debug, Deserialize, Serialize, Clone)]
pub struct DataSourceInfo {
    pub uri: String,
    pub enabled: bool,
    pub target: Option<String>,
    pub auth: Option<String>,
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
pub struct TradeAudit {
    pub enabled: bool,
    // max aount to per single order
    #[serde(rename = "maxOrderAmount")]
    pub max_order_amount: u32,
    pub flash: AuditMode,
    pub slug: AuditMode,
    // used to prevent loss, check downward trend while profit still positive
    pub revert: AuditMode,
}

#[derive(Debug, Deserialize, Serialize, Clone)]
pub struct AuditMode {
    // loss margin on trend downward
    #[serde(rename = "lossMarginRate")]
    pub loss_margin_rate: f32,
    pub rules: Vec<AuditRule>,
}

#[derive(Debug, Deserialize, Serialize, Clone)]
pub struct AuditRule {
    #[serde(default = "default_trends")]
    pub trends: Vec<TrendCriteria>,
    #[serde(default = "default_deviations")]
    pub deviations: Vec<DeviationCriteria>,
    #[serde(default = "default_oscillations")]
    pub oscillations: Vec<OscillationCriteria>,
    #[serde(default = "default_lowers")]
    pub lowers: Vec<LowerCriteria>,
    #[serde(default = "default_evaluation")]
    pub evaluation: bool,
    pub mode: AuditRuleType,
}

fn default_trends() -> Vec<TrendCriteria> {
    Vec::new()
}
fn default_deviations() -> Vec<DeviationCriteria> {
    Vec::new()
}
fn default_oscillations() -> Vec<OscillationCriteria> {
    Vec::new()
}
fn default_lowers() -> Vec<LowerCriteria> {
    Vec::new()
}

fn default_evaluation() -> bool {
    false
}

#[derive(Debug, Deserialize, Serialize, Clone, PartialEq, Eq)]
pub enum AuditRuleType {
    Permit,
    Deny,
}

#[derive(Debug, Deserialize, Serialize, Clone)]
pub struct TrendCriteria {
    pub from: Option<String>,
    pub to: String,
    pub trend: Trend,
    pub up: Option<String>,
    pub down: Option<String>,
}

fn value_compare(config: Option<String>, target: u32) -> bool {
    if let Some(value) = config {
        if value.ends_with("-") {
            let len = value.len() - 1;
            // ex: config: 10-, target 9, result true
            if value[..len].parse::<u32>().unwrap() <= target {
                return false;
            }
        } else if value.ends_with("+") {
            let len = value.len() - 1;
            // ex: config: 10+, target 9, result false
            if value[..len].parse::<u32>().unwrap() >= target {
                return false;
            }
        } else {
            // ex: config 10, target 9, result false
            if value.parse::<u32>().unwrap() != target {
                return false;
            }
        }
    }
    true
}

impl TrendCriteria {
    pub fn up_compare(&self, up: u32) -> bool {
        if let Some(value) = &self.up {
            value_compare(Some(value.to_string()), up)
        } else {
            true
        }
    }
    pub fn down_compare(&self, down: u32) -> bool {
        if let Some(value) = &self.down {
            value_compare(Some(value.to_string()), down)
        } else {
            true
        }
    }
}

#[derive(Debug, Deserialize, Serialize, Clone)]
pub struct DeviationCriteria {
    pub from: Option<String>,
    pub to: String,
    pub value: f32,
}

#[derive(Debug, Deserialize, Serialize, Clone)]
pub struct OscillationCriteria {
    pub from: Option<String>,
    pub to: String,
    pub value: f32,
}

#[derive(Debug, Deserialize, Serialize, Clone)]
pub struct LowerCriteria {
    pub from: Option<String>,
    pub to: String,
    #[serde(rename = "compareTo")]
    pub compare_to: String,
    pub duration: u32,
}

#[derive(Debug, Deserialize, Serialize, Clone)]
pub struct ReplayBehavior {
    pub exports: Vec<ContentType>,
    pub outputs: Outputs,
}

impl ReplayBehavior {
    pub fn export_enabled(&self, name: &str) -> bool {
        self.exports
            .iter()
            .filter(|content_type| content_type.enabled)
            .any(|content_type| content_type.name == name)
    }
}

#[derive(Debug, Deserialize, Serialize, Clone)]
pub struct ContentType {
    pub name: String,
    pub enabled: bool,
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
