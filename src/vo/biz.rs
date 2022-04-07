use crate::proto::{biz::TickerEvent, yahoo::YahooTicker};
use chrono::Utc;
use rand::Rng;
use serde::{Deserialize, Serialize};
use std::{collections::BTreeMap, fmt::Display};

#[derive(Debug, Deserialize, Serialize)]
pub struct SubscribeCommand {
    pub subscribe: Vec<String>,
}

#[derive(Debug, Serialize, Deserialize, Clone, Copy)]
pub enum QuoteType {
    None = 0,
    Altsymbol = 5,
    Heartbeat = 7,
    Equity = 8,
    Index = 9,
    Mutualfund = 11,
    Moneymarket = 12,
    Option = 13,
    Currency = 14,
    Warrant = 15,
    Bond = 17,
    Future = 18,
    Etf = 20,
    Commodity = 23,
    Ecnquote = 28,
    Cryptocurrency = 41,
    Indicator = 42,
    Industry = 1000,
}

impl TryFrom<i32> for QuoteType {
    type Error = ();

    fn try_from(value: i32) -> Result<Self, Self::Error> {
        match value {
            x if x == QuoteType::None as i32 => Ok(QuoteType::None),
            x if x == QuoteType::Altsymbol as i32 => Ok(QuoteType::Altsymbol),
            x if x == QuoteType::Heartbeat as i32 => Ok(QuoteType::Heartbeat),
            x if x == QuoteType::Equity as i32 => Ok(QuoteType::Equity),
            x if x == QuoteType::Index as i32 => Ok(QuoteType::Index),
            x if x == QuoteType::Mutualfund as i32 => Ok(QuoteType::Mutualfund),
            x if x == QuoteType::Moneymarket as i32 => Ok(QuoteType::Moneymarket),
            x if x == QuoteType::Option as i32 => Ok(QuoteType::Option),
            x if x == QuoteType::Currency as i32 => Ok(QuoteType::Currency),
            x if x == QuoteType::Warrant as i32 => Ok(QuoteType::Warrant),
            x if x == QuoteType::Bond as i32 => Ok(QuoteType::Bond),
            x if x == QuoteType::Future as i32 => Ok(QuoteType::Future),
            x if x == QuoteType::Etf as i32 => Ok(QuoteType::Etf),
            x if x == QuoteType::Commodity as i32 => Ok(QuoteType::Commodity),
            x if x == QuoteType::Ecnquote as i32 => Ok(QuoteType::Ecnquote),
            x if x == QuoteType::Cryptocurrency as i32 => Ok(QuoteType::Cryptocurrency),
            x if x == QuoteType::Indicator as i32 => Ok(QuoteType::Indicator),
            x if x == QuoteType::Industry as i32 => Ok(QuoteType::Industry),
            _ => Err(()),
        }
    }
}

#[derive(Debug, Serialize, Deserialize, Clone, Copy)]
pub enum MarketHoursType {
    PreMarket = 0,
    RegularMarket = 1,
    PostMarket = 2,
    ExtendedHoursMarket = 3,
}

impl TryFrom<i32> for MarketHoursType {
    type Error = ();

    fn try_from(value: i32) -> Result<Self, Self::Error> {
        match value {
            x if x == MarketHoursType::PreMarket as i32 => Ok(MarketHoursType::PreMarket),
            x if x == MarketHoursType::RegularMarket as i32 => Ok(MarketHoursType::RegularMarket),
            x if x == MarketHoursType::PostMarket as i32 => Ok(MarketHoursType::PostMarket),
            x if x == MarketHoursType::ExtendedHoursMarket as i32 => {
                Ok(MarketHoursType::ExtendedHoursMarket)
            }
            _ => Err(()),
        }
    }
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct Ticker {
    pub id: String,
    pub price: f32,
    pub time: i64,

    pub quote_type: QuoteType,
    pub market_hours: MarketHoursType,
    pub day_volume: i64,
    #[serde(skip_serializing)]
    pub volume: Option<i64>,
    pub change: f32,
}

impl From<YahooTicker> for Ticker {
    fn from(value: YahooTicker) -> Self {
        Self {
            id: value.id,
            price: value.price,
            time: value.time,
            quote_type: value.quote_type.try_into().unwrap(),
            market_hours: value.market_hours.try_into().unwrap(),
            day_volume: value.day_volume,
            volume: None,
            change: value.change,
        }
    }
}

impl From<&Ticker> for TickerEvent {
    fn from(value: &Ticker) -> Self {
        Self {
            id: value.id.to_string(),
            price: value.price,
            time: value.time,
            quote_type: value.quote_type as i32,
            market_hours: value.market_hours as i32,
            day_volume: value.day_volume,
            volume: value.volume.unwrap_or(0),
            change: value.change,
        }
    }
}

impl From<TickerEvent> for Ticker {
    fn from(value: TickerEvent) -> Self {
        Self {
            id: value.id.to_string(),
            price: value.price,
            time: value.time,
            quote_type: value.quote_type.try_into().unwrap(),
            market_hours: value.market_hours.try_into().unwrap(),
            day_volume: value.day_volume,
            volume: if value.volume == 0 {
                None
            } else {
                Some(value.volume)
            },
            change: value.change,
        }
    }
}

#[derive(Debug, Serialize, Deserialize, Clone, PartialEq, Eq, Hash)]
pub struct TimeUnit {
    pub name: String,
    pub duration: i32,
    pub period: u32,
}

impl Display for TimeUnit {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.name)
    }
}

impl TimeUnit {
    pub fn new(name: &str, duration: i32, period: u32) -> TimeUnit {
        TimeUnit {
            name: name.to_string(),
            duration,
            period,
        }
    }

    pub fn is_moving_unit(name: &str) -> bool {
        name.starts_with("m")
    }
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct Protfolio {
    pub id: String,
    pub time: i64,
    pub price: f32,

    pub kind: char,

    pub quote_type: QuoteType,
    pub market_hours: MarketHoursType,

    pub volume: i64,

    // Calculation unit
    pub unit: TimeUnit,
    pub unit_time: i64,
    // Period type
    pub period_type: i32,

    pub max_price: f32,
    pub min_price: f32,
    pub open_price: f32,
    pub close_price: f32,

    pub sample_size: u32,
    pub slope: Option<f64>,
    pub b_num: Option<f64>,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct TradeInfo {
    pub id: String,
    pub time: i64,
    pub price: f32,

    #[serde(skip_serializing, skip_deserializing)]
    pub message_id: i64,

    pub kind: char,

    pub quote_type: QuoteType,
    pub market_hours: MarketHoursType,

    #[serde(skip_serializing, skip_deserializing)]
    pub replay: bool,
    #[serde(skip_serializing, skip_deserializing)]
    pub unit_size: usize,
    pub states: BTreeMap<String, Vec<f64>>,
}

impl TradeInfo {
    pub fn from(ticker: &Ticker, message_id: i64, unit_size: usize, replay: bool) -> Self {
        Self {
            id: ticker.id.clone(),
            time: ticker.time,
            price: ticker.price,
            message_id,
            kind: 't',
            quote_type: ticker.quote_type,
            market_hours: ticker.market_hours,
            unit_size,
            replay,
            states: BTreeMap::new(),
        }
    }

    pub fn update_state(&mut self, unit: &str, slope: Vec<f64>) {
        self.states.insert(unit.to_string(), slope);
    }

    pub fn finalized(&self) -> bool {
        self.unit_size == self.states.len()
    }

    pub fn action_time(&self) -> i64 {
        if self.replay {
            self.time
        } else {
            Utc::now().timestamp_millis()
        }
    }
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct SlopeLine {
    pub id: String,
    pub price: f64,
    pub time: i64,

    pub kind: char,

    // Period type
    pub period_type: i32,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct Order {
    pub id: String,
    pub symbol: String,
    pub created_time: i64,
    pub created_price: f32,
    pub created_rival_price: f32,
    pub created_volume: u32,
    pub accepted_time: Option<i64>,
    pub accepted_price: Option<f32>,
    pub accepted_volume: Option<u32>,
    pub write_off_time: Option<i64>,
    pub status: OrderStatus,
    pub audit: AuditState,
    // rival order ID
    pub constraint_id: Option<String>,
}

impl Order {
    pub fn new(
        symbol: &str,
        price: f32,
        rival_price: f32,
        volume: u32,
        time: i64,
        audit: AuditState,
    ) -> Self {
        Self {
            id: format!(
                "{}{}{}",
                symbol,
                Utc::now().timestamp_millis() % 31536000000,
                random_suffix()
            ),
            symbol: symbol.to_string(),
            created_time: time,
            created_price: price,
            created_rival_price: rival_price,
            created_volume: volume,
            accepted_time: None,
            accepted_price: None,
            accepted_volume: None,
            status: OrderStatus::Init,
            audit,
            constraint_id: None,
            write_off_time: None,
        }
    }
}

fn random_suffix() -> char {
    let chars = "ABCDEFGHIJKLMNOPQRSTUVWXYZ";
    let mut cc = chars.chars();
    let len = chars.len();
    let mut rng = rand::thread_rng();
    cc.nth(rng.gen_range(0..len)).unwrap()
}

#[derive(Debug, Serialize, Deserialize, Clone, PartialEq, Eq)]
pub enum OrderStatus {
    // order created
    Init,
    // order submitted successfully
    Accepted,
    // order submitted failed
    Rejected,
    // order has been paired done
    WriteOff,
    // order has been paried with loss
    LossPair,
}

#[derive(Debug, Serialize, Deserialize, Clone, PartialEq, Eq)]
pub enum AuditState {
    Flash,
    Slug,
    Loss,
    ProfitTaking,
    EarlySell,
    Decline,
}

#[derive(Debug, Deserialize, Serialize, Clone, PartialEq, Eq)]
pub enum Trend {
    Upward,
    Downward,
}

#[derive(Debug, Deserialize, Serialize, Clone)]
pub struct TradeTrend {
    pub unit: String,
    pub trend: Trend,
    pub rebound_at: i32,
    pub up_count: i32,
    pub down_count: i32,
}
