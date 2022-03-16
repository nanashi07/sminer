use crate::proto::yahoo::YahooMarketHoursType;
use crate::proto::yahoo::YahooQuoteType;
use crate::proto::yahoo::YahooTicker;
use serde::{Deserialize, Serialize};

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

impl From<YahooQuoteType> for QuoteType {
    fn from(value: YahooQuoteType) -> Self {
        match value {
            YahooQuoteType::None => QuoteType::None,
            YahooQuoteType::Altsymbol => QuoteType::Altsymbol,
            YahooQuoteType::Heartbeat => QuoteType::Heartbeat,
            YahooQuoteType::Equity => QuoteType::Equity,
            YahooQuoteType::Index => QuoteType::Index,
            YahooQuoteType::Mutualfund => QuoteType::Mutualfund,
            YahooQuoteType::Moneymarket => QuoteType::Moneymarket,
            YahooQuoteType::Option => QuoteType::Option,
            YahooQuoteType::Currency => QuoteType::Currency,
            YahooQuoteType::Warrant => QuoteType::Warrant,
            YahooQuoteType::Bond => QuoteType::Bond,
            YahooQuoteType::Future => QuoteType::Future,
            YahooQuoteType::Etf => QuoteType::Etf,
            YahooQuoteType::Commodity => QuoteType::Commodity,
            YahooQuoteType::Ecnquote => QuoteType::Ecnquote,
            YahooQuoteType::Cryptocurrency => QuoteType::Cryptocurrency,
            YahooQuoteType::Indicator => QuoteType::Indicator,
            YahooQuoteType::Industry => QuoteType::Industry,
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

impl From<YahooMarketHoursType> for MarketHoursType {
    fn from(value: YahooMarketHoursType) -> Self {
        match value {
            YahooMarketHoursType::PreMarket => MarketHoursType::PreMarket,
            YahooMarketHoursType::RegularMarket => MarketHoursType::RegularMarket,
            YahooMarketHoursType::PostMarket => MarketHoursType::PostMarket,
            YahooMarketHoursType::ExtendedHoursMarket => MarketHoursType::ExtendedHoursMarket,
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
    pub change: f32,
}

impl Ticker {}

impl From<YahooTicker> for Ticker {
    fn from(value: YahooTicker) -> Self {
        Ticker {
            id: value.id,
            price: value.price,
            time: value.time,
            quote_type: YahooQuoteType::from_i32(value.quote_type).unwrap().into(),
            market_hours: YahooMarketHoursType::from_i32(value.market_hours)
                .unwrap()
                .into(),
            day_volume: value.day_volume,
            change: value.change,
        }
    }
}

impl From<&Ticker> for Ticker {
    fn from(value: &Ticker) -> Self {
        Ticker {
            id: value.id.to_string(),
            price: value.price,
            time: value.time,
            quote_type: value.quote_type,
            market_hours: value.market_hours,
            day_volume: value.day_volume,
            change: value.change,
        }
    }
}
