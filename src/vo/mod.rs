use serde::{Deserialize, Serialize};

use crate::proto::YahooMarketHoursType;
use crate::proto::YahooOptionType;
use crate::proto::YahooQuoteType;
use crate::proto::YahooTicker;

#[derive(Debug, Deserialize, Serialize)]
pub struct SubscribeCommand {
    pub subscribe: Vec<String>,
}

#[derive(Debug, Serialize, Deserialize)]
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

#[derive(Debug, Serialize, Deserialize)]
pub enum OptionType {
    Call = 0,
    Put = 1,
}

#[derive(Debug, Serialize, Deserialize)]
pub enum MarketHoursType {
    PreMarket = 0,
    RegularMarket = 1,
    PostMarket = 2,
    ExtendedHoursMarket = 3,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct Ticker {
    pub id: String,
    pub price: f32,
    pub time: i64,

    pub quote_type: QuoteType,
    pub market_hours: MarketHoursType,
    pub day_volume: i64,
    pub day_high: f32,
    pub day_low: f32,
    pub change: f32,
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

impl From<YahooOptionType> for OptionType {
    fn from(value: YahooOptionType) -> Self {
        match value {
            YahooOptionType::Call => OptionType::Call,
            YahooOptionType::Put => OptionType::Put,
        }
    }
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
            day_high: value.day_high,
            day_low: value.day_low,
            change: value.change,
        }
    }
}
