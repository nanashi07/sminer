use std::fmt::Display;

use crate::proto::biz::TickerEvent;
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
        Ticker {
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
        TickerEvent {
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
        Ticker {
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
    pub fn values() -> Vec<TimeUnit> {
        vec![
            TimeUnit {
                name: String::from("SecondTen"),
                duration: 10,
                period: 0,
            },
            TimeUnit {
                name: String::from("SecondThirty"),
                duration: 30,
                period: 0,
            },
            TimeUnit {
                name: String::from("MinuteOne"),
                duration: 60,
                period: 0,
            },
            TimeUnit {
                name: String::from("MinuteTwo"),
                duration: 120,
                period: 0,
            },
            TimeUnit {
                name: String::from("MinuteThree"),
                duration: 180,
                period: 0,
            },
            TimeUnit {
                name: String::from("MinuteFour"),
                duration: 240,
                period: 0,
            },
            TimeUnit {
                name: String::from("MinuteFive"),
                duration: 300,
                period: 0,
            },
            TimeUnit {
                name: String::from("MinuteTen"),
                duration: 600,
                period: 0,
            },
            TimeUnit {
                name: String::from("MinuteTwenty"),
                duration: 1200,
                period: 0,
            },
            TimeUnit {
                name: String::from("MinuteThirty"),
                duration: 1800,
                period: 0,
            },
            TimeUnit {
                name: String::from("HourOne"),
                duration: 3600,
                period: 0,
            },
            TimeUnit {
                name: String::from("MovingSecondTen"),
                duration: 10,
                period: 60,
            },
            TimeUnit {
                name: String::from("MovingSecondTwenty"),
                duration: 20,
                period: 30,
            },
            TimeUnit {
                name: String::from("MovingSecondThirty"),
                duration: 30,
                period: 20,
            },
            TimeUnit {
                name: String::from("MovingMinuteOne"),
                duration: 60,
                period: 10,
            },
            TimeUnit {
                name: String::from("MovingMinuteTwo"),
                duration: 120,
                period: 5,
            },
            TimeUnit {
                name: String::from("MovingMinuteThree"),
                duration: 180,
                period: 4,
            },
            TimeUnit {
                name: String::from("MovingMinuteFour"),
                duration: 240,
                period: 3,
            },
            TimeUnit {
                name: String::from("MovingMinuteFive"),
                duration: 300,
                period: 2,
            },
        ]
    }

    pub fn find(name: &str) -> Option<TimeUnit> {
        TimeUnit::values().into_iter().find(|u| u.name == name)
    }
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct Protfolio {
    pub id: String,
    pub price: f32,
    pub time: i64,

    pub quote_type: QuoteType,
    pub market_hours: MarketHoursType,

    pub volume: i64,
    pub change: f32,
    pub change_rate: f32,

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
}
