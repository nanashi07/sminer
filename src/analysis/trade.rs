use crate::{
    vo::{
        biz::TradeInfo,
        core::{AppConfig, AssetContext},
    },
    Result,
};
use log::{debug, warn};
use std::{f64::NAN, sync::Arc};

pub fn prepare_trade(
    asset: Arc<AssetContext>,
    _config: Arc<AppConfig>,
    message_id: i64,
) -> Result<()> {
    if let Some(lock) = asset.search_trade(message_id) {
        let trade = lock.read().unwrap();

        debug!("Trade info: {:?}", &trade);
        // audit trade
        if audit_trade(&trade) {
            debug!("");
            // check profit to previous order
            // forecast next possible profit, after 5m place order
            // forecast possible lost
        }
    } else {
        warn!("No trade info for message ID: {} found!", &message_id);
    }
    Ok(())
}

pub fn audit_trade(trade: &TradeInfo) -> bool {
    // use m1m as initial step
    let m1m = trade.states.get("m1m").unwrap();
    let rebount_1m = rebound_at(m1m);

    if matches!(rebount_1m.trend, Trend::Upward) && rebount_1m.up_count == 1 {
        // TODO: check others
        // check other trends
        // check max/min price in past sec/min/hour
    }

    false
}

fn isiw() -> bool {
    false
}

#[derive(Debug, Clone)]
pub enum Trend {
    Upward,
    Downward,
}

#[derive(Debug, Clone)]
pub struct SlopeTrend {
    pub trend: Trend,
    pub rebound_at: i32,
    pub up_count: i32,
    pub down_count: i32,
}

pub fn rebound_at(slopes: &Vec<f64>) -> SlopeTrend {
    let mut trend = Trend::Upward;
    let mut rebound_at = -1;
    let mut up_count = 0;
    let mut down_count = 0;

    let mut last_slope = NAN;

    for (index, value) in slopes.iter().enumerate() {
        let slope = *value;
        match index {
            0 => {
                if slope < 0.0 {
                    trend = Trend::Downward;
                    break;
                }
                up_count += 1;
            }
            _ => {
                if rebound_at > -1 && last_slope < 0.0 && slope >= 0.0 {
                    // 此區間向上，前一區間向下，且反轉點已設定，代表二次反轉
                    break;
                }
                if last_slope >= 0.0 && slope < 0.0 {
                    // 此區間向下，前一區間向上，即為反轉點
                    rebound_at = index as i32;
                }

                if slope >= 0.0 {
                    up_count += 1;
                } else {
                    down_count += 1;
                }
            }
        }

        last_slope = slope;
    }

    SlopeTrend {
        trend,
        rebound_at,
        up_count,
        down_count,
    }
}
