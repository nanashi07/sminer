use crate::{
    persist::grafana::add_annotation,
    vo::{
        biz::{MarketHoursType, Order, TradeInfo},
        core::{AppConfig, AssetContext},
    },
    Result,
};
use chrono::{TimeZone, Utc};
use log::{debug, warn};
use std::{f64::NAN, sync::Arc};

pub fn prepare_trade(
    asset: Arc<AssetContext>,
    _config: Arc<AppConfig>,
    message_id: i64,
) -> Result<()> {
    if let Some(lock) = asset.search_trade(message_id) {
        let trade = lock.read().unwrap();

        // only accept regular market
        if !matches!(trade.market_hours, MarketHoursType::RegularMarket) {
            return Ok(());
        }

        // TODO: ticker time check, drop if time difference too large

        debug!("Trade info: {:?}", &trade);
        // audit trade
        if audit_trade(Arc::clone(&asset), &trade) {
            // debug!("");
            // check exists order (same side)
            // check profit to previous order
            // forecast next possible profit, after 5m place order
            // forecast possible lost
        }
    } else {
        warn!("No trade info for message ID: {} found!", &message_id);
    }

    Ok(())
}

pub fn audit_trade(asset: Arc<AssetContext>, trade: &TradeInfo) -> bool {
    let rebounds = rebound_all(trade);
    // use m1m as initial step
    if let Some(m1m) = rebounds.iter().find(|r| r.unit == "m1m") {
        if matches!(m1m.trend, Trend::Upward) && m1m.up_count == 1 && m1m.down_count > 1 {
            if let Some(protfolios_5m) = asset.get_protfolios(&trade.id, "m5m") {
                let reader_5m = protfolios_5m.read().unwrap();
                let min_price_5m = reader_5m.front().unwrap().min_price;

                if min_price_5m > trade.price {
                    if let Some(_order) = asset.find_running_order_test(&trade.id, trade.time) {
                        // no
                        // log::info!("----------------- {:?}", &_order);
                    } else {
                        // log::info!(
                        //     "symbold: {}, time: {}, price: {}, m1m: {:?}",
                        //     &trade.id,
                        //     Utc.timestamp_millis(trade.time),
                        //     trade.price,
                        //     m1m
                        // );
                        if asset.add_order(Order::new(&trade.id, trade.price, 10, trade.time)) {
                            // add_annotation(trade.time, "Place TQQQ order", &vec!["TQQQ"], 1, 2)
                            //     .await?;
                        }
                    }
                    // TODO: check others
                    // check other trends
                    // check max/min price in past sec/min/hour
                }
            }
            // if let Some(protfolios_10m) = asset.get_protfolios(&trade.id, "m10m") {
            //     let reader_10m = protfolios_10m.read().unwrap();
            //     let min_price_10m = reader_10m.front().unwrap().min_price;

            //     if min_price_10m > trade.price {
            //         log::info!(
            //             "symbold: {}, time: {}, price: {}, m1m: {:?}",
            //             &trade.id,
            //             Utc.timestamp_millis(trade.time),
            //             trade.price,
            //             m1m
            //         );
            //         // TODO: check others
            //         // check other trends
            //         // check max/min price in past sec/min/hour
            //     }
            // }
        }
    } else {
        warn!("No m1m found for trade: {:?}", trade);
    }

    false
}

#[derive(Debug, Clone)]
pub enum Trend {
    Upward,
    Downward,
}

#[derive(Debug, Clone)]
pub struct SlopeTrend {
    pub unit: String,
    pub trend: Trend,
    pub rebound_at: i32,
    pub up_count: i32,
    pub down_count: i32,
}

pub fn rebound_all(trade: &TradeInfo) -> Vec<SlopeTrend> {
    trade
        .states
        .iter()
        .map(|(key, values)| rebound_at(&key, &values))
        .collect::<Vec<SlopeTrend>>()
}

pub fn rebound_at(unit: &str, slopes: &Vec<f64>) -> SlopeTrend {
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
        unit: unit.to_string(),
        trend,
        rebound_at,
        up_count,
        down_count,
    }
}
