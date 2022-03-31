use crate::{
    persist::grafana::add_order_annotation,
    vo::{
        biz::{MarketHoursType, Order, TradeInfo},
        core::{AppConfig, AssetContext},
    },
    Result,
};
use chrono::{TimeZone, Utc};
use log::{debug, info, warn};
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

        // TODO: ticker time check, drop if time difference too long

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

// 小於區間內最小值(扣除目前的上升區間)
// 區間內與最大值的價差（比率）
// 區間內的振蕩性
// 區間內的最大最小價差
// 與反向 eft 的利差（數值）

pub fn audit_trade(asset: Arc<AssetContext>, trade: &TradeInfo) -> bool {
    let rebounds = rebound_all(trade);
    // use m0060 as initial step
    if let Some(m0060) = rebounds.iter().find(|r| r.unit == "m0060") {
        // check
        // let min = DateTime::parse_from_rfc3339("2022-03-09T14:54:00.000Z")
        //     .unwrap()
        //     .timestamp_millis();
        // let max = DateTime::parse_from_rfc3339("2022-03-09T14:54:30.000Z")
        //     .unwrap()
        //     .timestamp_millis();

        // if trade.time > min && trade.time < max {
        //     print_meta(Arc::clone(&asset), trade, &rebounds);
        // }

        if matches!(m0060.trend, Trend::Upward) && m0060.up_count == 1 && m0060.down_count > 1 {
            if !validate_min_price(Arc::clone(&asset), trade) {
                return false;
            }

            if matches!(
                asset.find_running_order_test(&trade.id, trade.time),
                Some(_duplicated)
            ) {
                // duplicated order, do nothing
                return false;
            }

            if asset.add_order(Order::new(&trade.id, trade.price, 10, trade.time)) {
                print_meta(Arc::clone(&asset), trade, &rebounds);
                let symbol = trade.id.clone();
                let time = Utc.timestamp_millis(trade.time);
                let tags = vec![
                    trade.id.clone(),
                    format!("MSG-{}", &trade.message_id),
                    format!(
                        "1m {:?} ({}/{})",
                        m0060.trend, m0060.up_count, m0060.down_count
                    ),
                    trade.price.to_string(),
                ];
                add_order_annotation(symbol, time, "Place order".to_owned(), tags).unwrap();
            }
            // TODO: check others
            // check other trends
            // check max/min price in past sec/min/hour
        }
    } else {
        warn!("No 1m moving data found for trade: {:?}", trade);
    }

    false
}

// print details
fn print_meta(asset: Arc<AssetContext>, trade: &TradeInfo, rebounds: &Vec<SlopeTrend>) {
    info!(
        "################################### MSG-{} ###################################",
        &trade.message_id
    );

    let min_price_5m = find_min_price(Arc::clone(&asset), &trade.id, "m0060", 1, 6);
    let max_price_5m = find_max_price(Arc::clone(&asset), &trade.id, "m0060", 1, 6);
    let min_price_10m = find_min_price(Arc::clone(&asset), &trade.id, "m0060", 1, 11);
    let max_price_10m = find_max_price(Arc::clone(&asset), &trade.id, "m0060", 1, 11);

    info!(
        "5m min: {min} < {price} : {higher}, min diff: {min_diff} ({min_diff_rate:.2}%), max: {max}, min-max: {min_max_diff} ({min_max_diff_rate:.2}%)",
        min               = min_price_5m,
        price             = trade.price,
        higher            = min_price_5m < trade.price,
        min_diff          = trade.price - min_price_5m,
        min_diff_rate     = 100.0 * (trade.price - min_price_5m) / min_price_5m,
        max               = max_price_5m,
        min_max_diff      = max_price_5m - min_price_5m,
        min_max_diff_rate = 100.0 * (max_price_5m - min_price_5m) / max_price_5m,
    );

    info!(
        "10 min: {min} < {price} : {higher}, min diff: {min_diff} ({min_diff_rate:.2}%), max: {max}, min-max: {min_max_diff} ({min_max_diff_rate:.2}%)",
        min               = min_price_10m,
        price             = trade.price,
        higher            = min_price_10m < trade.price,
        min_diff          = trade.price - min_price_10m,
        min_diff_rate     = 100.0 * (trade.price - min_price_10m) / min_price_10m,
        max               = max_price_10m,
        min_max_diff      = max_price_10m - min_price_10m,
        min_max_diff_rate = 100.0 * (max_price_10m - min_price_10m) / max_price_10m,
    );

    for trend in rebounds {
        info!("{:?}", trend);
    }
    let protfolio_map = asset.symbol_protfolios(&trade.id).unwrap();
    for (unit, lock) in protfolio_map {
        let reader = lock.read().unwrap();
        info!(
            "*********************************** unit {} ***********************************",
            unit
        );
        for protfolios in reader.iter() {
            info!("unit: {}, {:?}", unit, protfolios);
        }
    }
}

fn validate_min_price(asset: Arc<AssetContext>, trade: &TradeInfo) -> bool {
    let min_price_05m = find_min_price(Arc::clone(&asset), &trade.id, "m0060", 1, 6);
    // let min_price_10m = find_min_price(Arc::clone(&asset), &trade.id, "m0060", 1, 11);
    // let min_price_30m = find_min_price(Arc::clone(&asset), &trade.id, "m0060", 1, 31);
    // let min_price_50m = find_min_price(Arc::clone(&asset), &trade.id, "m0060", 1, 51);

    min_price_05m.is_normal() && min_price_05m > trade.price * 0.999
}

fn find_min_price(
    asset: Arc<AssetContext>,
    symbol: &str,
    unit: &str,
    start: usize,
    end: usize,
) -> f32 {
    if let Some(lock) = asset.get_protfolios(symbol, unit) {
        let reader = lock.read().unwrap();
        if reader.is_empty() {
            f32::NAN
        } else {
            let min = reader
                .iter()
                .skip(start)
                .take(end - start)
                .map(|p| p.min_price)
                .reduce(f32::min)
                .unwrap();
            min
        }
    } else {
        f32::NAN
    }
}

fn find_max_price(
    asset: Arc<AssetContext>,
    symbol: &str,
    unit: &str,
    start: usize,
    end: usize,
) -> f32 {
    if let Some(lock) = asset.get_protfolios(symbol, unit) {
        let reader = lock.read().unwrap();
        if reader.is_empty() {
            f32::NAN
        } else {
            let max = reader
                .iter()
                .skip(start)
                .take(end - start)
                .map(|p| p.max_price)
                .reduce(f32::max)
                .unwrap();
            max
        }
    } else {
        f32::NAN
    }
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
