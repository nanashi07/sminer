use crate::{
    persist::grafana::add_annotation,
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
        let panel_map: std::collections::HashMap<&str, i64> = [
            ("TQQQ", 2),
            ("SQQQ", 5),
            ("SOXL", 3),
            ("SOXS", 4),
            ("SPXL", 6),
            ("SPXS", 7),
            ("LABU", 9),
            ("LABD", 8),
            ("TNA", 10),
            ("TZA", 11),
            ("YINN", 14),
            ("YANG", 15),
            ("UDOW", 12),
            ("SDOW", 13),
        ]
        .iter()
        .cloned()
        .collect();

        if matches!(m0060.trend, Trend::Upward) && m0060.up_count == 1 && m0060.down_count > 1 {
            // check min price in 5m
            let min_price_5m = find_min_price(Arc::clone(&asset), &trade.id, "m0060", 1, 6);
            let max_price_5m = find_max_price(Arc::clone(&asset), &trade.id, "m0060", 1, 6);

            if min_price_5m.is_normal() && min_price_5m > trade.price {
                if let Some(_duplicated) = asset.find_running_order_test(&trade.id, trade.time) {
                    // duplicated order, do nothing
                } else {
                    if asset.add_order(Order::new(&trade.id, trade.price, 10, trade.time)) {
                        // print details
                        info!("################################### trade {} ###################################", &trade.message_id);
                        for trend in &rebounds {
                            info!("{:?}", trend);
                        }
                        let protfolio_map = asset.symbol_protfolios(&trade.id).unwrap();
                        for (unit, lock) in protfolio_map {
                            let reader = lock.read().unwrap();
                            info!("*********************************** unit {} ***********************************", unit);
                            for protfolios in reader.iter() {
                                info!("unit: {}, {:?}", unit, protfolios);
                            }
                        }

                        let time = Utc.timestamp_millis(trade.time);
                        let tags = vec![
                            trade.id.clone(),
                            format!("MSG-{}", &trade.message_id),
                            format!(
                                "1m {:?} ({}/{})",
                                m0060.trend, m0060.up_count, m0060.down_count
                            ),
                            format!("1-6m min: {}", min_price_5m),
                            format!("1-6m max: {}", max_price_5m),
                            trade.price.to_string(),
                        ];
                        let panel_id = *panel_map.get(trade.id.as_str()).unwrap();
                        let rt = tokio::runtime::Builder::new_current_thread()
                            .enable_time()
                            .enable_io()
                            .build()
                            .unwrap();
                        rt.block_on(async {
                            add_annotation(&time, "Place order", &tags, 1, panel_id)
                                .await
                                .unwrap();
                        });
                    }
                }
                // TODO: check others
                // check other trends
                // check max/min price in past sec/min/hour
            }
        }
    } else {
        warn!("No 1m moving data found for trade: {:?}", trade);
    }

    false
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
