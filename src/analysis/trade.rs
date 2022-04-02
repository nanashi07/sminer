use crate::{
    persist::grafana::add_order_annotation,
    vo::{
        biz::{MarketHoursType, Order, TradeInfo},
        core::{AppConfig, AssetContext},
    },
    Result,
};
use chrono::{DateTime, TimeZone, Utc};
use log::{debug, warn};
use std::{f64::NAN, fs::OpenOptions, io::BufWriter, io::Write, path::Path, sync::Arc};

const PRICE_DEVIATION_RATE_TO_MIN: f32 = (100.0 - 0.3) / 100.0; // 0.3%
const PRICE_OSCILLATION_RANGE: f32 = 0.015; // 1.5%

pub fn prepare_trade(
    asset: Arc<AssetContext>,
    config: Arc<AppConfig>,
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
        let state = audit_trade(Arc::clone(&asset), Arc::clone(&config), &trade);
        match state {
            AuditState::Flash | AuditState::Slug => {
                let order = Order::new(&trade.id, trade.price, 10, trade.action_time());
                if asset.add_order(order.clone()) {
                    let order_id = order.id.clone();

                    print_meta(
                        Arc::clone(&asset),
                        Arc::clone(&config),
                        Some(order.clone()),
                        &trade,
                    )
                    .unwrap_or_default();

                    let symbol = trade.id.clone();
                    let time = Utc.timestamp_millis(trade.action_time());
                    let tags = vec![
                        trade.id.clone(),
                        order_id,
                        format!("{:?}", state),
                        format!("MSG-{}", &trade.message_id),
                        trade.price.to_string(),
                    ];

                    // write off previouse order
                    asset.write_off_order(&order);

                    // add grafana annotation
                    add_order_annotation(symbol, time, "Place order".to_owned(), tags).unwrap();
                }
            }
            AuditState::Decline => {}
        }
    } else {
        warn!("No trade info for message ID: {} found!", &message_id);
    }

    Ok(())
}

pub fn audit_trade(
    asset: Arc<AssetContext>,
    config: Arc<AppConfig>,
    trade: &TradeInfo,
) -> AuditState {
    // FIXME: period check
    if false {
        let min = DateTime::parse_from_rfc3339("2022-03-09T14:54:00.000Z")
            .unwrap()
            .timestamp_millis();
        let max = DateTime::parse_from_rfc3339("2022-03-09T14:54:30.000Z")
            .unwrap()
            .timestamp_millis();

        if trade.time > min && trade.time < max {
            print_meta(Arc::clone(&asset), Arc::clone(&config), None, trade).unwrap_or_default();
        }
    }

    let mut result = AuditState::Decline;

    // flash check
    if flash::audit(Arc::clone(&asset), Arc::clone(&config), trade) {
        result = AuditState::Flash;
    }

    // slug check
    if slug::audit(Arc::clone(&asset), Arc::clone(&config), trade) {
        result = AuditState::Slug;
    }

    // TODO: reutrn if decline, unnecessary to check following

    // FIXME: check previous order status
    if matches!(
        // asset.find_running_order_test(&trade.id, trade.action_time()),
        asset.find_running_order(&trade.id),
        Some(_duplicated)
    ) {
        // duplicated order, do nothing
        return AuditState::Decline;
    }

    // 小於區間內最小值(扣除目前的上升區間)
    // 區間內與最大值的價差（比率）
    // 區間內的振蕩性
    // 區間內的最大最小價差
    // 與反向 eft 的利差（數值）

    // TODO: check others
    // check other trends
    // check exists order (same side)
    // check profit to previous order
    // forecast next possible profit, after 5m place order
    // forecast possible lost

    result
}

// print details
fn print_meta(
    asset: Arc<AssetContext>,
    config: Arc<AppConfig>,
    order: Option<Order>,
    trade: &TradeInfo,
) -> Result<()> {
    let mut buffered: Vec<String> = Vec::new();

    buffered.push(format!(
        "################################### MSG-{} ###################################",
        &trade.message_id
    ));

    buffered.push(format!(
        "PRICE_DEVIATION_RATE_TO_MIN: {}",
        PRICE_DEVIATION_RATE_TO_MIN
    ));
    buffered.push(format!(
        "PRICE_OSCILLATION_RANGE: {}",
        PRICE_OSCILLATION_RANGE
    ));

    buffered.push(format!(
        "------------------------------------------------------------------------"
    ));

    if let Some(value) = order {
        buffered.push(format!("{:?}", value));
        buffered.push(format!(
            "----------------------------------- {} -----------------------------------",
            &value.id
        ));
    }

    let price_check_ranges = [
        (1, 6),
        (6, 11),
        (11, 16),
        (16, 21),
        (21, 26),
        (26, 31),
        (31, 36),
        (36, 41),
        (41, 46),
        (46, 51),
        (51, 56),
        (56, 61),
        (1, 11),
        (11, 21),
        (21, 31),
        (31, 41),
        (41, 51),
        (51, 61),
        (1, 16),
        (16, 31),
        (31, 46),
        (46, 61),
        (1, 31),
        (31, 61),
    ];

    for (start, end) in price_check_ranges {
        let min_price = find_min_price(Arc::clone(&asset), &trade.id, "m0060", start, end);
        let max_price = find_max_price(Arc::clone(&asset), &trade.id, "m0060", start, end);

        buffered.push(format!(
            "{start:02}-{end:02}m price: {price:.4}, min: {min:.4}, min diff: {min_diff:.4} ({min_diff_rate:.3}%), max: {max:.4}, min-max: {min_max_diff:.4} ({min_max_diff_rate:.3}%)",
            start             = start,
            end               = end,
            min               = min_price,
            price             = trade.price,
            min_diff          = trade.price - min_price,
            min_diff_rate     = 100.0 * (trade.price - min_price) / min_price,
            max               = max_price,
            min_max_diff      = max_price - min_price,
            min_max_diff_rate = 100.0 * (max_price - min_price) / max_price,
        ));
    }

    buffered.push(format!(
        "------------------------------------------------------------------------"
    ));

    let rebounds = rebound_all(trade);
    for trend in rebounds {
        buffered.push(format!("{:?}", trend));
    }

    // let protfolio_map = asset.symbol_protfolios(&trade.id).unwrap();
    // for (unit, lock) in protfolio_map {
    //     let reader = lock.read().unwrap();
    //     buffered.push(format!(
    //         "*********************************** unit {} ***********************************",
    //         unit
    //     ));
    //     for protfolios in reader.iter() {
    //         buffered.push(format!("unit: {}, {:?}", unit, protfolios));
    //     }
    // }

    let path = format!(
        "{base}/orders/{symbol}/{day}/MSG-{id}.ord",
        base = &config.replay.output.base_folder,
        symbol = &trade.id,
        day = Utc.timestamp_millis(trade.time).format("%Y-%m-%d"),
        id = &trade.message_id
    );
    let parent = Path::new(&path).parent().unwrap();
    if !parent.exists() {
        std::fs::create_dir_all(parent.to_str().unwrap())?;
    }

    let output = OpenOptions::new()
        .write(true)
        .create(true)
        .truncate(true)
        .open(&path)?;
    let mut writer = BufWriter::new(output);

    for line in buffered {
        write!(&mut writer, "{}\n", &line)?;
    }

    Ok(())
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
                .unwrap_or(f32::NAN);
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
                .unwrap_or(f32::NAN);
            max
        }
    } else {
        f32::NAN
    }
}

pub fn rebound_all(trade: &TradeInfo) -> Vec<TradeTrend> {
    trade
        .states
        .iter()
        .map(|(key, values)| rebound_at(&key, &values))
        .collect::<Vec<TradeTrend>>()
}

pub fn rebound_at(unit: &str, slopes: &Vec<f64>) -> TradeTrend {
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

    TradeTrend {
        unit: unit.to_string(),
        trend,
        rebound_at,
        up_count,
        down_count,
    }
}

mod flash {

    use super::{find_max_price, find_min_price, rebound_all, Trend};
    use crate::vo::{
        biz::TradeInfo,
        core::{AppConfig, AssetContext},
    };
    use std::sync::Arc;

    pub fn audit(asset: Arc<AssetContext>, config: Arc<AppConfig>, trade: &TradeInfo) -> bool {
        // check oscillation first, should be greater than `base rate`
        if !validate_oscillation(Arc::clone(&asset), Arc::clone(&config), trade) {
            return false;
        }

        // check trend
        if !validate_trend(Arc::clone(&asset), Arc::clone(&config), trade) {
            return false;
        }

        // check min price difference
        if !validate_min_price(Arc::clone(&asset), Arc::clone(&config), trade) {
            return false;
        }

        true
    }

    fn validate_trend(
        _asset: Arc<AssetContext>,
        _config: Arc<AppConfig>,
        trade: &TradeInfo,
    ) -> bool {
        let mut result = true;
        let rebounds = rebound_all(trade);

        // use 10s as initial step
        result = result
            && if let Some(m0010) = rebounds.iter().find(|r| r.unit == "m0010") {
                // check 10s trend, should be upward and with multiple previous downwards
                if !(matches!(m0010.trend, Trend::Upward)
                    && m0010.up_count == 1
                    && m0010.down_count > 1)
                {
                    true
                } else {
                    false
                }
            } else {
                false
            };

        // check 30s trend, should be downward
        result = result
            && if let Some(m0030) = rebounds.iter().find(|r| r.unit == "m0030") {
                if matches!(m0030.trend, Trend::Downward) {
                    true
                } else {
                    false
                }
            } else {
                false
            };

        // check 60s trend, should be downward
        result = result
            && if let Some(m0060) = rebounds.iter().find(|r| r.unit == "m0060") {
                if matches!(m0060.trend, Trend::Downward) {
                    true
                } else {
                    false
                }
            } else {
                false
            };

        result
    }

    fn validate_min_price(
        asset: Arc<AssetContext>,
        config: Arc<AppConfig>,
        trade: &TradeInfo,
    ) -> bool {
        let deviation_rate_to_min = config.trade.flash.min_deviation_rate;

        // 70s min price
        let min_price = find_min_price(Arc::clone(&asset), &trade.id, "m0010", 0, 7);

        // assume trade price is higher than min_price
        min_price.is_normal() && (trade.price - min_price) / min_price < deviation_rate_to_min
    }

    fn validate_oscillation(
        asset: Arc<AssetContext>,
        config: Arc<AppConfig>,
        trade: &TradeInfo,
    ) -> bool {
        let mut result = true;

        // 70s oscillation (min to max)
        let oscillation = *config.trade.flash.oscillation_rage.get("m0070").unwrap();

        // 當下振幅, 0s - 70s
        let min_price = find_min_price(Arc::clone(&asset), &trade.id, "m0010", 0, 7);
        let max_price = find_max_price(Arc::clone(&asset), &trade.id, "m0010", 0, 7);

        if (max_price - min_price) / max_price < oscillation {
            result = false;
        }

        result
    }
}

mod slug {
    use super::{find_max_price, find_min_price, rebound_all, Trend};
    use crate::vo::{
        biz::TradeInfo,
        core::{AppConfig, AssetContext},
    };
    use std::sync::Arc;

    pub fn audit(asset: Arc<AssetContext>, config: Arc<AppConfig>, trade: &TradeInfo) -> bool {
        // check trend
        if !validate_trend(Arc::clone(&asset), Arc::clone(&config), trade) {
            return false;
        }

        // check min price difference
        if !validate_min_price(Arc::clone(&asset), Arc::clone(&config), trade) {
            return false;
        }

        // check oscillation first, should be greater than `base rate`
        if !validate_oscillation(Arc::clone(&asset), Arc::clone(&config), trade) {
            return false;
        }

        true
    }

    fn validate_trend(
        _asset: Arc<AssetContext>,
        _config: Arc<AppConfig>,
        trade: &TradeInfo,
    ) -> bool {
        let mut result = true;
        let rebounds = rebound_all(trade);

        // use 1m as initial step
        result = result
            && if let Some(m0060) = rebounds.iter().find(|r| r.unit == "m0060") {
                // check 10s trend, should be upward and with multiple previous downwards
                if !(matches!(m0060.trend, Trend::Upward)
                    && m0060.up_count == 1
                    && m0060.down_count > 1)
                {
                    true
                } else {
                    false
                }
            } else {
                false
            };

        // check 30s trend, should be upward
        result = result
            && if let Some(m0030) = rebounds.iter().find(|r| r.unit == "m0030") {
                if matches!(m0030.trend, Trend::Upward) {
                    true
                } else {
                    false
                }
            } else {
                false
            };

        // check 60s trend, should be upward
        result = result
            && if let Some(m0010) = rebounds.iter().find(|r| r.unit == "m0010") {
                if matches!(m0010.trend, Trend::Upward) {
                    true
                } else {
                    false
                }
            } else {
                false
            };

        result
    }

    fn validate_min_price(
        asset: Arc<AssetContext>,
        config: Arc<AppConfig>,
        trade: &TradeInfo,
    ) -> bool {
        let deviation_rate_to_min = config.trade.slug.min_deviation_rate;

        // 10m min price
        let min_price = find_min_price(Arc::clone(&asset), &trade.id, "m0060", 0, 5);

        // assume trade price is higher than min_price
        min_price.is_normal() && (trade.price - min_price) / min_price < deviation_rate_to_min
    }

    fn validate_oscillation(
        asset: Arc<AssetContext>,
        config: Arc<AppConfig>,
        trade: &TradeInfo,
    ) -> bool {
        let mut result = true;

        // 70s oscillation (min to max)
        let oscillation = *config.trade.slug.oscillation_rage.get("m0300").unwrap();

        let min_price_05 = find_min_price(Arc::clone(&asset), &trade.id, "m0060", 0, 5);
        let max_price_05 = find_max_price(Arc::clone(&asset), &trade.id, "m0060", 0, 5);

        let min_price_10 = find_min_price(Arc::clone(&asset), &trade.id, "m0060", 5, 10);
        let max_price_10 = find_max_price(Arc::clone(&asset), &trade.id, "m0060", 5, 10);

        let min_price_15 = find_min_price(Arc::clone(&asset), &trade.id, "m0060", 10, 15);
        let max_price_15 = find_max_price(Arc::clone(&asset), &trade.id, "m0060", 10, 15);

        let min_price_20 = find_min_price(Arc::clone(&asset), &trade.id, "m0060", 15, 20);
        let max_price_20 = find_max_price(Arc::clone(&asset), &trade.id, "m0060", 15, 20);

        // TODO: magic number
        if max_price_05.is_normal()
            && min_price_05.is_normal()
            && (max_price_05 - min_price_05) / max_price_05 < oscillation
        {
            result = false;
        }

        result
    }
}

#[derive(Debug, Clone)]
pub enum Trend {
    Upward,
    Downward,
}

#[derive(Debug, Clone)]
pub struct TradeTrend {
    pub unit: String,
    pub trend: Trend,
    pub rebound_at: i32,
    pub up_count: i32,
    pub down_count: i32,
}

#[derive(Debug, Clone)]
pub enum AuditState {
    Flash,
    Slug,
    Decline,
}
