use crate::{
    persist::grafana::add_order_annotation,
    vo::{
        biz::{AuditState, MarketHoursType, Order, TradeInfo},
        core::{
            AppConfig, AssetContext, KEY_EXTRA_PRINT_TRADE_META_END_TIME,
            KEY_EXTRA_PRINT_TRADE_META_START_TIME,
        },
    },
    Result,
};
use chrono::{TimeZone, Utc};
use log::*;
use std::{
    collections::{HashMap, HashSet},
    f64::NAN,
    fs::OpenOptions,
    io::BufWriter,
    io::Write,
    path::Path,
    sync::Arc,
};

pub fn profit_evaluate(asset: Arc<AssetContext>, _config: Arc<AppConfig>) -> Result<bool> {
    // find all orders
    let lock = asset.orders();
    let readers = lock.read().unwrap();
    let symbols: HashSet<String> = readers.iter().map(|o| o.symbol.to_string()).collect();

    // check all regular market closed
    for symbol in &symbols {
        if let Some(market) = asset.get_current_market(&symbol) {
            match market {
                MarketHoursType::PostMarket => {}
                _ => return Ok(false),
            }
        }
    }

    let close_prices: HashMap<String, f32> = symbols
        .iter()
        .map(|symbol| asset.get_first_post_ticker(symbol).unwrap())
        .map(|ticker| (ticker.id.clone(), ticker.price))
        .collect();

    info!("####################################################################################################");
    info!("####################################################################################################");

    // estimate profit
    let mut total_amount = 0.0;
    let mut total_profit = 0.0;
    let lock = asset.orders();
    let readers = lock.read().unwrap();
    for order in readers.iter().rev() {
        let post_market_price = *close_prices.get(&order.symbol).unwrap();
        // FIXME: use accepted
        let profit = (post_market_price - order.created_price) * order.created_volume as f32;
        info!("profit: {} for {:?}", profit, order);
        // FIXME: use accepted
        total_amount += order.created_price * order.created_volume as f32;
        total_profit += profit;
    }
    info!(
        "closed prices {:?}, order count: {}, total profit: {}, total amount: {}",
        close_prices,
        readers.len(),
        total_profit,
        total_amount
    );

    info!("####################################################################################################");
    info!("####################################################################################################");

    Ok(true) //FIXME:
}

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
                // calculate volume
                let suspected_volume =
                    calculate_volum(Arc::clone(&asset), Arc::clone(&config), &trade);
                // create order
                let order = Order::new(
                    &trade.id,
                    trade.price,
                    suspected_volume,
                    trade.action_time(),
                    state.clone(),
                );
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
                        format!("{:?}", &state),
                        format!("MSG-{}", &trade.message_id),
                        trade.price.to_string(),
                    ];

                    // write off previouse order
                    asset.write_off(&order);

                    // add grafana annotation
                    add_order_annotation(symbol, time, "Place order".to_owned(), tags).unwrap();
                }
            }
            AuditState::Loss => {
                // get latest rival ticker
                let symbol = &trade.id;
                let rival_symbol = asset.find_pair_symbol(symbol).unwrap();
                let time = trade.time;

                // replace with rival latest trade
                let mut trade = asset.get_latest_trade(&rival_symbol).unwrap();
                trade.time = time + 1;

                // calculate volume
                let suspected_volume =
                    calculate_volum(Arc::clone(&asset), Arc::clone(&config), &trade);
                // create order
                let order = Order::new(
                    &trade.id,
                    trade.price,
                    suspected_volume,
                    trade.action_time(),
                    state.clone(),
                );
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
                        format!("{:?}", &state),
                        format!("MSG-{}", &trade.message_id),
                        trade.price.to_string(),
                    ];

                    // take loss previouse order
                    asset.realized_loss(&order);

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

pub fn calculate_volum(asset: Arc<AssetContext>, config: Arc<AppConfig>, trade: &TradeInfo) -> u32 {
    // restricted amount
    let max_amount = config.trade.max_order_amount;
    // get opposite
    let rival_symbol = asset.find_pair_symbol(&trade.id).unwrap();
    if let Some(rival_order) = asset.find_running_order(&rival_symbol) {
        // FIXME: calculation
        let total = (rival_order.created_volume as f32) * rival_order.created_price;
        let suspect_volumn = total / trade.price;
        suspect_volumn.round() as u32
    } else {
        let suspect_volumn = (max_amount as f32) / trade.price;
        suspect_volumn.round() as u32
    }
}

pub fn audit_trade(
    asset: Arc<AssetContext>,
    config: Arc<AppConfig>,
    trade: &TradeInfo,
) -> AuditState {
    // print period meta
    if config.extra_present(KEY_EXTRA_PRINT_TRADE_META_START_TIME)
        && config.extra_present(KEY_EXTRA_PRINT_TRADE_META_END_TIME)
    {
        let start_at = config
            .extra_get(KEY_EXTRA_PRINT_TRADE_META_START_TIME)
            .unwrap()
            .parse::<i64>()
            .unwrap();
        let end_at = config
            .extra_get(KEY_EXTRA_PRINT_TRADE_META_END_TIME)
            .unwrap()
            .parse::<i64>()
            .unwrap();

        if trade.time > start_at && trade.time < end_at {
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
    if let Some(exists_order) = asset.find_running_order(&trade.id) {
        // exists order, check PnL
        if loss_recognition(asset, config, trade, &exists_order) {
            return AuditState::Loss;
        }
        result = AuditState::Decline;
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

fn loss_recognition(
    _asset: Arc<AssetContext>,
    config: Arc<AppConfig>,
    trade: &TradeInfo,
    order: &Order,
) -> bool {
    let margin_rate = match order.audit {
        AuditState::Flash => config.trade.flash.loss_margin_rate,
        AuditState::Slug => config.trade.slug.loss_margin_rate,
        _ => 100.0,
    };
    let price = trade.price;
    // FIXME : use accepted price
    let order_price = order.created_price;
    if price < order_price && (order_price - price) / order_price > margin_rate {
        true
    } else {
        false
    }
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
        "################################### MSG-{} @ {} ###################################",
        &trade.message_id,
        Utc.timestamp_millis(trade.time).format("%Y-%m-%d %H:%M:%S")
    ));

    buffered.push(format!(
        "[Config] flash.loss_margin_rate: {:?}",
        &config.trade.flash.loss_margin_rate
    ));
    buffered.push(format!(
        "[Config] flash.min_deviation_rate: {:?}",
        &config.trade.flash.min_deviation_rate
    ));
    for (key, value) in &config.trade.flash.oscillation_rage {
        buffered.push(format!(
            "[Config] flash.oscillation_rage: {:?} = {:?}",
            key, value
        ));
    }

    buffered.push(format!(
        "[Config] slug.loss_margin_rate: {:?}",
        &config.trade.slug.loss_margin_rate
    ));
    buffered.push(format!(
        "[Config] slug.min_deviation_rate: {:?}",
        &config.trade.slug.min_deviation_rate
    ));
    for (key, value) in &config.trade.slug.oscillation_rage {
        buffered.push(format!(
            "[Config] slug.oscillation_rage: {:?} = {:?}",
            key, value
        ));
    }

    buffered.push(format!(
        "----------------------------------flash--------------------------------------"
    ));

    for name in config.get_trade_deviation_keys("flash") {
        let deviation_rate_to_min = config.get_trade_deviation("flash", &name).unwrap();

        // parse period from key (ex: m0070 => 70 / 10 = 7)
        let period = name[1..].parse::<usize>().unwrap() / 10;

        // min price
        let min_price = find_min_price(Arc::clone(&asset), &trade.id, "m0010", 0, period);

        // assume trade price is higher than min_price
        buffered.push(format!(
            "flash min price, period: {}, price: {}, min price{}, value {} < eviation {} = {}",
            period,
            trade.price,
            min_price,
            (trade.price - min_price) / min_price,
            deviation_rate_to_min,
            !min_price.is_normal() || (trade.price - min_price) / min_price > deviation_rate_to_min
        ));
    }
    for name in config.get_trade_oscillation_keys("flash") {
        let oscillation = config.get_trade_oscillation("flash", &name).unwrap();

        // parse period from key (ex: m0070 => 70 / 10 = 7)
        let period = name[1..].parse::<usize>().unwrap() / 60;

        // min price
        let min_price = find_min_price(Arc::clone(&asset), &trade.id, "m0010", 0, period);
        let max_price = find_max_price(Arc::clone(&asset), &trade.id, "m0010", 0, period);

        // assume trade price is higher than min_price
        buffered.push(format!(
            "flash oscillation, period: {}, max price: {}, min price{}, rate {} < oscillation {} = {}",
            period,
            max_price,
            min_price,
            (max_price - min_price) / max_price,
            oscillation,
            !max_price.is_normal() || !min_price.is_normal() || (max_price - min_price) / max_price < oscillation
        ));
    }

    buffered.push(format!(
        "---------------------------------slug---------------------------------------"
    ));

    for name in config.get_trade_deviation_keys("slug") {
        let deviation_rate_to_min = config.get_trade_deviation("slug", &name).unwrap();

        // parse period from key (ex: m0300 => 300 / 60 = 5 )
        let period = name[1..].parse::<usize>().unwrap() / 60;

        // min price
        let min_price = find_min_price(Arc::clone(&asset), &trade.id, "m0060", 0, period);

        // assume trade price is higher than min_price
        buffered.push(format!(
            "slug min price, period: {}, price: {}, min price{}, value {} < eviation {} = {}",
            period,
            trade.price,
            min_price,
            (trade.price - min_price) / min_price,
            deviation_rate_to_min,
            !min_price.is_normal() || (trade.price - min_price) / min_price > deviation_rate_to_min
        ));
    }
    for name in config.get_trade_oscillation_keys("slug") {
        let oscillation = config.get_trade_oscillation("slug", &name).unwrap();

        // parse period from key (ex: m0300 => 300 / 60 = 5 )
        let period = name[1..].parse::<usize>().unwrap() / 60;

        // min price
        let min_price = find_min_price(Arc::clone(&asset), &trade.id, "m0060", 0, period);
        let max_price = find_max_price(Arc::clone(&asset), &trade.id, "m0060", 0, period);

        // assume trade price is higher than min_price
        buffered.push(format!(
            "slug oscillation, period: {}, max price: {}, min price{}, rate {} < oscillation {} = {}",
            period,
            max_price,
            min_price,
            (max_price - min_price) / max_price, oscillation,
            !max_price.is_normal() || !min_price.is_normal() || (max_price - min_price) / max_price < oscillation
        ));
    }

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
    use chrono::Duration;
    use log::*;
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

        // check last order to prevent place mutiple orders (watch within 30s)
        if let Some(order) = asset.find_last_flash_order(&trade.id) {
            if trade.action_time() - order.created_time < Duration::seconds(30).num_milliseconds() {
                debug!("Found flash order within 30s, ignore {:?}", trade);
                return false;
            }
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
                matches!(m0010.trend, Trend::Upward) && m0010.up_count == 1 && m0010.down_count > 1
            } else {
                false
            };

        // check 30s trend, should be downward
        result = result
            && if let Some(m0030) = rebounds.iter().find(|r| r.unit == "m0030") {
                matches!(m0030.trend, Trend::Downward)
            } else {
                false
            };

        // check 60s trend, should be downward
        result = result
            && if let Some(m0060) = rebounds.iter().find(|r| r.unit == "m0060") {
                matches!(m0060.trend, Trend::Downward)
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
        for name in config.get_trade_deviation_keys("flash") {
            let deviation_rate_to_min = config.get_trade_deviation("flash", &name).unwrap();

            // parse period from key (ex: m0070 => 70 / 10 = 7)
            let period = name[1..].parse::<usize>().unwrap() / 10;

            // min price
            let min_price = find_min_price(Arc::clone(&asset), &trade.id, "m0010", 0, period);

            // assume trade price is higher than min_price
            if !min_price.is_normal()
                || (trade.price - min_price) / min_price > deviation_rate_to_min
            {
                debug!(
                    "validate flash min price failed, period: {}, price: {}, min price{}, value {} < eviation {}",
                    period, trade.price, min_price, (trade.price - min_price) / min_price, deviation_rate_to_min
                );
                return false;
            }
        }

        true
    }

    fn validate_oscillation(
        asset: Arc<AssetContext>,
        config: Arc<AppConfig>,
        trade: &TradeInfo,
    ) -> bool {
        for name in config.get_trade_oscillation_keys("flash") {
            let oscillation = config.get_trade_oscillation("flash", &name).unwrap();

            // parse period from key (ex: m0070 => 70 / 10 = 7)
            let period = name[1..].parse::<usize>().unwrap() / 60;

            // min price
            let min_price = find_min_price(Arc::clone(&asset), &trade.id, "m0010", 0, period);
            let max_price = find_max_price(Arc::clone(&asset), &trade.id, "m0010", 0, period);

            // assume trade price is higher than min_price
            if !max_price.is_normal()
                || !min_price.is_normal()
                || (max_price - min_price) / max_price < oscillation
            {
                debug!(
                    "validate flash oscillation failed, period: {}, max price: {}, min price{}, rate {} < oscillation {}",
                    period, max_price, min_price, (max_price - min_price) / max_price, oscillation
                );
                return false;
            }
        }

        true
    }
}

mod slug {
    use log::debug;

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
                // check 60s trend, should be upward and with multiple previous downwards
                matches!(m0060.trend, Trend::Upward) && m0060.up_count == 1 && m0060.down_count > 1
            } else {
                false
            };

        // check 30s trend, should be upward
        result = result
            && if let Some(m0030) = rebounds.iter().find(|r| r.unit == "m0030") {
                matches!(m0030.trend, Trend::Upward)
            } else {
                false
            };

        // check 60s trend, should be upward
        result = result
            && if let Some(m0010) = rebounds.iter().find(|r| r.unit == "m0010") {
                matches!(m0010.trend, Trend::Upward)
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
        for name in config.get_trade_deviation_keys("slug") {
            let deviation_rate_to_min = config.get_trade_deviation("slug", &name).unwrap();

            // parse period from key (ex: m0300 => 300 / 60 = 5 )
            let period = name[1..].parse::<usize>().unwrap() / 60;

            // min price
            let min_price = find_min_price(Arc::clone(&asset), &trade.id, "m0060", 0, period);

            // assume trade price is higher than min_price
            if !min_price.is_normal()
                || (trade.price - min_price) / min_price > deviation_rate_to_min
            {
                debug!(
                    "validate slug min price failed, period: {}, price: {}, min price{}, value {} < eviation {}",
                    period, trade.price, min_price, (trade.price - min_price) / min_price, deviation_rate_to_min
                );
                return false;
            }
        }

        true
    }

    fn validate_oscillation(
        asset: Arc<AssetContext>,
        config: Arc<AppConfig>,
        trade: &TradeInfo,
    ) -> bool {
        for name in config.get_trade_oscillation_keys("slug") {
            let oscillation = config.get_trade_oscillation("slug", &name).unwrap();

            // parse period from key (ex: m0300 => 300 / 60 = 5 )
            let period = name[1..].parse::<usize>().unwrap() / 60;

            // min price
            let min_price = find_min_price(Arc::clone(&asset), &trade.id, "m0060", 0, period);
            let max_price = find_max_price(Arc::clone(&asset), &trade.id, "m0060", 0, period);

            // assume trade price is higher than min_price
            if !max_price.is_normal()
                || !min_price.is_normal()
                || (max_price - min_price) / max_price < oscillation
            {
                debug!(
                    "validate slug oscillation failed, period: {}, max price: {}, min price{}, rate {} < oscillation {}",
                    period, max_price, min_price, (max_price - min_price) / max_price, oscillation
                );
                return false;
            }
        }

        true
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
