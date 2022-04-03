use crate::{
    persist::grafana::add_order_annotation,
    vo::{
        biz::{AuditState, MarketHoursType, Order, TradeInfo, TradeTrend, Trend},
        core::{
            AppConfig, AssetContext, AuditRule, DeviationCriteria, OscillationCriteria,
            TrendCriteria, KEY_EXTRA_PRINT_TRADE_META_END_TIME,
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
    let mut loss_order = 0;
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
        if matches!(order.audit, AuditState::Loss) {
            loss_order += 1;
        }
    }
    info!(
        "closed prices {:?}, order count: {}, loss order: {}, total profit: {}, total amount: {}",
        close_prices,
        readers.len(),
        loss_order,
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

    // buffered.push(format!(
    //     "[Config] flash.loss_margin_rate: {:?}",
    //     &config.trade.flash.loss_margin_rate
    // ));
    // for (key, value) in &config.trade.flash.min_deviation_rate {
    //     buffered.push(format!(
    //         "[Config] flash.min_deviation_rate: {:?} = {:?}",
    //         key, value
    //     ));
    // }
    // for (key, value) in &config.trade.flash.oscillation_rage {
    //     buffered.push(format!(
    //         "[Config] flash.oscillation_rage: {:?} = {:?}",
    //         key, value
    //     ));
    // }

    // buffered.push(format!(
    //     "[Config] slug.loss_margin_rate: {:?}",
    //     &config.trade.slug.loss_margin_rate
    // ));
    // for (key, value) in &config.trade.slug.min_deviation_rate {
    //     buffered.push(format!(
    //         "[Config] slug.min_deviation_rate: {:?} = {:?}",
    //         key, value
    //     ));
    // }
    // for (key, value) in &config.trade.slug.oscillation_rage {
    //     buffered.push(format!(
    //         "[Config] slug.oscillation_rage: {:?} = {:?}",
    //         key, value
    //     ));
    // }

    // buffered.push(format!(
    //     "----------------------------------flash--------------------------------------"
    // ));

    // for name in config.get_trade_deviation_keys("flash") {
    //     let deviation_rate_to_min = config.get_trade_deviation("flash", &name).unwrap();

    //     // parse period from key (ex: m0070 => 70 / 10 = 7)
    //     let period = name[1..].parse::<usize>().unwrap() / 10;

    //     // min price
    //     let min_price = find_min_price(Arc::clone(&asset), &trade.id, "m0010", 0, period);

    //     // assume trade price is higher than min_price
    //     buffered.push(format!(
    //         "flash min price, period: {}, price: {}, min price{}, value {} < eviation {} = {}",
    //         period,
    //         trade.price,
    //         min_price,
    //         (trade.price - min_price) / min_price,
    //         deviation_rate_to_min,
    //         !(!min_price.is_normal()
    //             || (trade.price - min_price) / min_price > deviation_rate_to_min)
    //     ));
    // }
    // for name in config.get_trade_oscillation_keys("flash") {
    //     let oscillation = config.get_trade_oscillation("flash", &name).unwrap();

    //     // parse period from key (ex: m0070 => 70 / 10 = 7)
    //     let period = name[1..].parse::<usize>().unwrap() / 60;

    //     // min price
    //     let min_price = find_min_price(Arc::clone(&asset), &trade.id, "m0010", 0, period);
    //     let max_price = find_max_price(Arc::clone(&asset), &trade.id, "m0010", 0, period);

    //     // assume trade price is higher than min_price
    //     buffered.push(format!(
    //         "flash oscillation, period: {}, max price: {}, min price{}, rate {} > oscillation {} = {}",
    //         period,
    //         max_price,
    //         min_price,
    //         (max_price - min_price) / max_price,
    //         oscillation,
    //         !(!max_price.is_normal() || !min_price.is_normal() || (max_price - min_price) / max_price < oscillation)
    //     ));
    // }

    // buffered.push(format!(
    //     "---------------------------------slug---------------------------------------"
    // ));

    // for name in config.get_trade_deviation_keys("slug") {
    //     let deviation_rate_to_min = config.get_trade_deviation("slug", &name).unwrap();

    //     // parse period from key (ex: m0300 => 300 / 30 = 10 )
    //     let period = name[1..].parse::<usize>().unwrap() / 30;

    //     // min price
    //     let min_price = find_min_price(Arc::clone(&asset), &trade.id, "m0030", 0, period);

    //     // assume trade price is higher than min_price
    //     buffered.push(format!(
    //         "slug min price, period: {}, price: {}, min price{}, value {} < eviation {} = {}",
    //         period,
    //         trade.price,
    //         min_price,
    //         (trade.price - min_price) / min_price,
    //         deviation_rate_to_min,
    //         !(!min_price.is_normal()
    //             || (trade.price - min_price) / min_price > deviation_rate_to_min)
    //     ));
    // }
    // for name in config.get_trade_oscillation_keys("slug") {
    //     let oscillation = config.get_trade_oscillation("slug", &name).unwrap();

    //     // parse period from key (ex: m0300 => 300 / 30 = 10 )
    //     let period = name[1..].parse::<usize>().unwrap() / 30;

    //     // min price
    //     let min_price = find_min_price(Arc::clone(&asset), &trade.id, "m0030", 0, period);
    //     let max_price = find_max_price(Arc::clone(&asset), &trade.id, "m0030", 0, period);

    //     // assume trade price is higher than min_price
    //     buffered.push(format!(
    //         "slug oscillation, period: {}, max price: {}, min price{}, rate {} > oscillation {} = {}",
    //         period,
    //         max_price,
    //         min_price,
    //         (max_price - min_price) / max_price, oscillation,
    //         !(!max_price.is_normal() || !min_price.is_normal() || (max_price - min_price) / max_price < oscillation)
    //     ));
    // }

    buffered.push(format!(
        "------------------------------------------------------------------------"
    ));

    let rebounds = rebound_all(trade);
    for trend in rebounds {
        buffered.push(format!("{:?}", trend));
    }

    if let Some(value) = order {
        buffered.push(format!("{:?}", value));
        buffered.push(format!(
            "----------------------------------- {} -----------------------------------",
            &value.id
        ));
    }

    buffered.push(format!(
        "------------------------------------------------------------------------"
    ));

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

pub fn validate_audit_rule(
    asset: Arc<AssetContext>,
    config: Arc<AppConfig>,
    trade: &TradeInfo,
    rule: &AuditRule,
    duration: usize,
) -> bool {
    // validate trend
    if !validate_trend(Arc::clone(&asset), Arc::clone(&config), trade, &rule.trends) {
        return false;
    }

    // validate deviations
    if !validate_deviation(
        Arc::clone(&asset),
        Arc::clone(&config),
        trade,
        duration,
        &rule.deviations,
    ) {
        return false;
    }
    // validate oscillations
    if !validate_oscillation(
        Arc::clone(&asset),
        Arc::clone(&config),
        trade,
        duration,
        &rule.oscillations,
    ) {
        return false;
    }

    true
}

fn validate_trend(
    _asset: Arc<AssetContext>,
    _config: Arc<AppConfig>,
    trade: &TradeInfo,
    trend_rules: &Vec<TrendCriteria>,
) -> bool {
    for trend_rule in trend_rules {
        if let Some(_from) = &trend_rule.from {
            // TODO
        } else {
            let rebound = rebound_at(&trend_rule.to, trade.states.get(&trend_rule.to).unwrap());
            let actual_trend = rebound.trend;
            let target_trend = &trend_rule.trend;

            if &actual_trend != target_trend {
                return false;
            }
            if !trend_rule.up_compare(rebound.up_count) {
                return false;
            }
            if !trend_rule.down_compare(rebound.down_count) {
                return false;
            }
        }
    }

    true
}

fn validate_deviation(
    asset: Arc<AssetContext>,
    _config: Arc<AppConfig>,
    trade: &TradeInfo,
    duration: usize,
    deviation_rules: &Vec<DeviationCriteria>,
) -> bool {
    for deviation_rule in deviation_rules {
        let mut period_from = 0;

        if let Some(from) = &deviation_rule.from {
            period_from = from[1..].parse::<usize>().unwrap() / duration;
        }

        let base_unit = format!("m{:04}", duration);
        // parse period from key (ex: m0070 => 70 / 10 = 7)
        let period_to = deviation_rule.to[1..].parse::<usize>().unwrap() / duration;

        // min price
        let min_price = find_min_price(
            Arc::clone(&asset),
            &trade.id,
            &base_unit,
            period_from,
            period_to,
        );

        // assume trade price is higher than min_price
        if !min_price.is_normal() || (trade.price - min_price) / min_price > deviation_rule.value {
            debug!(
                    "validate {} min price failed, period: {}, price: {}, min price{}, value {} < eviation {}",
                    duration,
                    period_to,
                    trade.price,
                    min_price,
                    (trade.price - min_price) / min_price,
                    deviation_rule.value
                );
            return false;
        }
    }

    true
}

fn validate_oscillation(
    asset: Arc<AssetContext>,
    _config: Arc<AppConfig>,
    trade: &TradeInfo,
    duration: usize,
    oscillation_rules: &Vec<OscillationCriteria>,
) -> bool {
    for oscillation_rule in oscillation_rules {
        let mut period_from = 0;

        if let Some(from) = &oscillation_rule.from {
            period_from = from[1..].parse::<usize>().unwrap() / duration;
        }

        // let oscillation = config.get_trade_oscillation("flash", &name).unwrap();
        let base_unit = format!("m{:04}", duration);

        // parse period from key (ex: m0070 => 70 / 10 = 7)
        let period_to = oscillation_rule.to[1..].parse::<usize>().unwrap() / duration;

        // min price
        let min_price = find_min_price(
            Arc::clone(&asset),
            &trade.id,
            &base_unit,
            period_from,
            period_to,
        );
        let max_price = find_max_price(
            Arc::clone(&asset),
            &trade.id,
            &base_unit,
            period_from,
            period_to,
        );

        // assume trade price is higher than min_price
        if !max_price.is_normal()
            || !min_price.is_normal()
            || (max_price - min_price) / max_price < oscillation_rule.value
        {
            debug!(
                    "validate {} oscillation failed, period: {}, max price: {}, min price{}, rate {} < oscillation {}",
                    duration,
                    period_to,
                    max_price,
                    min_price,
                    (max_price - min_price) / max_price,
                    oscillation_rule.value
                );
            return false;
        }
    }

    true
}

mod flash {

    use super::validate_audit_rule;
    use crate::vo::{
        biz::TradeInfo,
        core::{AppConfig, AssetContext},
    };
    use chrono::Duration;
    use log::*;
    use std::sync::Arc;

    pub fn audit(asset: Arc<AssetContext>, config: Arc<AppConfig>, trade: &TradeInfo) -> bool {
        // general validation from config rules
        for rule in &config.trade.flash.rules {
            if !validate_audit_rule(Arc::clone(&asset), Arc::clone(&config), trade, rule, 10) {
                return false;
            }
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
}

mod slug {

    use super::validate_audit_rule;
    use crate::vo::{
        biz::TradeInfo,
        core::{AppConfig, AssetContext},
    };
    use std::sync::Arc;

    pub fn audit(asset: Arc<AssetContext>, config: Arc<AppConfig>, trade: &TradeInfo) -> bool {
        // general validation from config rules
        for rule in &config.trade.slug.rules {
            if !validate_audit_rule(Arc::clone(&asset), Arc::clone(&config), trade, rule, 30) {
                return false;
            }
        }

        true
    }
}
