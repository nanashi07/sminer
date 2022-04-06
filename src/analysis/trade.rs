use crate::{
    analysis::debug::print_meta,
    persist::grafana::add_order_annotation,
    vo::{
        biz::{AuditState, MarketHoursType, Order, Protfolio, TradeInfo, TradeTrend, Trend},
        core::{
            AppConfig, AssetContext, AuditRule, DeviationCriteria, LowerCriteria,
            OscillationCriteria, TrendCriteria, KEY_EXTRA_PRINT_TRADE_META_END_TIME,
            KEY_EXTRA_PRINT_TRADE_META_START_TIME,
        },
    },
    Result,
};
use chrono::{TimeZone, Utc};
use log::*;
use std::{f64::NAN, sync::Arc};

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
            AuditState::Flash
            | AuditState::Slug
            | AuditState::ProfitTaking
            | AuditState::EarlySell => {
                // calculate volume
                let suspected_volume =
                    calculate_volum(Arc::clone(&asset), Arc::clone(&config), &trade);

                if suspected_volume == 0 {
                    warn!("suspected order volume is zero, ignore order");
                    return Ok(());
                }

                // get rival price
                let mut rival_price = f32::NAN;
                if let Some(rival_ticker) = asset.get_latest_rival_ticker(&trade.id) {
                    rival_price = rival_ticker.price;
                }

                // create order
                let order = Order::new(
                    &trade.id,
                    trade.price,
                    rival_price,
                    suspected_volume,
                    trade.action_time(),
                    state.clone(),
                );
                if asset.add_order(order.clone()) {
                    let order_id = order.id.clone();

                    if config.replay.export_enabled("order") {
                        print_meta(
                            Arc::clone(&asset),
                            Arc::clone(&config),
                            Some(order.clone()),
                            &trade,
                        )
                        .unwrap_or_default();
                    }

                    let symbol = trade.id.clone();
                    let time = Utc.timestamp_millis(trade.action_time());
                    let tags = vec![
                        trade.id.clone(),
                        order_id,
                        format!("{:?}", &state),
                        format!("MSG-{}", &trade.message_id),
                        format!("${}", &order.created_price),
                        format!("v{}", &order.created_volume),
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

                if suspected_volume == 0 {
                    warn!("suspected order volume is zero, ignore order");
                    return Ok(());
                }

                // get rival price
                let mut rival_price = f32::NAN;
                if let Some(rival_ticker) = asset.get_latest_rival_ticker(&trade.id) {
                    rival_price = rival_ticker.price;
                }

                // create order
                let order = Order::new(
                    &trade.id,
                    trade.price,
                    rival_price,
                    suspected_volume,
                    trade.action_time(),
                    state.clone(),
                );
                if asset.add_order(order.clone()) {
                    let order_id = order.id.clone();

                    if config.replay.export_enabled("order") {
                        print_meta(
                            Arc::clone(&asset),
                            Arc::clone(&config),
                            Some(order.clone()),
                            &trade,
                        )
                        .unwrap_or_default();
                    }

                    let symbol = trade.id.clone();
                    let time = Utc.timestamp_millis(trade.action_time());
                    let tags = vec![
                        trade.id.clone(),
                        order_id,
                        format!("{:?}", &state),
                        format!("MSG-{}", &trade.message_id),
                        format!("${}", &order.created_price),
                        format!("v{}", &order.created_volume),
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
        // calculation concept:
        // * price change rate / total price change amount, should be the same
        // * adjust volume for balance total amount difference (!important)

        // ex:
        // TQQQ: 56.194 (rival price)
        // SQQQ: 35.351
        // SQQQ volume: 10

        // TQQQ: 53.845
        // SQQQ: 36.845
        // TQQQ volume: ?

        // rival_last_price = 35.351
        // rival_current_price = 36.845
        // rival_volume = 10
        // current_price = 53.845
        // last_price = 56.194

        // rival_total_change_amount = (rival_current_price - rival_last_price) * rival_volume
        //   = (36.845 - 35.351) * 10
        //   = 14.94

        // total_change_amount = rival_total_change_amount * -1.0
        //   = -14.94

        // expected_volume = total_change_amount / (current_price - last_price)
        //   = -14.94 / (53.845 - 56.194)
        //   = 6.3601532567049808429118773946360153256

        // test profit by calculate result
        // (53.845 - 56.194) * expected_volume + (36.845 - 35.351) * 10 > 0

        // *** (!important) clear price affects the write of result ***
        // (53.845 - 56.194) * 6 + (36.845 - 35.351) * 10 = 0.846
        // (42.355 - 56.194) * 6 + (48.540 - 35.351) * 10 = 48.856
        // (62.383 - 56.194) * 6 + (31.428 - 35.351) * 10 = -2.096
        // TODO: think a better approach to avoid this effection

        let rival_volume = rival_order.created_volume;
        let rival_last_price = rival_order.created_price;

        let current_price = trade.price;
        let mut last_price = rival_order.created_rival_price;

        // get rival price, ex: SQQQ current price
        let mut rival_current_price = f32::NAN;
        if let Some(rival_symbol) = asset.find_pair_symbol(&trade.id) {
            if let Some(rival_trade) = asset.get_latest_trade(&rival_symbol) {
                rival_current_price = rival_trade.price;
            }
        }
        if rival_current_price.is_nan() {
            warn!("rival price not available, skip order {:?}", trade);
            return 0;
        }

        let rival_price_change_rate = (rival_current_price - rival_last_price) / rival_last_price;
        let mut price_change_ratee = (current_price - last_price) / last_price;

        // 1. when both bull/bear upward or bull/bear downward, result volume is negative
        // 2. when change rate between bulk and bear is too high/low, result volume is too large (allow max to 3x)
        // use estimated last price for calculation
        if rival_price_change_rate * price_change_ratee > 0.0
            || (rival_price_change_rate / price_change_ratee).abs() > 3.0
            || (rival_price_change_rate / price_change_ratee).abs() > 3.0
        {
            warn!(
                "rival_price_change_rate * price_change_ratee = {} * {} > 0.0 : {}",
                rival_price_change_rate,
                price_change_ratee,
                rival_price_change_rate * price_change_ratee
            );
            warn!(
                "(rival_price_change_rate / price_change_ratee).abs() = abs({} / {}) > 3.0 : {}",
                rival_price_change_rate,
                price_change_ratee,
                (rival_price_change_rate / price_change_ratee).abs()
            );
            warn!(
                "(rival_price_change_rate / price_change_ratee).abs() = abs({} / {}) > 3.0 : {}",
                rival_price_change_rate,
                price_change_ratee,
                (rival_price_change_rate / price_change_ratee).abs()
            );
            let rival_change_rate = (rival_current_price - rival_last_price) / rival_last_price;
            let change_rate = rival_change_rate * -1.0;
            // change_rate = (current_price - estimated_last_price) / estimated_last_price
            // => estimated_last_price + change_rate * estimated_last_price = current_price
            // => estimated_last_price = current_price / (1.0 + change_rate)
            last_price = current_price / (1.0 + change_rate);
            price_change_ratee = (current_price - last_price) / last_price;
        }

        let rival_total_change_amount =
            (rival_current_price - rival_last_price) * rival_volume as f32;

        let total_change_amount = rival_total_change_amount * -1.0;

        // total_change_amount = (current_price - last_price) * volume;
        let expected_volume = total_change_amount / (current_price - last_price);

        // for debug only
        if false {
            info!("rival_volume = {}", rival_volume);
            info!("rival_last_price = {}", rival_last_price);
            info!("rival_current_price = {}", rival_current_price);
            info!("rival_price_change_rate = {}", rival_price_change_rate);

            info!("current_price = {}", current_price);
            info!("last_price = {}", last_price);
            info!("price_change_ratee = {}", price_change_ratee);

            info!(
                "chagne rate diff: {} / {} = {}, {} / {} = {}",
                rival_price_change_rate,
                price_change_ratee,
                rival_price_change_rate / price_change_ratee,
                price_change_ratee,
                rival_price_change_rate,
                price_change_ratee / rival_price_change_rate
            );

            info!("rival_total_change_amount = (rival_current_price - rival_last_price) * rival_volume >> {} = ({} - {}) * {}",
                rival_total_change_amount,
                rival_current_price,
                rival_last_price,
                rival_volume as f32
            );

            info!(
                "total_change_amount = rival_total_change_amount * -1.0 >> {} = {} * -1.0",
                total_change_amount, rival_total_change_amount
            );
            info!(
                "expected_volume = total_change_amount / (current_price - last_price) >> {} = {} / ({} - {})",
                expected_volume,
                total_change_amount,
                current_price,
                last_price
            );
        }

        // test profit
        if (rival_current_price - rival_last_price) * (rival_volume as f32)
            + (current_price - last_price) * expected_volume.floor()
            > (rival_current_price - rival_last_price) * (rival_volume as f32)
                + (current_price - last_price) * expected_volume.ceil()
        {
            let mut final_volume = expected_volume.floor() as u32;
            let restriction = final_volume / 2;
            // adjust volume to make profit positive
            while (rival_current_price - rival_last_price) * (rival_volume as f32)
                + (current_price - last_price) * (final_volume as f32)
                < 0.0
            {
                final_volume -= 1;
                if final_volume < restriction {
                    break;
                }
            }
            final_volume
        } else {
            let mut final_volume = expected_volume.ceil() as u32;
            let restriction = final_volume * 3;
            // adjust volume to make profit positive
            while (rival_current_price - rival_last_price) * (rival_volume as f32)
                + (current_price - last_price) * (final_volume as f32)
                < 0.0
            {
                final_volume += 1;
                if final_volume > restriction {
                    break;
                }
            }
            final_volume
        }
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
            // not affected by config.replay.exports.order
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

    // check last pair order if exists, make sure make off gain profit
    if let Some(rival_symbol) = asset.find_pair_symbol(&trade.id) {
        if let Some(rival_order) = asset.find_running_order(&rival_symbol) {
            // get latest trade of rival ticker
            if let Some(rival_trade) = asset.get_latest_trade(&rival_symbol) {
                // FIXME : use accepted price
                let price_diff = rival_trade.price - rival_order.created_price;
                let profit = price_diff * rival_order.created_volume as f32;
                let rate = price_diff / rival_order.created_price;

                if matches!(result, AuditState::Flash | AuditState::Slug) {
                    if profit < 0.0 {
                        warn!(
                            "block write off due to profit is negative: [{}] ({} - {}) x {} = {}",
                            rival_symbol,
                            rival_trade.price,
                            rival_order.created_price,
                            rival_order.created_volume,
                            profit
                        );
                        result = AuditState::Decline;
                    }
                } else {
                    // early sell even if there is no match rule found
                    if rate > 0.005 {
                        info!("profit taking, profit = {} ({:.04}%)", profit, rate * 100.0);
                        result = AuditState::ProfitTaking;
                    }
                    // early sell when the trend is starting to go down
                    else if rate > 0.0
                        && rate < 0.005 // TODO
                        && revert::audit(Arc::clone(&asset), Arc::clone(&config), &trade)
                    {
                        info!("early sell, profit = {} ({}%)", profit, rate * 100.0);
                        result = AuditState::EarlySell;
                    }
                }
            }
        }
    }

    // TODO: reutrn if decline, unnecessary to check following

    // FIXME: check previous order status
    if let Some(exists_order) = asset.find_running_order(&trade.id) {
        // exists order, check PnL
        if recognize_loss(asset, config, trade, &exists_order) {
            return AuditState::Loss;
        }
        result = AuditState::Decline;
    }

    // TODO: real time calculate PnL and avoid get loss finally

    // 區間內與最大值的價差（比率）
    // 與反向 eft 的利差（數值）

    result
}

fn recognize_loss(
    _asset: Arc<AssetContext>,
    config: Arc<AppConfig>,
    trade: &TradeInfo,
    order: &Order,
) -> bool {
    let margin_rate = match order.audit {
        AuditState::Flash => config.trade.flash.loss_margin_rate,
        AuditState::Slug => config.trade.slug.loss_margin_rate,
        _ => 0.006, // not affected
    };
    let price = trade.price;
    // FIXME : use accepted price
    let order_price = order.created_price;

    // check if loss over than configured margin value
    price < order_price && (order_price - price) / order_price > margin_rate
}

pub fn find_min_price_time(
    asset: Arc<AssetContext>,
    symbol: &str,
    unit: &str,
    start: usize,
    min_price: f32,
) -> Option<Protfolio> {
    if let Some(lock) = asset.get_protfolios(symbol, unit) {
        let reader = lock.read().unwrap();
        if reader.is_empty() {
            None
        } else {
            if let Some(last_protfolio) =
                reader.iter().skip(start).find(|p| p.min_price < min_price)
            {
                Some(last_protfolio.clone())
            } else {
                None
            }
        }
    } else {
        None
    }
}

pub fn find_min_price(
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

pub fn find_max_price(
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
    // analysis trade trend and match config
    if !validate_trend(Arc::clone(&asset), Arc::clone(&config), trade, &rule.trends) {
        return false;
    }

    // validate deviations between current price to min price
    if !validate_deviation(
        Arc::clone(&asset),
        Arc::clone(&config),
        trade,
        duration,
        &rule.deviations,
    ) {
        return false;
    }
    // validate oscillations, between max price and min price
    if !validate_oscillation(
        Arc::clone(&asset),
        Arc::clone(&config),
        trade,
        duration,
        &rule.oscillations,
    ) {
        return false;
    }

    // validate min price, which has lower price than current min
    if !validate_lower(
        Arc::clone(&asset),
        Arc::clone(&config),
        trade,
        duration,
        &rule.lowers,
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
                "validate min price failed, period: {:04} - {:04}, price: {}, min price: {}, value {} < deviation {}",
                period_from * duration,
                period_to * duration,
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
                "validate oscillation failed, period: {:04} - {:04}, max price: {}, min price: {}, rate {} < oscillation {}",
                period_from * duration,
                period_to * duration,
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

fn validate_lower(
    asset: Arc<AssetContext>,
    _config: Arc<AppConfig>,
    trade: &TradeInfo,
    duration: usize,
    lower_rules: &Vec<LowerCriteria>,
) -> bool {
    for lower_rule in lower_rules {
        let mut period_from = 0;

        if let Some(from) = &lower_rule.from {
            period_from = from[1..].parse::<usize>().unwrap() / duration;
        }

        let base_unit = format!("m{:04}", duration);

        // parse period from key (ex: m0070 => 70 / 10 = 7)
        let period_to = lower_rule.to[1..].parse::<usize>().unwrap() / duration;

        // min price
        let min_price = find_min_price(
            Arc::clone(&asset),
            &trade.id,
            &base_unit,
            period_from,
            period_to,
        );

        // find price time lower than min_price before
        if min_price.is_normal() {
            if let Some(last_protfolio) =
                find_min_price_time(Arc::clone(&asset), &trade.id, &base_unit, 0, min_price)
            {
                // there is lower price than catched min price with this duration
                let last_time = last_protfolio.time;
                if last_time > Utc::now().timestamp_millis() - lower_rule.duration as i64 {
                    debug!(
                        "validate lower failed, period: {:04} - {:04}, min price: {}, last min price: {} at {} ({}s before)",
                        period_from * duration,
                        period_to * duration,
                        min_price,
                        last_protfolio.min_price,
                        Utc.timestamp_millis(last_time).format("%Y-%m-%d %H:%M:%s"),
                        (trade.time - last_time) / 1000
                    );
                    return false;
                }
            } else {
                // no trade found, there is no lower price than current min price
            }
        } else {
            return false;
        };
    }

    true
}

pub mod flash {

    use super::validate_audit_rule;
    use crate::vo::{
        biz::TradeInfo,
        core::{AppConfig, AssetContext, AuditRuleType},
    };
    use chrono::Duration;
    use log::*;
    use std::sync::Arc;

    pub const BASE_DURATION: usize = 10;

    pub fn audit(asset: Arc<AssetContext>, config: Arc<AppConfig>, trade: &TradeInfo) -> bool {
        let mut results: Vec<bool> = Vec::new();

        // general validation from config rules, at least one success and no blocked rule
        for rule in config.trade.flash.rules.iter().filter(|r| !r.evaluation) {
            if validate_audit_rule(
                Arc::clone(&asset),
                Arc::clone(&config),
                trade,
                rule,
                BASE_DURATION,
            ) {
                match rule.mode {
                    AuditRuleType::Permit => {
                        results.push(true);
                    }
                    AuditRuleType::Deny => {
                        results.push(false);
                    }
                }
            } else {
                match rule.mode {
                    AuditRuleType::Permit => {} // ignore failed
                    AuditRuleType::Deny => {
                        results.push(true);
                    }
                }
            }
        }

        // check last order to prevent place mutiple orders (watch within 30s)
        if let Some(order) = asset.find_last_flash_order(&trade.id) {
            if trade.action_time() - order.created_time < Duration::seconds(30).num_milliseconds() {
                debug!("Found flash order within 30s, ignore {:?}", trade);
                results.push(false);
            }
        }

        !results.is_empty() && results.iter().all(|success| *success)
    }
}

pub mod slug {

    use super::validate_audit_rule;
    use crate::vo::{
        biz::TradeInfo,
        core::{AppConfig, AssetContext, AuditRuleType},
    };
    use std::sync::Arc;

    pub const BASE_DURATION: usize = 30;

    pub fn audit(asset: Arc<AssetContext>, config: Arc<AppConfig>, trade: &TradeInfo) -> bool {
        let mut results: Vec<bool> = Vec::new();

        // general validation from config rules, at least one success and no blocked rule
        for rule in config.trade.slug.rules.iter().filter(|r| !r.evaluation) {
            if validate_audit_rule(
                Arc::clone(&asset),
                Arc::clone(&config),
                trade,
                rule,
                BASE_DURATION,
            ) {
                match rule.mode {
                    AuditRuleType::Permit => {
                        results.push(true);
                    }
                    AuditRuleType::Deny => {
                        results.push(false);
                    }
                }
            } else {
                match rule.mode {
                    AuditRuleType::Permit => {} // ignore failed
                    AuditRuleType::Deny => {
                        results.push(true);
                    }
                }
            }
        }

        !results.is_empty() && results.iter().all(|success| *success)
    }
}

pub mod revert {

    use super::validate_audit_rule;
    use crate::vo::{
        biz::TradeInfo,
        core::{AppConfig, AssetContext, AuditRuleType},
    };
    use std::sync::Arc;

    pub const BASE_DURATION: usize = 10;

    pub fn audit(asset: Arc<AssetContext>, config: Arc<AppConfig>, trade: &TradeInfo) -> bool {
        let mut results: Vec<bool> = Vec::new();

        // general validation from config rules, at least one success and no blocked rule
        for rule in config.trade.revert.rules.iter().filter(|r| !r.evaluation) {
            if validate_audit_rule(
                Arc::clone(&asset),
                Arc::clone(&config),
                trade,
                rule,
                BASE_DURATION,
            ) {
                match rule.mode {
                    AuditRuleType::Permit => {
                        results.push(true);
                    }
                    AuditRuleType::Deny => {
                        results.push(false);
                    }
                }
            } else {
                match rule.mode {
                    AuditRuleType::Permit => {} // ignore failed
                    AuditRuleType::Deny => {
                        results.push(true);
                    }
                }
            }
        }

        !results.is_empty() && results.iter().all(|success| *success)
    }
}
