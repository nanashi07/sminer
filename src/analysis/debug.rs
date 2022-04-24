use super::trade::{
    find_max_price, find_min_price, get_min_duration, rebound, rebound_all, validate_audit_rule,
};
use crate::{
    vo::{
        biz::{AuditState, MarketHoursType, Order, OrderStatus, TradeInfo, Trend},
        core::{AppConfig, AssetContext, AuditMode, KEY_EXTRA_CONFIG_FILE_PATH},
    },
    Result,
};
use chrono::{TimeZone, Utc};
use log::*;
use rsc::{
    computer::Computer,
    lexer,
    parser::{self, Expr},
};
use std::{
    collections::{BTreeMap, HashMap, HashSet},
    fs::OpenOptions,
    io::{BufWriter, Write},
    path::Path,
    sync::Arc,
};

pub fn profit_evaluate(asset: Arc<AssetContext>, config: Arc<AppConfig>) -> Result<bool> {
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
    let mut pairs: BTreeMap<String, Vec<Order>> = BTreeMap::new();
    let mut formula: Vec<String> = Vec::new();

    let lock = asset.orders();
    let readers = lock.read().unwrap();

    if readers.is_empty() {
        info!("No available order listed");
        return Ok(true);
    }

    let print_orders = false;
    for order in readers.iter().rev() {
        let post_market_price = *close_prices.get(&order.symbol).unwrap();
        // FIXME: use accepted
        let profit = (post_market_price - order.created_price) * order.created_volume as f32;

        if print_orders {
            let level = if order.status == OrderStatus::LossPair || profit < 0.0 {
                log::Level::Warn
            } else {
                log::Level::Info
            };

            log::log!(level, "profit: {} for {:?}", profit, order);
        }

        formula.push(format!(
            "({} - {}) * {}",
            order.symbol, order.created_price, order.created_volume
        ));

        // FIXME: use accepted
        total_amount += order.created_price * order.created_volume as f32;
        total_profit += profit;
        if matches!(order.audit, AuditState::LossClear) {
            loss_order += 1;
        }

        if let Some(constraint) = &order.constraint_id {
            let constraint_id = constraint.to_string();
            if pairs.contains_key(&constraint_id) {
                let list = pairs.get_mut(&constraint_id).unwrap();
                list.push(order.clone());
            } else {
                pairs.insert(constraint_id, vec![order.clone()]);
            }
        }
    }

    let print_pairs = false;
    if print_pairs {
        info!("####################################################################################################");

        info!(
            "OR| {constraint:<10} | {status:<12} | {audit:<25} | {profit_a:<42} | {profit_b:<42} | {total_profit:<18} |",
            constraint = "Pair",
            status = "Status",
            audit = "Audit",
            profit_a = "Profit",
            profit_b = "Profit",
            total_profit = "Total profit"
        );

        info!("OR|------------|--------------|---------------------------|--------------------------------------------|--------------------------------------------|--------------------|");

        for (constraint, orders) in pairs {
            let one = orders.first().unwrap();
            let another = orders.last().unwrap();
            let one_post_market_price = *close_prices.get(&one.symbol).unwrap();
            let another_post_market_price = *close_prices.get(&another.symbol).unwrap();

            let level = if one.status == OrderStatus::LossPair {
                log::Level::Warn
            } else {
                log::Level::Info
            };

            // FIXME: use accepted instead
            log::log!(
                level,
                "OR| {constraint:<10} | {status:<12} | {audit:<25} | {profit_a:<42} | {profit_b:<42} | {total_profit:<18} |",
                constraint = constraint,
                status = format!("{:?}", one.status),
                audit = format!("{:?}/{:?}", one.audit, another.audit),
                profit_a = format!("[{}] ({} - {}) x {}", one.id, one_post_market_price, one.created_price, one.created_volume),
                profit_b = format!("[{}] ({} - {}) x {}", another.id, another_post_market_price, another.created_price, another.created_volume),
                total_profit = (one_post_market_price - one.created_price) * one.created_volume as f32
                    + (another_post_market_price - another.created_price)
                        * another.created_volume as f32
            );
        }
    }

    info!("formula = {}", formula.join(" + "));

    let time = Utc.timestamp_millis(readers.front().unwrap().created_time);

    info!("closed prices {:?}", close_prices,);
    info!(
        "total profit: {}",
        calculate_total_profit(&formula.join(" + "), &close_prices)
    );
    info!(
        "| {symbol:<10} | {date:<10} | {order_count:>11} | {loss_order_count:>11} | {loss_order_rate:<16} | {total_amount:<12} | {total_profit:<12} | {profit_rate:<10} | {sha:<40} |",
        symbol = "Symbols",
        date = "Date",
        order_count = "Order count",
        loss_order_count = "Loss orders",
        loss_order_rate = "Loss orders (%s)",
        total_amount = "Total amount",
        total_profit = "PnL",
        profit_rate = "PnL (%s)",
        sha = "Config SHA"
    );
    info!("|------------|------------|-------------|-------------|------------------|--------------|--------------|------------|------------------------------------------|");
    info!(
        "| {pair:<10} | {date:<10} | {order_count:>11} | {loss_order_count:>11} | {loss_order_rate:<16.5} | {total_amount:<12} | {total_profit:<12} | {profit_rate:<10.5} | {sha} |",
        pair = generate_pair_key(&close_prices),
        date = time.format("%Y-%m-%d"),
        order_count = readers.len(),
        loss_order_count = loss_order,
        loss_order_rate = 100.0 * loss_order as f64 / readers.len() as f64,
        total_profit = total_profit,
        total_amount = total_amount,
        profit_rate = total_profit / total_amount * 100.0,
        sha = get_config_sha(Arc::clone(&config))
    );
    info!("####################################################################################################");

    // output config
    for option in &config.trade.options {
        info!("symbols: {:?}", option.symbols);
        info!(
            "validateIncreasedProfit: {}",
            option.validate_increased_profit
        );
        info!("enableProfitTake: {}", option.enable_profit_take);
        info!("enableEarlyClear: {}", option.enable_early_clear);
        info!("enableLossClear: {}", option.enable_loss_clear);
        info!("enableCloseTrade: {}", option.enable_close_trade);
        info!("maxOrderAmount: {}", option.max_order_amount);
        info!("profitTakeRate: {}", option.profit_take_rate);
        info!("earlyClearRate: {}", option.early_clear_rate);
        info!("------------------------------------------------------------------------");
    }
    for (name, mode) in [
        ("flash", &config.trade.flash),
        ("slug", &config.trade.slug),
        ("revert", &config.trade.revert),
    ] {
        print_config(name, mode);

        info!("------------------------------------------------------------------------");
    }

    info!("####################################################################################################");
    info!("####################################################################################################");

    Ok(true) //FIXME:
}

fn generate_pair_key(close_prices: &HashMap<String, f32>) -> String {
    let mut symbols: Vec<String> = close_prices.keys().map(|k| k.to_string()).collect();
    symbols.sort();
    symbols.join("-")
}

fn get_config_sha(config: Arc<AppConfig>) -> String {
    let config_file = config.extra_get(KEY_EXTRA_CONFIG_FILE_PATH).unwrap();
    let base = Path::new(&config_file).parent().unwrap();
    let sha = Path::file_name(base).unwrap().to_str().unwrap();
    sha.to_owned()
}

fn calculate_total_profit(formula: &str, closed_price: &HashMap<String, f32>) -> f64 {
    let tokens = lexer::tokenize(&formula, true).unwrap();
    let mut ast = parser::parse(&tokens).unwrap();
    let mut computer = Computer::<f64>::default();

    // calculate current first, mark offset flag
    for (symbol, price) in closed_price {
        ast.replace(
            &Expr::Identifier(symbol.to_owned()),
            &Expr::Constant(*price as f64),
            false,
        );
    }

    let value = computer.compute(&ast).unwrap();
    value
}

fn print_config(name: &str, mode: &AuditMode) {
    info!(
        "[Config] {}.loss_margin_rate: {:?}",
        name, &mode.loss_margin_rate
    );

    for (index, rule) in mode.rules.iter().filter(|r| !r.evaluation).enumerate() {
        info!(
            "########## [{} rule {} - {:?}] ##########",
            name, index, rule.mode
        );
        info!("[rule {}] SYMBOLS: {:?}", index, rule.symbols);
        for trend in &rule.trends {
            info!(
                "[rule {}] TREND, from: {:?}, to: {}, trend: {:?}, up: {:?}, down: {:?}",
                index, trend.from, trend.to, trend.trend, trend.up, trend.down
            );
        }
        for deviation in &rule.deviations {
            info!(
                "[rule {}] DEVIATION, from: {:?}, to: {}, value: {}%",
                index,
                deviation.from,
                deviation.to,
                deviation.value * 100.0
            );
        }
        for oscillation in &rule.oscillations {
            info!(
                "[rule {}] OSCILLATION, from: {:?}, to: {}, value: {}%",
                index,
                oscillation.from,
                oscillation.to,
                oscillation.value * 100.0
            );
        }
        for lower in &rule.lowers {
            info!(
                "[rule {}] LOWER, from: {:?}, to: {}, compareTo: {}, duration: {}",
                index, lower.from, lower.to, lower.compare_to, lower.duration
            );
        }
    }
}

fn buffer_config(buffered: &mut Vec<String>, name: &str, mode: &AuditMode) {
    buffered.push(format!(
        "[Config] {}.loss_margin_rate: {:?}",
        name, &mode.loss_margin_rate
    ));
    for (index, rule) in mode.rules.iter().filter(|r| !r.evaluation).enumerate() {
        buffered.push(format!(
            "########## [{} rule {} - {:?}] ##########",
            name, index, rule.mode
        ));
        buffered.push(format!("[rule {}] SYMBOLS: {:?}", index, rule.symbols));
        for trend in &rule.trends {
            buffered.push(format!(
                "[rule {}] TREND, from: {:?}, to: {}, trend: {:?}, up: {:?}, down: {:?}",
                index, trend.from, trend.to, trend.trend, trend.up, trend.down
            ));
        }
        for deviation in &rule.deviations {
            buffered.push(format!(
                "[rule {}] DEVIATION, from: {:?}, to: {}, value: {}%",
                index,
                deviation.from,
                deviation.to,
                deviation.value * 100.0
            ));
        }
        for oscillation in &rule.oscillations {
            buffered.push(format!(
                "[rule {}] OSCILLATION, from: {:?}, to: {}, value: {}%",
                index,
                oscillation.from,
                oscillation.to,
                oscillation.value * 100.0
            ));
        }
        for lower in &rule.lowers {
            buffered.push(format!(
                "[rule {}] LOWER, from: {:?}, to: {}, compareTo: {}, duration: {}",
                index, lower.from, lower.to, lower.compare_to, lower.duration
            ));
        }
    }
}

fn buffer_matched_rules(
    asset: Arc<AssetContext>,
    config: Arc<AppConfig>,
    trade: &TradeInfo,
    buffered: &mut Vec<String>,
    mode_name: &str,
    mode: &AuditMode,
) {
    let duration: usize = 10;

    for (index, rule) in mode.rules.iter().enumerate() {
        let result = validate_audit_rule(Arc::clone(&asset), Arc::clone(&config), trade, rule);
        let matched = result && !rule.evaluation;

        if !matched {
            continue;
        }

        for trend_rule in &rule.trends {
            let mut result = true;
            let mut trend_from = 0;
            let trend_to = trend_rule.to[1..].parse::<usize>().unwrap();
            if let Some(from) = &trend_rule.from {
                trend_from = from[1..].parse::<usize>().unwrap();
            }

            // calculate min duration
            let keys: Vec<String> = trade.states.keys().map(|k| k.to_string()).collect();
            let duration = get_min_duration(trend_to - trend_from, &keys).unwrap();

            let rebound = rebound(&duration, trend_from, trade.states.get(&duration).unwrap());
            let actual_trend = rebound.trend;
            let expected_trend = &trend_rule.trend;

            if &actual_trend != expected_trend {
                result = false;
            }

            let first_section = rebound.sections[0];
            let second_section = if rebound.sections.len() > 1 {
                rebound.sections[1]
            } else {
                0
            };

            match actual_trend {
                Trend::Upward => {
                    if !trend_rule.up_compare(first_section) {
                        result = false;
                    }
                    if !trend_rule.down_compare(second_section) {
                        result = false;
                    }
                }
                Trend::Downward => {
                    if !trend_rule.down_compare(first_section) {
                        result = false;
                    }
                    if !trend_rule.up_compare(second_section) {
                        result = false;
                    }
                }
            }

            buffered.push(format!(
                "[{}/{:?}] {} trend, from: {:?}, to: {}, trend: {:?}, up: {:?}, down: {:?} = {}",
                index,
                rule.mode,
                mode_name,
                trend_rule.from,
                trend_rule.to,
                trend_rule.trend,
                trend_rule.up,
                trend_rule.down,
                result,
            ));
        }
        for deviation_rule in &rule.deviations {
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

            buffered.push(format!(
                "[{}/{:?}] {} min price, period: {:04} - {:04}, price: {}, min price: {}, rate {:.03}% < deviation {:.03}% = {}",
                index,
                rule.mode,
                mode_name,
                period_from * duration,
                period_to * duration,
                trade.price,
                min_price,
                (trade.price - min_price) / min_price * 100.0,
                deviation_rule.value * 100.0,
                !(!min_price.is_normal()
                    || (trade.price - min_price) / min_price > deviation_rule.value)
            ));
        }
        for oscillation_rule in &rule.oscillations {
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

            buffered.push(format!(
                "[{}/{:?}] {} oscillation, period: {:04} - {:04}, max price: {}, min price: {}, rate {:.03}% > oscillation {:.03}% = {}",
                index,
                rule.mode,
                mode_name,
                period_from * duration,
                period_to * duration,
                max_price,
                min_price,
                (max_price - min_price) / max_price * 100.0,
                oscillation_rule.value * 100.0,
                !(!max_price.is_normal() || !min_price.is_normal() || (max_price - min_price) / max_price < oscillation_rule.value)
            ));
        }
        for lower_rule in &rule.lowers {
            let mut period_from = 0;

            if let Some(from) = &lower_rule.from {
                period_from = from[1..].parse::<usize>().unwrap() / duration;
            }

            let base_unit = format!("m{:04}", duration);

            // parse period from key (ex: m0070 => 70 / 10 = 7)
            let period_to = lower_rule.to[1..].parse::<usize>().unwrap() / duration;

            // get min price from wider range
            let min_price = find_min_price(
                Arc::clone(&asset),
                &trade.id,
                &base_unit,
                period_from,
                period_to,
            );

            // find price time lower than min_price before
            let mut recent_min_price = f32::NAN;
            if min_price.is_normal() {
                let recent_period_to =
                    lower_rule.compare_to[1..].parse::<usize>().unwrap() / duration;
                recent_min_price = find_min_price(
                    Arc::clone(&asset),
                    &trade.id,
                    &base_unit,
                    0,
                    recent_period_to,
                );

                if !recent_min_price.is_normal() || recent_min_price > min_price {
                    buffered.push(format!(
                        "[{}/{:?}] {} lower, period: {:04} - {:04}, min price: {}, recent min price: {} in {} = false",
                        index,
                        rule.mode,
                        mode_name,
                        period_from * duration,
                        period_to * duration,
                        min_price,
                        recent_min_price,
                        &lower_rule.compare_to[1..]
                    ));
                    continue;
                }
            };
            buffered.push(format!(
                "[{}/{:?}] {} lower, period: {:04} - {:04}, min price: {}, recent min price: {} in {} = true",
                index,
                rule.mode,
                mode_name,
                period_from * duration,
                period_to * duration,
                min_price,
                recent_min_price,
                &lower_rule.compare_to[1..]
            ));
        }

        buffered.push(format!(
            "[{}/{:?}] {}, result: {}, evaluate: {}",
            index, rule.mode, mode_name, result, rule.evaluation,
        ));
    }
}

// print details
pub fn print_meta(
    asset: Arc<AssetContext>,
    config: Arc<AppConfig>,
    order: Option<Order>,
    trade: &TradeInfo,
    output_message: bool,
    output_order: bool,
) -> Result<()> {
    if !output_message && !output_order {
        return Ok(());
    }

    let mut buffered: Vec<String> = Vec::new();

    buffered.push(format!(
        "################################### MSG-{} @ {} ###################################",
        &trade.message_id,
        Utc.timestamp_millis(trade.time).format("%Y-%m-%d %H:%M:%S")
    ));

    // output config
    for option in &config.trade.options {
        buffered.push(format!("symbols: {:?}", option.symbols));
        buffered.push(format!(
            "validateIncreasedProfit: {}",
            option.validate_increased_profit
        ));
        buffered.push(format!("enableProfitTake: {}", option.enable_profit_take));
        buffered.push(format!("enableEarlyClear: {}", option.enable_early_clear));
        buffered.push(format!("enableLossClear: {}", option.enable_loss_clear));
        buffered.push(format!("enableCloseTrade: {}", option.enable_close_trade));
        buffered.push(format!("maxOrderAmount: {}", option.max_order_amount));
        buffered.push(format!("profitTakeRate: {}", option.profit_take_rate));
        buffered.push(format!("earlyClearRate: {}", option.early_clear_rate));
        buffered.push(format!(
            "------------------------------------------------------------------------"
        ));
    }
    for (name, mode) in [
        ("flash", &config.trade.flash),
        ("slug", &config.trade.slug),
        ("revert", &config.trade.revert),
    ] {
        buffer_config(&mut buffered, name, mode);

        buffered.push(format!(
            "------------------------------------------------------------------------"
        ));
    }

    buffered.push(format!("{:?}", trade));

    if let Some(value) = &order {
        buffered.push(format!(
            "----------------------------------- {} / {:?} / {:?} -----------------------------------",
            &value.id, &value.status, &value.audit
        ));
        buffered.push(format!("{:?}", value));
    }

    for (name, mode) in [
        ("flash", &config.trade.flash),
        ("slug", &config.trade.slug),
        ("revert", &config.trade.revert),
    ] {
        buffered.push(format!(
            "----------------------------------{}--------------------------------------",
            name
        ));

        if name == "revert" {
            let option = config.trade.get_option(&trade.id);
            if !option.enable_early_clear {
                continue;
            }
        }

        buffer_matched_rules(
            Arc::clone(&asset),
            Arc::clone(&config),
            &trade,
            &mut buffered,
            name,
            mode,
        );
    }

    buffered.push(format!(
        "------------------------------------------------------------------------"
    ));

    let rebounds = rebound_all(trade);
    for trend in rebounds {
        buffered.push(format!("{:?}", trend));
    }

    buffered.push(format!(
        "------------------------------------------------------------------------"
    ));

    let price_check_ranges = [
        // per 30 sec
        (0000, 0030),
        (0030, 0060),
        (0060, 0090),
        (0090, 0120),
        (0120, 0150),
        (0150, 0180),
        (0180, 0210),
        (0210, 0240),
        (0240, 0270),
        (0270, 0300),
        (0300, 0330),
        (0330, 0360),
        (0360, 0390),
        (0390, 0420),
        (0420, 0450),
        (0450, 0480),
        (0480, 0510),
        (0510, 0540),
        (0540, 0570),
        (0570, 0600),
        // per 5 min
        (0000, 0300),
        (0300, 0600),
        (0600, 0900),
        (0900, 1200),
        (1200, 1500),
        (1500, 1800),
        (1800, 2100),
        (2100, 2400),
        (2400, 2700),
        (2700, 3000),
        (3000, 3300),
        (3300, 3600),
        //
    ];

    for (start, end) in price_check_ranges {
        let min_price =
            find_min_price(Arc::clone(&asset), &trade.id, "m0010", start / 10, end / 10);
        let max_price =
            find_max_price(Arc::clone(&asset), &trade.id, "m0010", start / 10, end / 10);

        buffered.push(format!(
            "{start:04}-{end:04} price: {price:.4}, min: {min:.4}, min diff: {min_diff:.4} ({min_diff_rate:.3}%), max: {max:.4}, min-max: {min_max_diff:.4} ({min_max_diff_rate:.3}%)",
            start             = start * 10,
            end               = end * 10,
            min               = min_price,
            price             = trade.price,
            min_diff          = trade.price - min_price,
            min_diff_rate     = 100.0 * (trade.price - min_price) / min_price,
            max               = max_price,
            min_max_diff      = max_price - min_price,
            min_max_diff_rate = 100.0 * (max_price - min_price) / max_price,
        ));
    }

    // only enable for check detail
    if false {
        let protfolio_map = asset.symbol_protfolios(&trade.id).unwrap();
        for (unit, lock) in protfolio_map {
            let reader = lock.read().unwrap();
            buffered.push(format!(
                "*********************************** unit {} ***********************************",
                unit
            ));
            for protfolios in reader.iter() {
                buffered.push(format!("unit: {}, {:?}", unit, protfolios));
            }
        }
    }

    if output_message {
        let config_file = config.extra_get(KEY_EXTRA_CONFIG_FILE_PATH).unwrap();
        let base = Path::new(&config_file).parent().unwrap();

        let path = format!(
            "{base}/msgs/{symbol}/{day}/MSG-{time}-{id}.txt",
            base = base.to_str().unwrap(),
            symbol = &trade.id,
            day = Utc.timestamp_millis(trade.time).format("%Y-%m-%d"),
            time = Utc.timestamp_millis(trade.time).format("%Y%m%d%H%M%S"),
            id = &trade.message_id
        );
        write_file(&path, &buffered)?;
    }

    if output_order && !&order.is_none() {
        let config_file = config.extra_get(KEY_EXTRA_CONFIG_FILE_PATH).unwrap();
        let base = Path::new(&config_file).parent().unwrap();

        let path = format!(
            "{base}/orders/{symbol}/{day}/ORD-{time}-{id}.txt",
            base = base.to_str().unwrap(),
            symbol = &trade.id,
            day = Utc.timestamp_millis(trade.time).format("%Y-%m-%d"),
            time = Utc.timestamp_millis(trade.time).format("%Y%m%d%H%M%S"),
            id = &trade.message_id
        );

        write_file(&path, &buffered)?;
    }

    Ok(())
}

fn write_file(path: &str, buffered: &Vec<String>) -> Result<()> {
    let parent = Path::new(path).parent().unwrap();
    if !parent.exists() {
        std::fs::create_dir_all(parent.to_str().unwrap())?;
    }

    let output = OpenOptions::new()
        .write(true)
        .create(true)
        .truncate(true)
        .open(path)?;
    let mut writer = BufWriter::new(output);

    for line in buffered {
        write!(&mut writer, "{}\n", &line)?;
    }

    Ok(())
}
