use crate::{
    vo::{
        biz::{AuditState, MarketHoursType, Order, OrderStatus, TradeInfo},
        core::{AppConfig, AssetContext},
    },
    Result,
};
use chrono::{TimeZone, Utc};
use log::*;
use std::{
    collections::{BTreeMap, HashMap, HashSet},
    fs::OpenOptions,
    io::{BufWriter, Write},
    path::Path,
    sync::Arc,
};

use super::trade::{
    find_max_price, find_min_price, find_min_price_time, flash, rebound_all, rebound_at, slug,
    validate_audit_rule,
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

    let lock = asset.orders();
    let readers = lock.read().unwrap();
    for order in readers.iter().rev() {
        let post_market_price = *close_prices.get(&order.symbol).unwrap();
        // FIXME: use accepted
        let profit = (post_market_price - order.created_price) * order.created_volume as f32;

        let level = if order.status == OrderStatus::LossPair || profit < 0.0 {
            log::Level::Warn
        } else {
            log::Level::Info
        };

        log::log!(level, "profit: {} for {:?}", profit, order);
        // FIXME: use accepted
        total_amount += order.created_price * order.created_volume as f32;
        total_profit += profit;
        if matches!(order.audit, AuditState::Loss) {
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

    info!("####################################################################################################");

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
            "constraint: {}={:?},{:?}/{:?}, [{}] ({} - {}) x {} + [{}] ({} - {}) x {} = {}",
            constraint,
            one.status,
            one.audit,
            another.audit,
            one.symbol,
            one_post_market_price,
            one.created_price,
            one.created_volume,
            another.symbol,
            another_post_market_price,
            another.created_price,
            another.created_volume,
            (one_post_market_price - one.created_price) * one.created_volume as f32
                + (another_post_market_price - another.created_price)
                    * another.created_volume as f32
        );
    }
    info!("closed prices {:?}", close_prices,);
    info!(
        "order count: {}, loss order: {}, total profit: {}, total amount: {}, rate: {:.5}%",
        readers.len(),
        loss_order,
        total_profit,
        total_amount,
        total_profit / total_amount * 100.0
    );

    info!("####################################################################################################");

    info!(
        "[Config] flash.loss_margin_rate: {:?}%",
        &config.trade.flash.loss_margin_rate * 100.0
    );
    for (index, rule) in config
        .trade
        .flash
        .rules
        .iter()
        .filter(|r| !r.evaluation)
        .enumerate()
    {
        info!(
            "########## [flash rule {} - {:?}] ##########",
            index, rule.mode
        );
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
                "[rule {}] LOWER, from: {:?}, to: {}, duration: {}",
                index, lower.from, lower.to, lower.duration
            );
        }
    }

    info!("------------------------------------------------------------------------");

    info!(
        "[Config] slug.loss_margin_rate: {:?}%",
        &config.trade.slug.loss_margin_rate * 100.0
    );
    for (index, rule) in config
        .trade
        .slug
        .rules
        .iter()
        .filter(|r| !r.evaluation)
        .enumerate()
    {
        info!(
            "########## [slug rule {} - {:?}] ##########",
            index, rule.mode
        );
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
                "[rule {}] LOWER, from: {:?}, to: {}, duration: {}",
                index, lower.from, lower.to, lower.duration
            );
        }
    }

    info!("####################################################################################################");
    info!("####################################################################################################");

    Ok(true) //FIXME:
}

// print details
pub fn print_meta(
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
    for (index, rule) in config.trade.flash.rules.iter().enumerate() {
        buffered.push(format!(
            "########## [flash rule {} - {:?}] ##########",
            index, rule.mode
        ));
        for trend in &rule.trends {
            buffered.push(format!(
                "[rule {}] TREND, from: {:?}, to: {}, trend: {:?}, up: {:?}, down: {:?}",
                index, trend.from, trend.to, trend.trend, trend.up, trend.down
            ));
        }
        for deviation in &rule.deviations {
            buffered.push(format!(
                "[rule {}] DEVIATION, from: {:?}, to: {}, value: {:.03}%",
                index,
                deviation.from,
                deviation.to,
                deviation.value * 100.0
            ));
        }
        for oscillation in &rule.oscillations {
            buffered.push(format!(
                "[rule {}] OSCILLATION, from: {:?}, to: {}, value: {:.03}%",
                index,
                oscillation.from,
                oscillation.to,
                oscillation.value * 100.0
            ));
        }
        for lower in &rule.lowers {
            buffered.push(format!(
                "[rule {}] LOWER, from: {:?}, to: {}, duration: {}",
                index, lower.from, lower.to, lower.duration
            ));
        }
    }

    buffered.push(format!(
        "------------------------------------------------------------------------"
    ));

    buffered.push(format!(
        "[Config] slug.loss_margin_rate: {:?}",
        &config.trade.slug.loss_margin_rate
    ));
    for (index, rule) in config.trade.slug.rules.iter().enumerate() {
        buffered.push(format!(
            "########## [slug rule {} - {:?}] ##########",
            index, rule.mode
        ));
        for trend in &rule.trends {
            buffered.push(format!(
                "[rule {}] TREND, from: {:?}, to: {}, trend: {:?}, up: {:?}, down: {:?}",
                index, trend.from, trend.to, trend.trend, trend.up, trend.down
            ));
        }
        for deviation in &rule.deviations {
            buffered.push(format!(
                "[rule {}] DEVIATION, from: {:?}, to: {}, value: {:.03}%",
                index,
                deviation.from,
                deviation.to,
                deviation.value * 100.0
            ));
        }
        for oscillation in &rule.oscillations {
            buffered.push(format!(
                "[rule {}] OSCILLATION, from: {:?}, to: {}, value: {:.03}%",
                index,
                oscillation.from,
                oscillation.to,
                oscillation.value * 100.0
            ));
        }
        for lower in &rule.lowers {
            buffered.push(format!(
                "[rule {}] LOWER, from: {:?}, to: {}, duration: {}",
                index, lower.from, lower.to, lower.duration
            ));
        }
    }

    buffered.push(format!(
        "------------------------------------------------------------------------"
    ));

    buffered.push(format!("{:?}", trade));

    if let Some(value) = order {
        buffered.push(format!(
            "----------------------------------- {} / {:?} / {:?} -----------------------------------",
            &value.id, &value.status, &value.audit
        ));
        buffered.push(format!("{:?}", value));
    }

    buffered.push(format!(
        "----------------------------------flash--------------------------------------"
    ));

    for (index, rule) in config.trade.flash.rules.iter().enumerate() {
        for trend_rule in &rule.trends {
            let mut result = true;
            if let Some(_from) = &trend_rule.from {
                // TODO
            } else {
                let rebound = rebound_at(&trend_rule.to, trade.states.get(&trend_rule.to).unwrap());
                let actual_trend = rebound.trend;
                let target_trend = &trend_rule.trend;

                if &actual_trend != target_trend {
                    result = false;
                }
                if !trend_rule.up_compare(rebound.up_count) {
                    result = false;
                }
                if !trend_rule.down_compare(rebound.down_count) {
                    result = false;
                }
            }
            buffered.push(format!(
                "[{}/{:?}] flash trend, from: {:?}, to: {}, trend: {:?}, up: {:?}, down: {:?} = {}",
                index,
                rule.mode,
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
                period_from = from[1..].parse::<usize>().unwrap() / flash::BASE_DURATION;
            }

            let base_unit = format!("m{:04}", flash::BASE_DURATION);
            // parse period from key (ex: m0070 => 70 / 10 = 7)
            let period_to = deviation_rule.to[1..].parse::<usize>().unwrap() / flash::BASE_DURATION;

            // min price
            let min_price = find_min_price(
                Arc::clone(&asset),
                &trade.id,
                &base_unit,
                period_from,
                period_to,
            );

            buffered.push(format!(
                "[{}/{:?}] flash min price, period: {:04} - {:04}, price: {}, min price: {}, rate {:.03}% < deviation {:.03}% = {}",
                index,
                rule.mode,
                period_from * flash::BASE_DURATION,
                period_to * flash::BASE_DURATION,
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
                period_from = from[1..].parse::<usize>().unwrap() / flash::BASE_DURATION;
            }

            // let oscillation = config.get_trade_oscillation("flash", &name).unwrap();
            let base_unit = format!("m{:04}", flash::BASE_DURATION);

            // parse period from key (ex: m0070 => 70 / 10 = 7)
            let period_to =
                oscillation_rule.to[1..].parse::<usize>().unwrap() / flash::BASE_DURATION;

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
                "[{}/{:?}] flash oscillation, period: {:04} - {:04}, max price: {}, min price: {}, rate {:.03}% > oscillation {:.03}% = {}",
                index,
                rule.mode,
                period_from * flash::BASE_DURATION,
                period_to * flash::BASE_DURATION,
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
                period_from = from[1..].parse::<usize>().unwrap() / flash::BASE_DURATION;
            }

            // let oscillation = config.get_trade_oscillation("flash", &name).unwrap();
            let base_unit = format!("m{:04}", flash::BASE_DURATION);

            // parse period from key (ex: m0070 => 70 / 10 = 7)
            let period_to = lower_rule.to[1..].parse::<usize>().unwrap() / flash::BASE_DURATION;

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
                        buffered.push(format!(
                            "[{}/{:?}] flash lower, period: {:04} - {:04}, min price: {}, last min price: {} at {} ({}s before)",
                            index,
                            rule.mode,
                            period_from * flash::BASE_DURATION,
                            period_to * flash::BASE_DURATION,
                            min_price,
                            last_protfolio.min_price,
                            Utc.timestamp_millis(last_time).format("%Y-%m-%d %H:%M:%s"),
                            (trade.time - last_time) / 1000
                        ));
                        continue;
                    }
                }
            };
            buffered.push(format!(
                "[{}/{:?}] flash lower, period: {:04} - {:04}, min price: {:?}, last min price: {:?} at {:?} ({:?}s before)",
                index,
                rule.mode,
                period_from * flash::BASE_DURATION,
                period_to * flash::BASE_DURATION,
                min_price,
                "",
                "",
                ""
            ));
        }

        let result = validate_audit_rule(
            Arc::clone(&asset),
            Arc::clone(&config),
            trade,
            rule,
            flash::BASE_DURATION,
        );
        buffered.push(format!(
            "[{}/{:?}] {}flash, result: {}, evaluate: {}{}",
            index,
            rule.mode,
            if result && !rule.evaluation {
                "********"
            } else {
                ""
            },
            result,
            rule.evaluation,
            if result && !rule.evaluation {
                "********"
            } else {
                ""
            },
        ));
    }

    buffered.push(format!(
        "---------------------------------slug---------------------------------------"
    ));

    for (index, rule) in config.trade.slug.rules.iter().enumerate() {
        for trend_rule in &rule.trends {
            let mut result = true;
            if let Some(_from) = &trend_rule.from {
                // TODO
            } else {
                let rebound = rebound_at(&trend_rule.to, trade.states.get(&trend_rule.to).unwrap());
                let actual_trend = rebound.trend;
                let target_trend = &trend_rule.trend;

                if &actual_trend != target_trend {
                    result = false;
                }
                if !trend_rule.up_compare(rebound.up_count) {
                    result = false;
                }
                if !trend_rule.down_compare(rebound.down_count) {
                    result = false;
                }
            }
            buffered.push(format!(
                "[{}/{:?}] slug trend, from: {:?}, to: {}, trend: {:?}, up: {:?}, down: {:?} = {}",
                index,
                rule.mode,
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
                period_from = from[1..].parse::<usize>().unwrap() / slug::BASE_DURATION;
            }

            let base_unit = format!("m{:04}", slug::BASE_DURATION);
            // parse period from key (ex: m0070 => 70 / 10 = 7)
            let period_to = deviation_rule.to[1..].parse::<usize>().unwrap() / slug::BASE_DURATION;

            // min price
            let min_price = find_min_price(
                Arc::clone(&asset),
                &trade.id,
                &base_unit,
                period_from,
                period_to,
            );

            buffered.push(format!(
                "[{}/{:?}] slug min price, period: {:04} - {:04}, price: {}, min price: {}, rate {:.03}% < deviation {:.03}% = {}",
                index,
                rule.mode,
                period_from * slug::BASE_DURATION,
                period_to * slug::BASE_DURATION,
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
                period_from = from[1..].parse::<usize>().unwrap() / slug::BASE_DURATION;
            }

            // let oscillation = config.get_trade_oscillation("flash", &name).unwrap();
            let base_unit = format!("m{:04}", slug::BASE_DURATION);

            // parse period from key (ex: m0070 => 70 / 10 = 7)
            let period_to =
                oscillation_rule.to[1..].parse::<usize>().unwrap() / slug::BASE_DURATION;

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
                "[{}/{:?}] slug oscillation, period: {:04} - {:04}, max price: {}, min price: {}, rate {:.03}% > oscillation {:.03}% = {}",
                index,
                rule.mode,
                period_from * slug::BASE_DURATION,
                period_to * slug::BASE_DURATION,
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
                period_from = from[1..].parse::<usize>().unwrap() / slug::BASE_DURATION;
            }

            // let oscillation = config.get_trade_oscillation("flash", &name).unwrap();
            let base_unit = format!("m{:04}", slug::BASE_DURATION);

            // parse period from key (ex: m0070 => 70 / 10 = 7)
            let period_to = lower_rule.to[1..].parse::<usize>().unwrap() / slug::BASE_DURATION;

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
                        buffered.push(format!(
                            "[{}/{:?}] slug lower, period: {:04} - {:04}, min price: {}, last min price: {} at {} ({}s before)",
                            index,
                            rule.mode,
                            period_from * slug::BASE_DURATION,
                            period_to * slug::BASE_DURATION,
                            min_price,
                            last_protfolio.min_price,
                            Utc.timestamp_millis(last_time).format("%Y-%m-%d %H:%M:%s"),
                            (trade.time - last_time) / 1000
                        ));
                        continue;
                    }
                }
            };
            buffered.push(format!(
                "[{}/{:?}] slug lower, period: {:04} - {:04}, min price: {:?}, last min price: {:?} at {:?} ({:?}s before)",
                index,
                rule.mode,
                period_from * slug::BASE_DURATION,
                period_to * slug::BASE_DURATION,
                min_price,
                "",
                "",
                ""
            ));
        }

        let result = validate_audit_rule(
            Arc::clone(&asset),
            Arc::clone(&config),
            trade,
            rule,
            flash::BASE_DURATION,
        );
        buffered.push(format!(
            "[{}/{:?}] {}slug, result: {}, evaluate: {}{}",
            index,
            rule.mode,
            if result && !rule.evaluation {
                "********"
            } else {
                ""
            },
            result,
            rule.evaluation,
            if result && !rule.evaluation {
                "********"
            } else {
                ""
            },
        ));
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

    let path = format!(
        "{base}/orders/{symbol}/{day}/MSG-{time}-{id}.ord",
        base = &config.replay.output.base_folder,
        symbol = &trade.id,
        day = Utc.timestamp_millis(trade.time).format("%Y-%m-%d"),
        time = Utc.timestamp_millis(trade.time).format("%Y%m%d%H%M%S"),
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
