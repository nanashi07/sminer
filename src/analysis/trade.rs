use crate::{
    analysis::debug::print_meta,
    persist::grafana::add_order_annotation,
    vo::{
        biz::{
            AuditState, MarketHoursType, Order, PricePair, TotalProfit, TradeInfo, TradeTrend,
            TradeTrendInfo, Trend,
        },
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
use rsc::{
    computer::Computer,
    lexer,
    parser::{self, Expr},
};
use std::{f64::NAN, sync::Arc};

pub fn prepare_trade(
    asset: Arc<AssetContext>,
    config: Arc<AppConfig>,
    trade: &TradeInfo,
) -> Result<()> {
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
        | AuditState::EarlyClear => {
            // calculate volume
            let estimated_volume = calculate_volum(Arc::clone(&asset), Arc::clone(&config), &trade);

            if config.trade.get_option(&trade.id).ignore_zero_order && estimated_volume == 0 {
                warn!("estimated order volume is zero, ignore order");
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
                estimated_volume,
                trade.action_time(),
                state.clone(),
            );

            if asset.add_order(order.clone()) {
                // for debug only
                if true {
                    let rival_symbol = asset.find_rival_symbol(&trade.id).unwrap();
                    let rival_trade = asset.get_latest_trade(&rival_symbol).unwrap();
                    let (_, estimated_min_balance) = validate_total_profit(
                        Arc::clone(&asset),
                        Arc::clone(&config),
                        &trade,
                        &rival_trade,
                        estimated_volume,
                    );

                    info!(
                            "after order: [{}] {:<12} price: {:<10} rival price: {:<10} volume: {:<4} balance: {}",
                            &order.symbol,
                            format!("{:?}", &order.audit),
                            format!("{},",order.created_price),
                            format!("{},",order.created_rival_price),
                            format!("{},",order.created_volume),
                            estimated_min_balance
                        );
                }

                let order_id = order.id.clone();

                if config.replay.export_enabled("order") {
                    print_meta(
                        Arc::clone(&asset),
                        Arc::clone(&config),
                        Some(order.clone()),
                        &trade,
                        config.replay.export_enabled("message"),
                        config.replay.export_enabled("order"),
                    )
                    .unwrap_or_default();
                }

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
                add_order_annotation(Arc::clone(&config), time, "Place order".to_owned(), tags)
                    .unwrap();
            }
        }
        AuditState::LossClear | AuditState::LossBound | AuditState::CloseTrade => {
            // get latest rival ticker
            let symbol = &trade.id;
            let rival_symbol = asset.find_rival_symbol(symbol).unwrap();
            let time = trade.time;

            // replace with rival latest trade
            let mut rival_trade = asset.get_latest_trade(&rival_symbol).unwrap();
            rival_trade.time = time + 1;

            // calculate volume
            let estimated_volume =
                calculate_volum(Arc::clone(&asset), Arc::clone(&config), &rival_trade);

            if estimated_volume == 0 {
                warn!("estimated order volume is zero, ignore order");
                return Ok(());
            }

            // get rival price
            let mut rival_price = f32::NAN;
            if let Some(rival_ticker) = asset.get_latest_rival_ticker(&rival_trade.id) {
                rival_price = rival_ticker.price;
            }

            // create order
            let order = Order::new(
                &rival_trade.id,
                rival_trade.price,
                rival_price,
                estimated_volume,
                rival_trade.action_time(),
                state.clone(),
            );

            if asset.add_order(order.clone()) {
                // for debug only
                if true {
                    let (_, estimated_min_balance) = validate_total_profit(
                        Arc::clone(&asset),
                        Arc::clone(&config),
                        &rival_trade,
                        &trade,
                        estimated_volume,
                    );

                    info!(
                            "after order: [{}] {:<12} price: {:<10} rival price: {:<10} volume: {:<4} balance: {}",
                            &order.symbol,
                            format!("{:?}", &order.audit),
                            format!("{},",order.created_price),
                            format!("{},",order.created_rival_price),
                            format!("{},",order.created_volume),
                            estimated_min_balance
                        );
                }

                let order_id = order.id.clone();

                if config.replay.export_enabled("order") {
                    print_meta(
                        Arc::clone(&asset),
                        Arc::clone(&config),
                        Some(order.clone()),
                        &rival_trade,
                        config.replay.export_enabled("message"),
                        config.replay.export_enabled("order"),
                    )
                    .unwrap_or_default();
                }

                let time = Utc.timestamp_millis(rival_trade.action_time());
                let tags = vec![
                    rival_trade.id.clone(),
                    order_id,
                    format!("{:?}", &state),
                    format!("MSG-{}", &rival_trade.message_id),
                    format!("${}", &order.created_price),
                    format!("v{}", &order.created_volume),
                ];

                // take loss previouse order
                asset.realized_loss(&order);

                // add grafana annotation
                add_order_annotation(Arc::clone(&config), time, "Place order".to_owned(), tags)
                    .unwrap();
            }
        }
        AuditState::Decline => {}
    }

    Ok(())
}

pub fn calculate_volum(asset: Arc<AssetContext>, config: Arc<AppConfig>, trade: &TradeInfo) -> u32 {
    // restricted amount
    let max_amount = config.trade.get_option(&trade.id).max_order_amount;
    // get opposite
    let rival_symbol = asset.find_rival_symbol(&trade.id).unwrap();
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
        if let Some(rival_symbol) = asset.find_rival_symbol(&trade.id) {
            if let Some(rival_trade) = asset.get_latest_trade(&rival_symbol) {
                rival_current_price = rival_trade.price;
            }
        }
        if rival_current_price.is_nan() {
            warn!("rival price not available, skip order {:?}", trade);
            return 0;
        }

        let rival_price_change_rate = (rival_current_price - rival_last_price) / rival_last_price;
        let mut price_change_rate = (current_price - last_price) / last_price;

        // 1. when both bull/bear upward or bull/bear downward, result volume is negative
        // 2. when change rate between bulk and bear is too high/low, result volume is too large (allow max to 3x)
        // use estimated last price for calculation
        if rival_price_change_rate * price_change_rate > 0.0
            || (rival_price_change_rate / price_change_rate).abs() > 3.0
            || (price_change_rate / rival_price_change_rate).abs() > 3.0
        {
            debug!(
                "rival_price_change_rate * price_change_rate = {} * {} = {} > 0.0 : {}",
                rival_price_change_rate,
                price_change_rate,
                rival_price_change_rate * price_change_rate,
                rival_price_change_rate * price_change_rate > 0.0
            );
            debug!(
                "(rival_price_change_rate / price_change_rate).abs() = abs({} / {}) = {} > 3.0 : {}",
                rival_price_change_rate,
                price_change_rate,
                (rival_price_change_rate / price_change_rate).abs(),
                (rival_price_change_rate / price_change_rate).abs() > 3.0
            );
            debug!(
                "(price_change_rate / rival_price_change_rate).abs() = abs({} / {}) = {} > 3.0 : {}",
                price_change_rate,
                rival_price_change_rate,
                (price_change_rate / rival_price_change_rate).abs() ,
                (price_change_rate / rival_price_change_rate).abs() > 3.0
            );
            let rival_change_rate = (rival_current_price - rival_last_price) / rival_last_price;
            let change_rate = rival_change_rate * -1.0;
            // change_rate = (current_price - estimated_last_price) / estimated_last_price
            // => estimated_last_price + change_rate * estimated_last_price = current_price
            // => estimated_last_price = current_price / (1.0 + change_rate)
            last_price = current_price / (1.0 + change_rate);
            price_change_rate = (current_price - last_price) / last_price;
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
            info!("price_change_rate = {}", price_change_rate);

            info!(
                "chagne rate diff: {} / {} = {}, {} / {} = {}",
                rival_price_change_rate,
                price_change_rate,
                rival_price_change_rate / price_change_rate,
                price_change_rate,
                rival_price_change_rate,
                price_change_rate / rival_price_change_rate
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
            expected_volume.floor() as u32
        } else {
            expected_volume.ceil() as u32
        }
    } else {
        let estimated_volumn = (max_amount as f32) / trade.price;
        estimated_volumn.round() as u32
    }
}

pub fn audit_trade(
    asset: Arc<AssetContext>,
    config: Arc<AppConfig>,
    trade: &TradeInfo,
) -> AuditState {
    // for debug, print period meta
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
            print_meta(
                Arc::clone(&asset),
                Arc::clone(&config),
                None,
                trade,
                true,
                true,
            )
            .unwrap_or_default();
        }
    }

    let mut result = AuditState::Decline;
    let option = config.trade.get_option(&trade.id);

    // flash check
    if flash::audit(Arc::clone(&asset), Arc::clone(&config), trade) {
        debug!(
            "[{}] flash in, price = {}, profit check off",
            &trade.id, &trade.price
        );
        result = AuditState::Flash;
    }

    let mut _last_profit_positive: Option<bool> = None;
    let mut _last_estimated_min_profit: Option<f64> = None;
    let mut _profit_positive: Option<bool> = None;
    let mut _estimated_min_profit: Option<f64> = None;
    let estimated_volume = calculate_volum(Arc::clone(&asset), Arc::clone(&config), trade);
    let mut profit_increased: Option<bool> = None;

    let rival_symbol_option = asset.find_rival_symbol(&trade.id);
    let mut rival_trade_option: Option<TradeInfo> = None;

    if let Some(rival_symbol) = &rival_symbol_option {
        // get latest trade of rival ticker
        rival_trade_option = asset.get_latest_trade(&rival_symbol);
        if let Some(rival_trade) = &rival_trade_option {
            let (last_valid, last_min) = validate_total_profit(
                Arc::clone(&asset),
                Arc::clone(&config),
                trade,
                &rival_trade,
                0,
            );
            let (valid, min) = validate_total_profit(
                Arc::clone(&asset),
                Arc::clone(&config),
                trade,
                &rival_trade,
                estimated_volume,
            );

            _last_profit_positive = Some(last_valid);
            _last_estimated_min_profit = Some(last_min);
            _profit_positive = Some(valid);
            _estimated_min_profit = Some(min);
            profit_increased = Some(min > last_min);
        }
    }

    // slug check
    if slug::audit(Arc::clone(&asset), Arc::clone(&config), trade) {
        debug!("[{}] slug in, price = {}", &trade.id, &trade.price);
        // TODO: check when last min > 0
        result = AuditState::Slug;
    }

    // check last pair order if exists, make sure make off gain profit
    if option.validate_increased_profit || option.enable_profit_take || option.enable_early_clear {
        if let Some(rival_symbol) = &rival_symbol_option {
            // get latest trade of rival ticker
            if let Some(rival_trade) = &rival_trade_option {
                if let Some(rival_order) = asset.find_running_order(&rival_symbol) {
                    // rival symbol is the one has been placed order
                    let rival_price_change = rival_trade.price - rival_order.created_price;
                    let rival_price_change_rate = rival_price_change / rival_order.created_price;
                    let rival_volume = rival_order.created_volume as f32;
                    let rival_profit = rival_price_change * rival_volume;

                    let price_change = trade.price - rival_order.created_rival_price;
                    let price_change_rate = price_change / rival_order.created_rival_price;

                    let estimated_profit = price_change * estimated_volume as f32;

                    if matches!(result, AuditState::Flash | AuditState::Slug) {
                        if option.validate_increased_profit && !profit_increased.unwrap_or(true) {
                            debug!("[{}] block write off, price = {}", &trade.id, &trade.price);
                            result = AuditState::Decline;
                        }
                    } else {
                        if rival_price_change_rate > option.early_clear_rate.abs()
                            && profit_increased.unwrap_or(true)
                        {
                            // early sell even if there is no match rule found
                            if option.enable_profit_take
                                && (rival_price_change_rate > option.profit_take_rate.abs()
                                    || price_change_rate < -1.0 * option.profit_take_rate.abs())
                            {
                                debug!("[{}] take profit, price = {}", &trade.id, &trade.price);
                                trace!(
                                "rival change rate: {rival_price_change_rate:.5}%, change rate: {price_change_rate:.5}%, change deviation: {change_deviation}%, rival profit: {rival_profit}, estimated profit: {estimated_profit} estimated volume: {estimated_volume}, total profit: {total_profit}",
                                rival_price_change_rate = rival_price_change_rate * 100.0,
                                price_change_rate = price_change_rate * 100.0,
                                change_deviation = (rival_price_change_rate.abs() - price_change_rate.abs()) / rival_price_change_rate.abs(),
                                rival_profit = rival_profit,
                                estimated_profit = estimated_profit,
                                estimated_volume = estimated_volume,
                                total_profit = rival_profit + estimated_profit
                            );
                                result = AuditState::ProfitTaking;
                            }
                            // early sell when the trend is starting to go down
                            else if option.enable_early_clear
                                && revert::audit(Arc::clone(&asset), Arc::clone(&config), &trade)
                            {
                                debug!(
                                    "[{}] early clear, price = {}, change rate = {:.5}/{:.5}",
                                    &trade.id,
                                    &trade.price,
                                    rival_price_change_rate,
                                    price_change_rate,
                                );
                                trace!(
                                "rival change rate: {rival_price_change_rate:.5}%, change rate: {price_change_rate:.5}%, change deviation: {change_deviation}%, rival profit: {rival_profit}, estimated profit: {estimated_profit} estimated volume: {estimated_volume}, total profit: {total_profit}",
                                rival_price_change_rate = rival_price_change_rate * 100.0,
                                price_change_rate = price_change_rate * 100.0,
                                change_deviation = (rival_price_change_rate.abs() - price_change_rate.abs()) / rival_price_change_rate.abs(),
                                rival_profit = rival_profit,
                                estimated_profit = estimated_profit,
                                estimated_volume = estimated_volume,
                                total_profit = rival_profit + estimated_profit
                            );
                                result = AuditState::EarlyClear;
                            }
                        }
                    }
                }
            }
        }
    }
    // TODO: consider calculate diff between bear change rate / bulk change rate

    // TODO: reutrn if decline, unnecessary to check following

    // FIXME: check previous order status
    if option.enable_loss_clear {
        if let Some(exists_order) = asset.find_running_order(&trade.id) {
            if recognize_loss(
                Arc::clone(&asset),
                Arc::clone(&config),
                trade,
                &exists_order,
            ) {
                // TODO: do more trend check here
                debug!(
                    "[{}] loss clear, price = {}, profit check off",
                    &trade.id, &trade.price
                );
                return AuditState::LossClear;
            }
            result = AuditState::Decline;
        }
    }

    // check close time and buy last one if unpair order exists
    if option.enable_close_trade {
        if asset.regular_marketing_closing(trade.action_time()) {
            let symbol = &trade.id;
            if let Some(rival_symbol) = asset.find_rival_symbol(&symbol) {
                if let Some(target_symbol) =
                    asset.has_running_orders(&vec![symbol.clone(), rival_symbol.clone()])
                {
                    // only close same side symbol, rival symbol must wait for their trade
                    if symbol == &target_symbol {
                        debug!(
                        "[{}] close clear, price = {}, profit check off, it's time to take rest",
                        &trade.id, &trade.price
                    );
                        return AuditState::CloseTrade;
                    }
                } else {
                    // no running orders, decline all incoming orders when marketing is closing
                    return AuditState::Decline;
                }
            }
        }
    }

    // 區間內與最大值的價差（比率）
    // 與反向 eft 的利差（數值）

    result
}

// see 'rate_calc.xlsx' in doc
fn validate_total_profit(
    asset: Arc<AssetContext>,
    _config: Arc<AppConfig>,
    trade: &TradeInfo,
    rival_trade: &TradeInfo,
    estimated_volume: u32,
) -> (bool, f64) {
    // pair orders
    let symbol = &trade.id;
    let rival_symbol = &rival_trade.id;

    let orders = asset.find_orders_by_symbol(&vec![symbol.to_owned(), rival_symbol.to_owned()]);
    // formulat for calculate balance
    let formula = generate_formula(&orders, symbol, trade.price, estimated_volume);

    if formula.is_empty() {
        info!("formula is empty, order size = {}", &orders.len());
        return (true, -1.0);
    }

    let results = spread_results(trade, rival_trade, symbol, rival_symbol, &formula);
    let valid = !results.iter().any(|v| v.result < 0.0);
    let estimated_min_balance = results.iter().map(|v| v.result).reduce(f64::min).unwrap();

    (valid, estimated_min_balance)
}

fn generate_formula(orders: &Vec<Order>, symbol: &str, price: f32, volume: u32) -> String {
    let mut formula: Vec<String> = Vec::new();

    for order in orders {
        // formula
        formula.push(format!(
            "({} - {}) * {}",
            order.symbol, order.created_price, order.created_volume
        ));
    }

    // coming order
    formula.push(format!("({} - {}) * {}", symbol, price, volume));

    formula.join(" + ")
}

fn spread_results(
    trade: &TradeInfo,
    rival_trade: &TradeInfo,
    symbol: &str,
    rival_symbol: &str,
    formula: &str,
) -> Vec<TotalProfit> {
    // calculate formula
    let tokens = lexer::tokenize(&formula, true).unwrap();
    let mut ast = parser::parse(&tokens).unwrap();
    let mut computer = Computer::<f64>::default();

    let mut price_pairs: Vec<PricePair> = Vec::new();
    let mut results: Vec<TotalProfit> = Vec::new();

    // calculate current first, mark offset flag
    ast.replace(
        &Expr::Identifier(symbol.to_owned()),
        &Expr::Constant(trade.price as f64),
        false,
    );
    ast.replace(
        &Expr::Identifier(rival_symbol.to_owned()),
        &Expr::Constant(rival_trade.price as f64),
        false,
    );

    let value = computer.compute(&ast).unwrap();

    results.push(TotalProfit::new(
        symbol.to_owned(),
        trade.price,
        rival_symbol.to_owned(),
        rival_trade.price,
        value,
        false,
    ));

    let mut price = trade.price;
    let mut rival_price = rival_trade.price;
    for _ in 1..=100 {
        price = price + price * 0.003;
        rival_price = rival_price - rival_price * 0.003;
        price_pairs.push(PricePair {
            current: price,
            rival: rival_price,
        })
    }

    let mut price = trade.price;
    let mut rival_price = rival_trade.price;
    for _ in 1..=100 {
        price = price - price * 0.003;
        rival_price = rival_price + rival_price * 0.003;
        price_pairs.push(PricePair {
            current: price,
            rival: rival_price,
        })
    }

    for price_pair in price_pairs {
        ast.replace(
            &Expr::Identifier(symbol.to_owned()),
            &Expr::Constant(price_pair.current as f64),
            false,
        );
        ast.replace(
            &Expr::Identifier(rival_symbol.to_owned()),
            &Expr::Constant(price_pair.rival as f64),
            false,
        );

        let value = computer.compute(&ast).unwrap();

        results.push(TotalProfit::new(
            symbol.to_owned(),
            price_pair.current,
            rival_symbol.to_owned(),
            price_pair.rival,
            value,
            true,
        ));
    }

    results
}

fn recognize_loss(
    _asset: Arc<AssetContext>,
    config: Arc<AppConfig>,
    trade: &TradeInfo,
    order: &Order,
) -> bool {
    if let Some(margin_rate) = match order.audit {
        AuditState::Flash => config.trade.flash.loss_margin_rate,
        AuditState::Slug => config.trade.slug.loss_margin_rate,
        _ => Some(0.006), // not affected
    } {
        let price = trade.price;
        // FIXME : use accepted price
        let order_price = order.created_price;

        // check if loss over than configured margin value
        price < order_price && (order_price - price) / order_price > margin_rate
    } else {
        false
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

// debug only
pub fn rebound_all(trade: &TradeInfo) -> Vec<TradeTrendInfo> {
    trade
        .states
        .iter()
        .map(|(key, values)| rebound(&key, 0, &values))
        .collect::<Vec<TradeTrendInfo>>()
}

pub fn rebound(unit: &str, from: usize, slopes: &Vec<f64>) -> TradeTrendInfo {
    let mut trend = Trend::Upward;
    let mut count = 0;
    let mut last_slope = f64::NAN;
    let mut sections: Vec<u32> = Vec::new();

    for (index, value) in slopes.iter().skip(from).enumerate() {
        let slope = *value;

        if index == 0 && slope < 0.0 {
            trend = Trend::Downward;
        }

        // when first value or same side value (both positive or both negative)
        if last_slope.is_nan() || last_slope * slope > 0.0 {
            count = count + 1;
        }
        // value not in same side
        else {
            sections.push(count);
            count = 1;
        }
        last_slope = slope;
    }

    // push last section
    sections.push(count);

    TradeTrendInfo {
        unit: unit.to_string(),
        trend,
        sections,
    }
}

#[deprecated(note = "Use rebound instead")]
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
) -> bool {
    let duration: usize = 10;

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
            return false;
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
                    return false;
                }
                if !trend_rule.down_compare(second_section) {
                    return false;
                }
            }
            Trend::Downward => {
                if !trend_rule.down_compare(first_section) {
                    return false;
                }
                if !trend_rule.up_compare(second_section) {
                    return false;
                }
            }
        }
    }

    true
}

pub fn get_min_duration(duration: usize, keys: &Vec<String>) -> Result<String> {
    let mut values: Vec<usize> = keys
        .iter()
        .map(|key| key[1..].parse::<usize>().unwrap())
        .collect();

    values.sort();
    values.reverse();
    for value in values {
        if duration % value == 0 {
            return Ok(format!("m{:04}", value));
        }
    }
    panic!("No available minimal duration found");
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

        // get min price from wider range
        let min_price = find_min_price(
            Arc::clone(&asset),
            &trade.id,
            &base_unit,
            period_from,
            period_to,
        );

        // find price time lower than min_price before
        if min_price.is_normal() {
            let recent_period_to = lower_rule.compare_to[1..].parse::<usize>().unwrap() / duration;
            let recent_min_price = find_min_price(
                Arc::clone(&asset),
                &trade.id,
                &base_unit,
                0,
                recent_period_to,
            );

            if !recent_min_price.is_normal() || recent_min_price > min_price {
                return false;
            }
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

    pub fn audit(asset: Arc<AssetContext>, config: Arc<AppConfig>, trade: &TradeInfo) -> bool {
        let mut results: Vec<bool> = Vec::new();

        let use_symbol_rule = config
            .trade
            .flash
            .rules
            .iter()
            .filter(|r| r.symbols.contains(&trade.id))
            .filter(|r| !r.evaluation)
            .any(|_| true);

        // general validation from config rules, at least one success and no blocked rule
        for rule in config.trade.flash.rules.iter().filter(|r| !r.evaluation) {
            if (use_symbol_rule && !rule.symbols.contains(&trade.id))
                || (!use_symbol_rule && !rule.symbols.is_empty())
            {
                continue;
            }

            if validate_audit_rule(Arc::clone(&asset), Arc::clone(&config), trade, rule) {
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

    pub fn audit(asset: Arc<AssetContext>, config: Arc<AppConfig>, trade: &TradeInfo) -> bool {
        let mut results: Vec<bool> = Vec::new();

        let use_symbol_rule = config
            .trade
            .slug
            .rules
            .iter()
            .filter(|r| r.symbols.contains(&trade.id))
            .filter(|r| !r.evaluation)
            .any(|_| true);

        // general validation from config rules, at least one success and no blocked rule
        for rule in config.trade.slug.rules.iter().filter(|r| !r.evaluation) {
            if (use_symbol_rule && !rule.symbols.contains(&trade.id))
                || (!use_symbol_rule && !rule.symbols.is_empty())
            {
                continue;
            }

            if validate_audit_rule(Arc::clone(&asset), Arc::clone(&config), trade, rule) {
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

    pub fn audit(asset: Arc<AssetContext>, config: Arc<AppConfig>, trade: &TradeInfo) -> bool {
        let mut results: Vec<bool> = Vec::new();

        // general validation from config rules, at least one success and no blocked rule
        for rule in config.trade.revert.rules.iter().filter(|r| !r.evaluation) {
            if validate_audit_rule(Arc::clone(&asset), Arc::clone(&config), trade, rule) {
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
