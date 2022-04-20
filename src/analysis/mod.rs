mod computor;
mod debug;
pub mod trade;

use self::trade::prepare_trade;
use crate::{
    analysis::{computor::draw_slop_lines, debug::profit_evaluate},
    persist::es::{
        bulk_index, protfolio_index_name, slope_index_name, take_index_time, trade_index_name,
        ElasticTicker, ElasticTrade,
    },
    vo::{
        biz::{MarketHoursType, Protfolio, Ticker, TimeUnit, TradeInfo},
        core::AppContext,
    },
    Result,
};
use chrono::{TimeZone, Utc};
use log::{debug, error, info, trace, warn};
use std::{
    collections::HashMap,
    fs::{create_dir_all, remove_dir_all, File, OpenOptions},
    io::{BufRead, BufReader, BufWriter, Write},
    path::Path,
    sync::Arc,
    thread::sleep,
    time::Duration,
};
use tokio::sync::RwLock;

pub async fn init_dispatcher(context: &Arc<AppContext>) -> Result<()> {
    let config = context.config();

    if config.sync_mongo_enabled() {
        handle_message_for_mongo(Arc::clone(&context)).await?;
    }

    if config.sync_elasticsearch_enabled() {
        handle_message_for_elasticsearch(Arc::clone(&context)).await?;
    }

    if config.trade.enabled {
        handle_message_for_preparatory(Arc::clone(&context)).await?;

        handle_message_for_calculator(Arc::clone(&context)).await?;

        handle_message_for_trade(Arc::clone(&context)).await?;
    }

    Ok(())
}

async fn handle_message_for_mongo(context: Arc<AppContext>) -> Result<()> {
    info!("Initialize mongo event persist handler");
    let post_man = context.post_man();
    let mut rx = post_man.subscribe_store();

    let buffer: Arc<RwLock<Vec<Ticker>>> = Arc::new(RwLock::new(Vec::new()));
    let temp = Arc::clone(&buffer);

    tokio::spawn(async move {
        debug!("Initialize mongo event persist handler - receiver");
        loop {
            match rx.recv().await {
                Ok(event) => {
                    let ticker: Ticker = event.into();
                    let mut guard = temp.write().await;
                    guard.push(ticker);
                }
                Err(err) => error!("Handle ticker for mongo error: {:?}", err),
            }
        }
    });

    let persist = context.persistence();
    let temp = Arc::clone(&buffer);

    tokio::spawn(async move {
        debug!("Initialize mongo event persist handler - processor");
        loop {
            let mut guard = temp.write().await;
            if let Some(event) = guard.pop() {
                let ticker: Ticker = event.into();
                if let Err(err) = ticker.save_to_mongo(Arc::clone(&persist)).await {
                    error!("Save ticker for mongo error: {:?}", err)
                }
            } else {
                // avoid busy wait
                std::thread::sleep(std::time::Duration::from_millis(100));
            }
        }
    });

    Ok(())
}

async fn handle_message_for_elasticsearch(context: Arc<AppContext>) -> Result<()> {
    info!("Initialize elasticsearch event persist handler");
    let post_man = context.post_man();
    let mut rx = post_man.subscribe_store();

    let buffer: Arc<RwLock<Vec<ElasticTicker>>> = Arc::new(RwLock::new(Vec::new()));
    let temp = Arc::clone(&buffer);

    tokio::spawn(async move {
        debug!("Initialize elasticsearch event persist handler - receiver");
        loop {
            match rx.recv().await {
                Ok(event) => {
                    let ticker: ElasticTicker = event.into();
                    let mut guard = temp.write().await;
                    guard.push(ticker);
                }
                Err(err) => error!("Handle ticker for elasticsearch error: {:?}", err),
            }
        }
    });

    let persist = context.persistence();
    let temp = Arc::clone(&buffer);

    tokio::spawn(async move {
        debug!("Initialize elasticsearch event persist handler - processor");
        loop {
            let mut guard = temp.write().await;
            let mut items: Vec<ElasticTicker> = Vec::new();
            while let Some(item) = guard.pop() {
                items.push(item);
            }
            std::mem::drop(guard);

            if !items.is_empty() {
                if let Err(err) =
                    ElasticTicker::batch_save_to_elasticsearch(Arc::clone(&persist), &items).await
                {
                    error!("Save ticker for elasticsearch error: {:?}", err);
                }
            } else {
                // avoid busy wait
                std::thread::sleep(std::time::Duration::from_millis(100));
            }
        }
    });

    Ok(())
}

async fn handle_message_for_preparatory(ctx: Arc<AppContext>) -> Result<()> {
    info!("Initialize event preparatory handler");
    let post_man = ctx.post_man();
    let mut rx = post_man.subscribe_prepare();

    let buffer: Arc<RwLock<Vec<Ticker>>> = Arc::new(RwLock::new(Vec::new()));
    let temp = Arc::clone(&buffer);

    tokio::spawn(async move {
        debug!("Initialize event preparatory handler - receiver");
        loop {
            match rx.recv().await {
                Ok(event) => {
                    let ticker: Ticker = event.into();
                    let mut guard = temp.write().await;
                    guard.push(ticker);
                }
                Err(err) => {
                    error!("Handle ticker for preparatory error: {:?}", err);
                }
            }
        }
    });

    let context = Arc::clone(&ctx);
    let temp = Arc::clone(&buffer);

    tokio::spawn(async move {
        debug!("Initialize event preparatory handler - processor");
        loop {
            let mut guard = temp.write().await;
            if let Some(event) = guard.pop() {
                let ticker: Ticker = event.into();
                // Add into source list
                if let Some(lock) = context.asset().symbol_tickers(&ticker.id) {
                    if let Ok(mut guard) = lock.write() {
                        guard.push_front(ticker.clone());
                    } else {
                        error!("get mutable tickers error: {}", &ticker.id);
                        continue;
                    }
                } else {
                    error!("No tickers container {} initialized", &ticker.id);
                    continue;
                }

                // Add ticker decision data first (id/time... with empty analysis data)
                let asset = context.asset();
                let config = context.config();
                let units = config.time_units();
                let message_id = asset.next_message_id();
                // only take moving data
                let unit_size = units.iter().filter(|u| u.period > 0).count();

                let trade = TradeInfo::from(&ticker, message_id, unit_size, false);
                asset.add_trade(&ticker.id, trade);

                // Send signal for symbol analysis
                if let Err(err) = context.post_man().calculate(&ticker.id, message_id) {
                    error!("send to calculate error: {}", err);
                }
            } else {
                // avoid busy wait
                std::thread::sleep(std::time::Duration::from_millis(100));
            }
        }
    });

    Ok(())
}

async fn handle_message_for_calculator(ctx: Arc<AppContext>) -> Result<()> {
    info!("Initialize event calculator handler");
    let config = ctx.config();
    let post_man = ctx.post_man();

    for unit in config.time_units() {
        for symbol in config.symbols() {
            debug!(
                "Initialize event calculate {} for {:?} handler",
                &symbol, unit
            );
            let mut rx = post_man.subscribe_calculate(&symbol);
            let unit = unit.clone();

            let buffer: Arc<RwLock<Vec<i64>>> = Arc::new(RwLock::new(Vec::new()));
            let temp = Arc::clone(&buffer);

            let symbol_name = symbol.to_string();
            let unit_name = unit.name.to_string();

            tokio::spawn(async move {
                debug!(
                    "Initialize event calculator handler - receiver : {}/{}",
                    &symbol_name, &unit_name
                );
                loop {
                    // Receive message ID only
                    match rx.recv().await {
                        Ok(message_id) => {
                            let mut guard = temp.write().await;
                            guard.push(message_id);
                        }
                        Err(err) => error!("Handle ticker for calculator error: {:?}", err),
                    }
                }
            });

            let context = Arc::clone(&ctx);
            let temp = Arc::clone(&buffer);
            let symbol_name = symbol.to_string();
            let unit_name = unit.name.to_string();

            tokio::spawn(async move {
                debug!(
                    "Initialize event calculator handler - processor: {}/{}",
                    &symbol_name, &unit_name
                );
                loop {
                    let mut guard = temp.write().await;
                    if let Some(message_id) = guard.pop() {
                        trace!("handle_message_for_calculator: {:?} of {}", unit, symbol);
                        // route to calculation
                        if let Err(err) = context.route(message_id, &symbol, &unit) {
                            error!("route calculation error: {:?}", err);
                            continue;
                        }

                        // check all values finalized then push to prepare trade
                        if context.config().trade.enabled
                            && context.asset().is_trade_finalized(&symbol, message_id)
                        {
                            debug!(
                                "Prepare to handle finalized trade info, symbol: {}, message_id: {}",
                                symbol, &message_id
                            );
                            if let Err(err) = context.post_man().watch_trade(message_id).await {
                                error!("send calculate resoult for trade error: {:?}", err);
                            }
                        }
                    } else {
                        // avoid busy wait
                        std::thread::sleep(std::time::Duration::from_millis(100));
                    }
                }
            });
        }
    }

    Ok(())
}

async fn handle_message_for_trade(ctx: Arc<AppContext>) -> Result<()> {
    info!("Initialize event trade handler");
    let post_man = ctx.post_man();
    let mut rx = post_man.subscribe_trade();

    let mut map: HashMap<String, RwLock<Vec<TradeInfo>>> = HashMap::new();

    for symbol in ctx.config().symbols() {
        map.insert(symbol, RwLock::new(Vec::new()));
    }

    let buffer: Arc<HashMap<String, RwLock<Vec<TradeInfo>>>> = Arc::new(map);
    let temp = Arc::clone(&buffer);
    let asset = ctx.asset();

    tokio::spawn(async move {
        debug!("Initialize event trade handler - processor");
        loop {
            match rx.recv().await {
                Ok(message_id) => {
                    let mut result: Option<TradeInfo> = None;
                    if let Some(lock) = asset.search_trade(message_id) {
                        if let Ok(trade) = lock.read() {
                            result = Some(trade.to_owned());
                        }
                    }

                    if let Some(trade) = result {
                        if let Some(lock) = temp.get(&trade.id) {
                            let mut guard = lock.write().await;
                            guard.push(trade);
                        }
                    } else {
                        warn!("No trade info for message ID: {} found!", &message_id);
                    }
                }
                Err(err) => error!("Handle message for prepare trade error: {:?}", err),
            }
        }
    });

    for symbol in ctx.config().symbols() {
        let temp = Arc::clone(&buffer);

        let context = Arc::clone(&ctx);

        tokio::spawn(async move {
            debug!("Initialize event trade handler - processor");
            loop {
                if let Some(lock) = temp.get(&symbol) {
                    let mut guard = lock.write().await;
                    if let Some(value) = guard.pop() {
                        if let Err(err) = prepare_trade(context.asset(), context.config(), &value) {
                            error!("Prepare trade error: {:?}", err);
                        }
                    } else {
                        // avoid busy wait
                        std::thread::sleep(std::time::Duration::from_millis(100));
                    }
                }
            }
        });
    }

    Ok(())
}

impl AppContext {
    pub fn route(&self, message_id: i64, symbol: &str, unit: &TimeUnit) -> Result<()> {
        debug!(
            "========== Route calculation for {} of {}, message_id: {} ==========",
            symbol, unit.duration, &message_id
        );

        let asset = self.asset();

        if let Some(lock) = asset.get_protfolios(symbol, &unit.name) {
            debug!(
                "Handle calculation for {} of {}, message_id: {}",
                symbol, unit.duration, &message_id
            );

            // Get ticker source
            let tickers = asset.symbol_tickers(symbol).unwrap();
            let symbol_tickers = tickers.read().unwrap();

            // Get target protfolios
            let mut protfolios = lock.write().unwrap();

            // Get target trade info
            let trade_lock = asset.find_trade(symbol, message_id).unwrap();

            // Start calculation
            unit.rebalance(
                symbol,
                message_id,
                &symbol_tickers,
                &mut protfolios,
                trade_lock,
            )?;
        } else {
            error!(
                "Not protfolios container {} of {} initialized",
                unit.name, symbol
            );
        }
        Ok(())
    }
}

#[derive(Debug, PartialEq, Eq)]
pub enum ReplayMode {
    // Sync mode for normal replay
    Sync,
    // Async mode for dispatch test
    Async { delay: u64 },
}

pub async fn replay(context: &AppContext, file: &str, mode: ReplayMode) -> Result<()> {
    info!("Loading tickers: {}", file);

    let config = context.config();
    let asset = context.asset();

    let f = File::open(file)?;
    let reader = BufReader::new(f);
    let mut tickers: Vec<Ticker> = reader
        .lines()
        .into_iter()
        .map(|w| w.unwrap())
        .map(|line| serde_json::from_str::<Ticker>(&line).unwrap())
        .collect();

    let total = tickers.len();
    let mut handl_count = 0;
    let mut seconds = Utc::now().timestamp() / 60;

    info!("Loaded tickers: {} from {}", total, file);

    for ticker in tickers.iter_mut() {
        if mode == ReplayMode::Sync {
            debug!("************************************************************************************************************");
            context
                .dispatch_direct(ticker, asset.next_message_id())
                .await?;
        } else {
            context.dispatch(ticker).await?;
        }
        handl_count = handl_count + 1;

        if seconds < Utc::now().timestamp() / 60 {
            debug!("Handled items {}/{} for {}", handl_count, total, file);
            seconds = seconds + 1;
        }

        // delay for backpress in async mode
        if let ReplayMode::Async { delay } = mode {
            if delay > 0 {
                sleep(Duration::from_millis(delay));
            }
        }

        // settle all orders when turns to post market
        if matches!(ticker.market_hours, MarketHoursType::PostMarket) {
            if profit_evaluate(context.asset(), context.config())? {
                break;
            }
        }
    }
    info!("Tickers: {} replay done", &file);

    if config.replay.outputs.file.enabled || config.replay.outputs.elasticsearch.enabled {
        let source_file = Path::new(file).file_name().unwrap().to_str().unwrap();

        if config.replay.export_enabled("protfolio") {
            info!("Exporting protfolios for {}", source_file);
            export_protfolios(&context, source_file).await?;
        }

        if config.replay.export_enabled("slope") {
            info!("Exporting slopes for {}", source_file);
            export_slope_points(&context, source_file).await?;
        }

        if config.replay.export_enabled("trade") {
            info!("Exporting trades info for {}", source_file);
            export_trades(&context, source_file).await?;
        }
    }

    // clean memory
    info!("Clean up cached data for next run");
    context.asset().clean()?;

    Ok(())
}

async fn export_protfolios(context: &AppContext, file: &str) -> Result<()> {
    let config = context.config();
    let persistence = context.persistence();

    // delete file
    if config.replay.outputs.file.enabled && config.truncat_enabled() {
        let base_path = format!("{}/analysis/{}", &config.replay.outputs.base_folder, file);
        if Path::new(&base_path).exists() {
            info!("Remove files under {}", &base_path);
            remove_dir_all(&base_path)?;
        }
    }
    // delete index
    if config.replay.outputs.elasticsearch.enabled && config.truncat_enabled() {
        let index_time = take_index_time(&file);
        let index_name = protfolio_index_name(&index_time);
        persistence.delete_index(&index_name).await?;
    }

    for (symbol, groups) in context.asset().protfolios().as_ref() {
        for (unit, lock) in groups {
            // ignore moving protfolios
            if TimeUnit::is_moving_unit(unit) {
                continue;
            }

            let list_reader = lock.read().unwrap();
            if !list_reader.is_empty() {
                if config.replay.outputs.file.enabled {
                    let output_name = format!(
                        "{}/analysis/{}/protfolio-{}-{}.json",
                        &config.replay.outputs.base_folder, file, symbol, unit
                    );
                    let path = Path::new(&output_name).parent().unwrap().to_str().unwrap();
                    create_dir_all(&path)?;
                    let output = OpenOptions::new()
                        .write(true)
                        .create(true)
                        .truncate(true)
                        .open(&output_name)?;
                    let mut writer = BufWriter::new(output);

                    debug!("Dump analysis: {}", &output_name);
                    list_reader.iter().rev().for_each(|item| {
                        let json = serde_json::to_string(&item).unwrap();
                        write!(&mut writer, "{}\n", &json).unwrap();
                    });
                    debug!("Finish analysis: {} file", &output_name);
                }
                if config.replay.outputs.elasticsearch.enabled {
                    let protfolios: Vec<Protfolio> =
                        list_reader.iter().map(|p| p.clone()).collect();

                    // generate index name
                    let time = Utc.timestamp_millis(protfolios.first().unwrap().time);
                    let index_name = protfolio_index_name(&time);

                    bulk_index(&context, &index_name, &protfolios).await?;
                }
            }
        }
    }

    Ok(())
}

async fn export_slope_points(context: &AppContext, file: &str) -> Result<()> {
    let config = context.config();
    let persistence = context.persistence();

    // delete file
    if config.replay.outputs.file.enabled && config.truncat_enabled() {
        let base_path = format!("{}/slopes/{}", &config.replay.outputs.base_folder, file);
        if Path::new(&base_path).exists() {
            info!("Remove files under {}", &base_path);
            remove_dir_all(&base_path)?;
        }
    }
    // delete index
    if config.replay.outputs.elasticsearch.enabled && config.truncat_enabled() {
        let index_time = take_index_time(&file);
        let index_name = slope_index_name(&index_time);
        persistence.delete_index(&index_name).await?;
    }

    for (symbol, groups) in context.asset().protfolios().as_ref() {
        for (unit, lock) in groups {
            // ignore moving protfolios
            if TimeUnit::is_moving_unit(unit) {
                continue;
            }

            let list_reader = lock.read().unwrap();
            if !list_reader.is_empty() {
                if config.replay.outputs.file.enabled {
                    let output_name = format!(
                        "{}/slopes/{}/slope-{}-{}.json",
                        &config.replay.outputs.base_folder, file, symbol, unit
                    );
                    let path = Path::new(&output_name).parent().unwrap().to_str().unwrap();
                    create_dir_all(&path)?;
                    let output = OpenOptions::new()
                        .write(true)
                        .create(true)
                        .truncate(true)
                        .open(&output_name)?;
                    let mut writer = BufWriter::new(output);

                    let protfolios: Vec<Protfolio> =
                        list_reader.iter().map(|p| p.clone()).collect();
                    let points = draw_slop_lines(&protfolios);

                    debug!("Dump slope: {}", &output_name);
                    points.iter().for_each(|item| {
                        let json = serde_json::to_string(&item).unwrap();
                        write!(&mut writer, "{}\n", &json).unwrap();
                    });
                    debug!("Finish slope: {} file", &output_name);
                }
                if config.replay.outputs.elasticsearch.enabled {
                    let protfolios: Vec<Protfolio> =
                        list_reader.iter().map(|p| p.clone()).collect();
                    let points = draw_slop_lines(&protfolios);

                    // generate index name
                    let time = Utc.timestamp_millis(points.first().unwrap().time);
                    let index_name = slope_index_name(&time);

                    bulk_index(&context, &index_name, &points).await?;
                }
            }
        }
    }

    Ok(())
}

async fn export_trades(context: &AppContext, file: &str) -> Result<()> {
    let config = context.config();
    let persistence = context.persistence();
    let asset = context.asset();

    // delete file
    if config.replay.outputs.file.enabled && config.truncat_enabled() {
        let base_path = format!("{}/trades/{}", &config.replay.outputs.base_folder, file);
        if Path::new(&base_path).exists() {
            info!("Remove files under {}", &base_path);
            remove_dir_all(&base_path)?;
        }
    }
    // delete index
    if config.replay.outputs.elasticsearch.enabled && config.truncat_enabled() {
        let index_time = take_index_time(&file);
        let index_name = trade_index_name(&index_time);
        persistence.delete_index(&index_name).await?;
    }

    for (symbol, list_lock) in asset.trades().as_ref() {
        let list_reader = list_lock.read().unwrap();
        if config.replay.outputs.file.enabled {
            let output_name = format!(
                "{}/trades/{}/trade-{}.json",
                &config.replay.outputs.base_folder, file, symbol
            );
            let path = Path::new(&output_name).parent().unwrap().to_str().unwrap();
            create_dir_all(&path)?;
            let output = OpenOptions::new()
                .write(true)
                .create(true)
                .truncate(true)
                .open(&output_name)?;
            let mut writer = BufWriter::new(output);

            debug!("Dump trades: {}", &output_name);
            list_reader.iter().rev().for_each(|item_lock| {
                let item = item_lock.read().unwrap().clone();
                let json = serde_json::to_string(&item).unwrap();
                write!(&mut writer, "{}\n", &json).unwrap();
            });
            debug!("Finish trades: {} file", &output_name);
        }
        // too many data, stop export temporary
        if config.replay.outputs.elasticsearch.enabled && false {
            let trades: Vec<ElasticTrade> = list_reader
                .iter()
                .flat_map(|item_lock| ElasticTrade::from(&item_lock.read().unwrap()))
                .collect();

            if !trades.is_empty() {
                // generate index name
                let time = Utc.timestamp_millis(trades.first().unwrap().timestamp);
                let index_name = trade_index_name(&time);

                for chunk in trades.chunks(10000) {
                    bulk_index(
                        &context,
                        &index_name,
                        &chunk
                            .iter()
                            .map(|t| t.clone())
                            .collect::<Vec<ElasticTrade>>(),
                    )
                    .await?;
                }
            }
        }
    }

    Ok(())
}
