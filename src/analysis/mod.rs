mod computor;
mod debug;
pub mod trade;

use self::trade::prepare_trade;
use crate::{
    analysis::{computor::draw_slop_lines, debug::profit_evaluate},
    persist::{
        es::{
            bulk_index, protfolio_index_name, slope_index_name, take_index_time, trade_index_name,
            ElasticTicker, ElasticTrade,
        },
        PersistenceContext,
    },
    proto::biz::TickerEvent,
    vo::{
        biz::{MarketHoursType, Protfolio, Ticker, TimeUnit, TradeInfo},
        core::AppContext,
    },
    Result,
};
use chrono::{TimeZone, Utc};
use log::{debug, error, info, trace};
use std::{
    fs::{create_dir_all, remove_dir_all, File, OpenOptions},
    io::{BufRead, BufReader, BufWriter, Write},
    path::Path,
    sync::Arc,
    thread::sleep,
    time::Duration,
};
use tokio::sync::broadcast::Receiver;

pub async fn init_dispatcher(context: &Arc<AppContext>) -> Result<()> {
    let config = context.config();
    let post_man = context.post_man();
    let persistence = context.persistence();

    info!("Initialize mongo event persist handler");
    let mut rx = post_man.subscribe_store();
    let ctx = Arc::clone(&persistence);
    tokio::spawn(async move {
        loop {
            match handle_message_for_persist_mongo(&mut rx, &ctx).await {
                Ok(_) => {}
                Err(err) => {
                    error!("Handle ticker for mongo error: {:?}", err);
                }
            }
        }
    });

    info!("Initialize elasticsearch event persist handler");
    let mut rx = post_man.subscribe_store();
    let ctx = Arc::clone(&persistence);
    tokio::spawn(async move {
        loop {
            match handle_message_for_persist_elasticsearch(&mut rx, &ctx).await {
                Ok(_) => {}
                Err(err) => {
                    error!("Handle ticker for elasticsearch error: {:?}", err);
                }
            }
        }
    });

    info!("Initialize event preparatory handler");
    let mut rx = post_man.subscribe_prepare();
    let root = Arc::clone(&context);
    tokio::spawn(async move {
        loop {
            match handle_message_for_preparatory(&mut rx, &root).await {
                Ok(_) => {}
                Err(err) => {
                    error!("Handle ticker for preparatory error: {:?}", err);
                }
            }
        }
    });

    if config.trade.enabled {
        info!("Initialize event trade handler");
        let mut rx = post_man.subscribe_trade();
        let root = Arc::clone(&context);
        tokio::spawn(async move {
            loop {
                match handle_message_for_trade(&mut rx, &root).await {
                    Ok(_) => {}
                    Err(err) => {
                        error!("Handle ticker for preparatory error: {:?}", err);
                    }
                }
            }
        });
    }

    info!("Initialize event calculator handler");
    let root = Arc::clone(&context);
    for unit in config.time_units() {
        for symbol in root.config().symbols() {
            debug!(
                "Initialize event calculate {} for {:?} handler",
                &symbol, unit
            );
            let mut rx = post_man.subscribe_calculate(&symbol);
            let root = Arc::clone(&context);
            let unit = unit.clone();
            tokio::spawn(async move {
                loop {
                    match handle_message_for_calculator(&mut rx, &root, &symbol, &unit).await {
                        Ok(_) => {}
                        Err(err) => {
                            error!("Handle ticker for calculator error: {:?}", err);
                        }
                    }
                }
            });
        }
    }

    Ok(())
}

async fn handle_message_for_persist_mongo(
    rx: &mut Receiver<TickerEvent>,
    context: &Arc<PersistenceContext>,
) -> Result<()> {
    let ticker: Ticker = rx.recv().await?.into();
    let config = context.config();
    if config.sync_mongo_enabled() {
        ticker.save_to_mongo(Arc::clone(context)).await?;
    }
    Ok(())
}

async fn handle_message_for_persist_elasticsearch(
    rx: &mut Receiver<TickerEvent>,
    context: &Arc<PersistenceContext>,
) -> Result<()> {
    let ticker: ElasticTicker = rx.recv().await?.into();
    let config = context.config();
    if config.sync_elasticsearch_enabled() {
        ticker.save_to_elasticsearch(Arc::clone(&context)).await?;
    }
    Ok(())
}

async fn handle_message_for_preparatory(
    rx: &mut Receiver<TickerEvent>,
    context: &Arc<AppContext>,
) -> Result<()> {
    let ticker: Ticker = rx.recv().await?.into();

    // Add into source list
    if let Some(lock) = context.asset().symbol_tickers(&ticker.id) {
        let mut list = lock.write().unwrap();
        list.push_front(ticker.clone());
    } else {
        error!("No tickers container {} initialized", &ticker.id);
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
    context.post_man().calculate(&ticker.id, message_id)?;

    Ok(())
}

async fn handle_message_for_calculator(
    rx: &mut Receiver<i64>,
    context: &Arc<AppContext>,
    symbol: &str,
    unit: &TimeUnit,
) -> Result<()> {
    // Receive message ID only
    let message_id: i64 = rx.recv().await?.into();
    trace!("handle_message_for_calculator: {:?} of {}", unit, symbol);
    context.route(message_id, symbol, unit)?;

    // check all values finalized then push to prepare trade
    if context.config().trade.enabled && context.asset().is_trade_finalized(symbol, message_id) {
        debug!(
            "Prepare handle finalized trade info, symbol: {}, message_id: {}",
            symbol, &message_id
        );
        context.post_man().watch_trade(message_id).await?;
    }
    Ok(())
}

async fn handle_message_for_trade(rx: &mut Receiver<i64>, context: &Arc<AppContext>) -> Result<()> {
    let message_id: i64 = rx.recv().await?.into();
    prepare_trade(context.asset(), context.config(), message_id)?;
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
