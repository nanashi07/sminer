mod computor;
pub mod trade;

use self::trade::prepare_trade;
use crate::{
    analysis::computor::draw_slop_lines,
    persist::{
        es::{
            index_protfolios, index_slope_points, protfolio_index_name, slope_index_name,
            take_index_time, ElasticTicker,
        },
        PersistenceContext,
    },
    proto::biz::TickerEvent,
    vo::{
        biz::{Protfolio, Ticker, TimeUnit},
        core::AppContext,
    },
    Result,
};
use chrono::Utc;
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

    info!("Initialize event calculator handler");
    let root = Arc::clone(&context);
    for unit in TimeUnit::values() {
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

    // TODO: Add ticker decision data first (id/time... with empty analysis data)
    let message_id = Utc::now().timestamp_millis(); // TODO: make sure uniq

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
    if context.asset().is_trade_finalized(symbol, message_id) {
        context.post_man().watch_trade(message_id)?;
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

    info!("Loaded tickers: {} in {}", total, file);

    let mut message_id: i64 = 0;

    for ticker in tickers.iter_mut() {
        if mode == ReplayMode::Sync {
            debug!("************************************************************************************************************");
            message_id += 1;
            context.dispatch_direct(ticker, message_id).await?;
        } else {
            context.dispatch(ticker).await?;
        }
        handl_count = handl_count + 1;

        if seconds < Utc::now().timestamp() / 60 {
            info!("Handled items {}/{} for {}", handl_count, total, file);
            seconds = seconds + 1;
        }

        // delay for backpress in async mode
        if let ReplayMode::Async { delay } = mode {
            if delay > 0 {
                sleep(Duration::from_millis(delay));
            }
        }
    }
    info!("Tickers: {} replay done", &file);

    if config.analysis.output.file.enabled || config.analysis.output.elasticsearch.enabled {
        let filename = Path::new(file).file_name().unwrap().to_str().unwrap();

        info!("Exporting protfolios for {}", &filename);
        // output analysis file
        output_protfolios(&context, filename).await?;

        info!("Exporting slope for {}", &filename);
        output_slope_points(&context, filename).await?;
    }

    info!("Clean up cached data for next run");
    // clean memory
    context.asset().clean()?;

    Ok(())
}

async fn output_protfolios(context: &AppContext, file: &str) -> Result<()> {
    let config = context.config();
    let persistence = context.persistence();

    // delete file
    if config.analysis.output.file.enabled && config.truncat_enabled() {
        let base_path = format!("{}/analysis/{}", &config.analysis.output.base_folder, file);
        if Path::new(&base_path).exists() {
            info!("Remove files under {}", &base_path);
            remove_dir_all(&base_path)?;
        }
    }
    // delete index
    if config.analysis.output.elasticsearch.enabled && config.truncat_enabled() {
        let index_time = take_index_time(&file);
        let index_name = protfolio_index_name(&index_time);
        persistence.delete_index(&index_name).await?;
    }

    for (ticker_id, groups) in context.asset().protfolios().as_ref() {
        for (unit, lock) in groups {
            // ignore moving protfolios
            if TimeUnit::find(unit).unwrap().period > 0 {
                continue;
            }

            let list_reader = lock.read().unwrap();
            if !list_reader.is_empty() {
                if config.analysis.output.file.enabled {
                    let output_name = format!(
                        "{}/analysis/{}/{}-{}.json",
                        &config.analysis.output.base_folder, file, ticker_id, unit
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
                if config.analysis.output.elasticsearch.enabled {
                    let protfolios: Vec<Protfolio> =
                        list_reader.iter().map(|p| p.clone()).collect();
                    index_protfolios(&context, &protfolios).await?;
                }
            }
        }
    }

    Ok(())
}

async fn output_slope_points(context: &AppContext, file: &str) -> Result<()> {
    let config = context.config();
    let persistence = context.persistence();

    // delete file
    if config.analysis.output.file.enabled && config.truncat_enabled() {
        let base_path = format!("{}/slope/{}", &config.analysis.output.base_folder, file);
        if Path::new(&base_path).exists() {
            info!("Remove files under {}", &base_path);
            remove_dir_all(&base_path)?;
        }
    }
    // delete index
    if config.analysis.output.elasticsearch.enabled && config.truncat_enabled() {
        let index_time = take_index_time(&file);
        let index_name = slope_index_name(&index_time);
        persistence.delete_index(&index_name).await?;
    }

    for (ticker_id, groups) in context.asset().protfolios().as_ref() {
        for (unit, lock) in groups {
            // ignore moving protfolios
            if TimeUnit::find(unit).unwrap().period > 0 {
                continue;
            }

            let list_reader = lock.read().unwrap();
            if !list_reader.is_empty() {
                if config.analysis.output.file.enabled {
                    let output_name = format!(
                        "{}/slope/{}/{}-{}.json",
                        &config.analysis.output.base_folder, file, ticker_id, unit
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
                if config.analysis.output.elasticsearch.enabled {
                    let protfolios: Vec<Protfolio> =
                        list_reader.iter().map(|p| p.clone()).collect();
                    let points = draw_slop_lines(&protfolios);
                    index_slope_points(&context, &points).await?;
                }
            }
        }
    }

    Ok(())
}
