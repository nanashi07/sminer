mod computor;

use crate::{
    persist::{es::ElasticTicker, PersistenceContext},
    proto::biz::TickerEvent,
    vo::{
        biz::{Ticker, TimeUnit},
        core::AppContext,
    },
    Result,
};
use chrono::Utc;
use log::{debug, error, info, trace};
use std::{
    fs::{File, OpenOptions},
    io::{BufRead, BufReader, BufWriter, Write},
    path::Path,
    sync::Arc,
    thread::sleep,
    time::Duration,
};
use tokio::sync::broadcast::Receiver;

pub async fn init_dispatcher(context: &Arc<AppContext>) -> Result<()> {
    let house_keeper = &context.house_keeper;
    let preparatory = &context.preparatory;
    let persistence = context.persistence();

    debug!("Initialize mongo event handler");
    let mut rx = house_keeper.subscribe();
    let ctx = Arc::clone(&persistence);
    tokio::spawn(async move {
        loop {
            match handle_message_for_mongo(&mut rx, &ctx).await {
                Ok(_) => {}
                Err(err) => {
                    error!("Handle ticker for mongo error: {:?}", err);
                }
            }
        }
    });

    debug!("Initialize elasticsearch event handler");
    let mut rx = house_keeper.subscribe();
    let ctx = Arc::clone(&persistence);
    tokio::spawn(async move {
        loop {
            match handle_message_for_elasticsearch(&mut rx, &ctx).await {
                Ok(_) => {}
                Err(err) => {
                    error!("Handle ticker for elasticsearch error: {:?}", err);
                }
            }
        }
    });

    debug!("Initialize event preparatory handler");
    let mut rx = preparatory.subscribe();
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

    let root = Arc::clone(&context);
    for time_unit in TimeUnit::values() {
        for symbol in root.config.symbols() {
            debug!(
                "Initialize event calculate {} for {:?} handler",
                symbol, time_unit
            );
            let calculator = Arc::clone(&context.calculator);

            let mut rx = calculator.get(&symbol).unwrap().subscribe();
            let root = Arc::clone(&context);
            tokio::spawn(async move {
                loop {
                    match handle_message_for_calculator(&mut rx, &root, &symbol, &time_unit).await {
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

async fn handle_message_for_mongo(
    rx: &mut Receiver<TickerEvent>,
    context: &Arc<PersistenceContext>,
) -> Result<()> {
    let ticker: Ticker = rx.recv().await?.into();
    let config = context.config();
    if config.mongo_enabled() {
        ticker.save_to_mongo(Arc::clone(context)).await?;
    }
    Ok(())
}

async fn handle_message_for_elasticsearch(
    rx: &mut Receiver<TickerEvent>,
    context: &Arc<PersistenceContext>,
) -> Result<()> {
    let ticker: ElasticTicker = rx.recv().await?.into();
    let config = context.config();
    if config.mongo_enabled() {
        ticker.save_to_elasticsearch(Arc::clone(&context)).await?;
    }
    Ok(())
}

async fn handle_message_for_preparatory(
    rx: &mut Receiver<TickerEvent>,
    context: &Arc<AppContext>,
) -> Result<()> {
    let ticker: Ticker = rx.recv().await?.into();
    // TODO: split for sync replay
    let ctx = Arc::clone(context);

    // Add into source list
    let tickers = Arc::clone(&ctx.tickers);
    if let Some(lock) = tickers.get(&ticker.id) {
        let mut list = lock.write().unwrap();
        list.push_front(ticker.clone());
    } else {
        error!("No tickers container {} initialized", &ticker.id);
    }

    // Send signal for symbol analysis
    let calculator = Arc::clone(&context.calculator);
    let sender = calculator.get(&ticker.id).unwrap();
    sender.send(Utc::now().timestamp_millis())?;

    Ok(())
}

async fn handle_message_for_calculator(
    rx: &mut Receiver<i64>,
    context: &Arc<AppContext>,
    symbol: &str,
    unit: &TimeUnit,
) -> Result<()> {
    // Receive signal only
    let _: i64 = rx.recv().await?.into();
    trace!("handle_message_for_calculator: {:?} of {}", unit, symbol);
    context.route(symbol, unit)?;
    Ok(())
}

impl AppContext {
    pub fn route(&self, symbol: &str, unit: &TimeUnit) -> Result<()> {
        debug!("Route calculation for {:?} of {}", unit, symbol);
        let protfolios = Arc::clone(&self.protfolios);
        if let Some(uniter) = protfolios.get(symbol) {
            if let Some(lock) = uniter.get(unit) {
                debug!("handle calc for {} of {:?}", symbol, unit);
                // Get ticker source
                let tickers = self.tickers.get(symbol).unwrap();
                let symbol_tickers = tickers.read().unwrap();
                // Get target protfolios
                let mut list = lock.write().unwrap();
                // Start calculation
                unit.rebalance(&symbol_tickers, &mut list)?;
            } else {
                error!(
                    "Not protfolios container {:?} of {} initialized",
                    unit, symbol
                );
            }
        } else {
            error!("Not protfolios container {} initialized", symbol);
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

    let f = File::open(file)?;
    let reader = BufReader::new(f);
    let tickers: Vec<Ticker> = reader
        .lines()
        .into_iter()
        .map(|w| w.unwrap())
        .map(|line| serde_json::from_str::<Ticker>(&line).unwrap())
        .collect();

    let total = tickers.len();
    let mut handl_count = 0;
    let mut seconds = Utc::now().timestamp() / 60;

    info!("Loaded tickers: {} for {}", total, file);

    for ticker in tickers {
        if mode == ReplayMode::Sync {
            context.dispatch_direct(&ticker).await?;
        } else {
            context.dispatch(&ticker).await?;
        }
        handl_count = handl_count + 1;

        if seconds < Utc::now().timestamp() / 60 {
            info!("Handled items {}/{} for {}", handl_count, total, file);
            seconds = seconds + 1;
        }

        // delay for backpress
        if let ReplayMode::Async { delay } = mode {
            if delay > 0 {
                sleep(Duration::from_millis(delay));
            }
        }
    }
    info!("Tickers: {} replay done", &file);

    if context.config.analysis.output.file.enabled {
        // output analysis file
        let filename = Path::new(file).file_name().unwrap().to_str().unwrap();
        output_protfolios(&context, filename);
    }

    // clean memory
    context.clean()?;

    Ok(())
}

fn output_protfolios(context: &AppContext, file: &str) {
    let config = context.config();
    let protfolios = Arc::clone(&context.protfolios);
    protfolios.iter().for_each(|(ticker_id, groups)| {
        groups.iter().for_each(|(unit, lock)| {
            let list_reader = lock.read().unwrap();
            if !list_reader.is_empty() {
                let output_name = format!(
                    "{}/analysis/{}/{}-{:?}.json",
                    &config.analysis.output.base_folder, file, ticker_id, unit
                );
                let path = Path::new(&output_name).parent().unwrap().to_str().unwrap();
                std::fs::create_dir_all(&path).unwrap();
                let output = OpenOptions::new()
                    .write(true)
                    .create(true)
                    .truncate(true)
                    .open(&output_name)
                    .unwrap();
                let mut writer = BufWriter::new(output);
                debug!("Dump analysis: {}", &output_name);

                list_reader.iter().rev().for_each(|item| {
                    let json = serde_json::to_string(&item).unwrap();
                    write!(&mut writer, "{}\n", &json).unwrap();
                });
                info!("Finish analysis: {} file", &output_name);
            }
        });
    });
}
