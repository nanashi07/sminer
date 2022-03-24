mod computor;

use crate::{
    persist::{
        es::{index_protfolios, ElasticTicker},
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
    for unit in TimeUnit::values() {
        for symbol in root.config.symbols() {
            debug!(
                "Initialize event calculate {} for {:?} handler",
                symbol, unit
            );
            let calculator = Arc::clone(&context.calculator);

            let mut rx = calculator.get(&symbol).unwrap().subscribe();
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
    if config.elasticsearch_enabled() {
        ticker.save_to_elasticsearch(Arc::clone(&context)).await?;
    }
    Ok(())
}

async fn handle_message_for_preparatory(
    rx: &mut Receiver<TickerEvent>,
    context: &Arc<AppContext>,
) -> Result<()> {
    let ticker: Ticker = rx.recv().await?.into();
    let ctx = Arc::clone(context);

    // Add into source list
    let tickers = Arc::clone(&ctx.tickers);
    if let Some(lock) = tickers.get(&ticker.id) {
        let mut list = lock.write().unwrap();
        list.push_front(ticker.clone());
    } else {
        error!("No tickers container {} initialized", &ticker.id);
    }

    // TODO: Add ticker decision data first (id/time... with empty analysis data)
    let message_id = Utc::now().timestamp_millis(); // TODO: make sure uniq

    // Send signal for symbol analysis
    let calculator = Arc::clone(&context.calculator);
    let sender = calculator.get(&ticker.id).unwrap();

    sender.send(message_id)?;

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
    context.route(&message_id, symbol, unit)?;
    Ok(())
}

impl AppContext {
    pub fn route(&self, message_id: &i64, symbol: &str, unit: &TimeUnit) -> Result<()> {
        debug!(
            "========== Route calculation for {} of {}, message_id: {} ==========",
            symbol, unit.duration, message_id
        );
        let protfolios_map = Arc::clone(&self.protfolios);
        let points_map = Arc::clone(&self.slopes);

        if let Some(unit_map) = protfolios_map.get(symbol) {
            if let Some(protfolios_lock) = unit_map.get(&unit.name) {
                debug!(
                    "Handle calculation for {} of {}, message_id: {}",
                    symbol, unit.duration, message_id
                );
                // Get ticker source
                let tickers = self.tickers.get(symbol).unwrap();
                let symbol_tickers = tickers.read().unwrap();

                // Get target protfolios
                let mut protfolios = protfolios_lock.write().unwrap();

                // Start calculation
                unit.rebalance(
                    symbol,
                    message_id,
                    &symbol_tickers,
                    &mut protfolios, /*  &mut slope */
                )?;

                // // Get target slope point
                // if let Some(slopes_lock) = points_map.get(symbol) {
                //     // let jj = slopes_lock.write().unwrap();

                //     match slopes_lock.write() {
                //         Ok(mut slopes) => {
                //             let mut slope = slopes
                //                 .iter_mut()
                //                 .find(|s| s.message_id == *message_id)
                //                 .unwrap();

                //             // Start calculation
                //             unit.rebalance(&symbol_tickers, &mut protfolios, &mut slope)?;

                //             // TODO: check all values finalized and push
                //         }
                //         Err(err) => {
                //             error!("error : {:?}", err);
                //         }
                //     }
                // }
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
    let mut tickers: Vec<Ticker> = reader
        .lines()
        .into_iter()
        .map(|w| w.unwrap())
        .map(|line| serde_json::from_str::<Ticker>(&line).unwrap())
        .collect();

    let total = tickers.len();
    let mut handl_count = 0;
    let mut seconds = Utc::now().timestamp() / 60;

    info!("Loaded tickers: {} for {}", total, file);

    let mut message_id: i64 = 0;

    for ticker in tickers.iter_mut() {
        if mode == ReplayMode::Sync {
            debug!("************************************************************************************************************");
            message_id += 1;
            context.dispatch_direct(ticker, &message_id).await?;
        } else {
            context.dispatch(ticker).await?;
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

    if context.config.analysis.output.file.enabled
        || context.config.analysis.output.elasticsearch.enabled
    {
        info!("Exporting profpolios for {}", &file);
        // output analysis file
        let filename = Path::new(file).file_name().unwrap().to_str().unwrap();
        output_protfolios(&context, filename).await?;
    }

    info!("Clean up cached data for next run");
    // clean memory
    context.clean()?;

    Ok(())
}

async fn output_protfolios(context: &AppContext, file: &str) -> Result<()> {
    let config = context.config();
    let protfolios = Arc::clone(&context.protfolios);

    for (ticker_id, groups) in protfolios.as_ref() {
        for (unit, lock) in groups {
            // ignore moving protfolios
            if TimeUnit::find(unit).unwrap().period > 0 {
                continue;
            }

            let list_reader = lock.read().unwrap();
            if !list_reader.is_empty() {
                if config.analysis.output.file.enabled {
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
