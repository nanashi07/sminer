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
use log::{debug, error, info};
use std::{
    fs::File,
    io::{BufRead, BufReader},
    sync::Arc,
    thread::sleep,
    time::Duration,
};
use tokio::sync::broadcast::Receiver;

pub async fn init_dispatcher(context: &Arc<AppContext>) -> Result<()> {
    let house_keeper = &context.house_keeper;
    let preparatory = &context.preparatory;
    let persistence = Arc::clone(&context.persistence);

    info!("Initialize mongo event handler");
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

    info!("Initialize elasticsearch event handler");
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

    info!("Initialize event preparatory handler");
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
            info!(
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
    // TODO: split for sync replay
    ticker.save_to_mongo(Arc::clone(context)).await?;
    Ok(())
}

async fn handle_message_for_elasticsearch(
    rx: &mut Receiver<TickerEvent>,
    context: &Arc<PersistenceContext>,
) -> Result<()> {
    let ticker: ElasticTicker = rx.recv().await?.into();
    // TODO: split for sync replay
    ticker.save_to_elasticsearch(Arc::clone(&context)).await?;
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

    // Get ticker source
    let protfolios = Arc::clone(&context.protfolios);
    if let Some(uniter) = protfolios.get(symbol) {
        if let Some(lock) = uniter.get(unit) {
            debug!("handle calc for {} of {:?}", symbol, unit);
            let list = lock.write().unwrap();
            // list.push_front()
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
            info!("Hanlding process {}/{} for {}", handl_count, total, file);
            seconds = seconds + 1;
        }

        // delay for backpress
        if let ReplayMode::Async { delay } = mode {
            if delay > 0 {
                sleep(Duration::from_millis(delay));
            }
        }
    }
    info!("Tickers: {} replay done", file);
    Ok(())
}
