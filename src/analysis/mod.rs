use crate::{
    persist::{es::ElasticTicker, PersistenceContext},
    proto::biz::TickerEvent,
    vo::{biz::Ticker, core::AppContext},
    Result,
};
use chrono::Utc;
use log::{error, info};
use std::{
    fs::File,
    io::{BufRead, BufReader},
    sync::Arc,
    thread::sleep,
    time::Duration,
};
use tokio::sync::broadcast::{Receiver, Sender};

pub async fn init_dispatcher(
    sender: &Sender<TickerEvent>,
    persistence: &Arc<PersistenceContext>,
) -> Result<()> {
    info!("Initialize mongo event handler");
    let mut rx = sender.subscribe();
    let context = Arc::clone(&persistence);
    tokio::spawn(async move {
        loop {
            match handle_message_for_mongo(&mut rx, &context).await {
                Ok(_) => {}
                Err(err) => {
                    error!("Handle ticker for mongo error: {:?}", err);
                }
            }
        }
    });

    info!("Initialize elasticsearch event handler");
    let mut rx = sender.subscribe();
    let context = Arc::clone(&persistence);
    tokio::spawn(async move {
        loop {
            match handle_message_for_elasticsearch(&mut rx, &context).await {
                Ok(_) => {}
                Err(err) => {
                    error!("Handle ticker for elasticsearch error: {:?}", err);
                }
            }
        }
    });
    Ok(())
}

async fn handle_message_for_mongo(
    rx: &mut Receiver<TickerEvent>,
    context: &Arc<PersistenceContext>,
) -> Result<()> {
    let ticker: Ticker = rx.recv().await?.into();
    ticker.save_to_mongo(Arc::clone(context)).await?;
    Ok(())
}

async fn handle_message_for_elasticsearch(
    rx: &mut Receiver<TickerEvent>,
    context: &Arc<PersistenceContext>,
) -> Result<()> {
    let ticker: ElasticTicker = rx.recv().await?.into();
    ticker.save_to_elasticsearch(Arc::clone(&context)).await?;
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
        .map(|line| {
            let ticker: Ticker = serde_json::from_str(&line).unwrap();
            ticker
        })
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
