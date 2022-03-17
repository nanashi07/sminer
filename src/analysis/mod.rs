use crate::{
    persist::{es::ElasticTicker, PersistenceContext},
    proto::biz::TickerEvent,
    vo::biz::Ticker,
    Result,
};
use log::{error, info};
use std::sync::Arc;
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

pub async fn replay() -> Result<()> {
    Ok(())
}
