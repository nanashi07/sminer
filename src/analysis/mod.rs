use crate::{
    persist::{es::ElasticTicker, PersistenceContext},
    proto::biz::TickerEvent,
    vo::biz::Ticker,
    Result,
};
use log::info;
use std::sync::Arc;
use tokio::sync::broadcast::Sender;

pub async fn init_dispatcher(
    sender: &Sender<TickerEvent>,
    persistence: &Arc<PersistenceContext>,
) -> Result<()> {
    info!("Initialize mongo event handler");
    let mut rx = sender.subscribe();
    let context = Arc::clone(&persistence);
    tokio::spawn(async move {
        let ticker: Ticker = rx.recv().await.unwrap().into();

        ticker.save_to_mongo(Arc::clone(&context)).await.unwrap();
    });

    info!("Initialize elasticsearch event handler");
    let mut rx = sender.subscribe();
    let context = Arc::clone(&persistence);
    tokio::spawn(async move {
        let ticker: ElasticTicker = rx.recv().await.unwrap().into();

        ticker
            .save_to_elasticsearch(Arc::clone(&context))
            .await
            .unwrap();
    });
    Ok(())
}
