use crate::{
    persist::es::ElasticTicker,
    vo::{biz::Ticker, core::AppContext},
    Result,
};
use log::info;

pub async fn rebalance(context: &AppContext, ticker: &Ticker) -> Result<()> {
    info!("Rebalance {:?}", ticker);

    // Save to mongo
    ticker.save_to_mongo(&context.persistence).await?; // FIXME: pass in arg

    // Save to elasticsearch
    let t: ElasticTicker = ticker.into();
    t.save_to_elasticsearch(&context.persistence).await?;

    // TODO: analysis
    Ok(())
}
