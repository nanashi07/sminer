use futures::TryStreamExt;
use log::info;

use crate::{init_log, persist::mongo::query_ticker, vo::biz::Ticker, Result};

pub async fn rebalance(ticker: &Ticker) -> Result<()> {
    info!("{:?}", ticker);
    // TODO: save mongo
    // TODO: save es
    Ok(())
}

#[tokio::test]
async fn test_replay() -> Result<()> {
    init_log("TRACE").await?;
    let mut cursor = query_ticker("yahoo20220309", "TQQQ").await?;
    while let Some(ticker) = cursor.try_next().await? {
        rebalance(&ticker).await?;
    }
    Ok(())
}
