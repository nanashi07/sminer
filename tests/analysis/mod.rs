use futures::TryStreamExt;
use sminer::{analysis::rebalance, init_log, persist::mongo::query_ticker, Result};

#[tokio::test]
#[ignore = "manually run only, replay from file"]
async fn test_replay() -> Result<()> {
    init_log("TRACE").await?;
    let file = "tickers20220309";
    let mut cursor = query_ticker("yahoo", "tickers20220309").await?;
    while let Some(ticker) = cursor.try_next().await? {
        rebalance(&ticker).await?;
    }
    Ok(())
}
