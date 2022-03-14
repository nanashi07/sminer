use crate::persist::read_from_file;
use log::info;
use sminer::{
    analysis::rebalance,
    init_log,
    persist::{mongo::get_mongo_client, DataSource},
    vo::{biz::Ticker, core::AppContext},
    Result,
};

#[tokio::test]
#[ignore = "manually run only, replay from file"]
async fn test_replay() -> Result<()> {
    init_log("INFO").await?;
    let context = AppContext::new();

    // TODO: temp sollution
    for _ in 1..10 {
        info!("create conn");
        let conn = get_mongo_client().await?;
        context.persistence.close_connection(conn)?;
        info!("conn created");
    }

    let files = vec!["tickers20220309", "tickers20220310", "tickers20220311"];
    for file in files {
        let tickers: Vec<Ticker> = read_from_file(file)?
            .iter()
            .map(|line| {
                let ticker: Ticker = serde_json::from_str(line).unwrap();
                ticker
            })
            .collect();
        info!("Loaded tickers: {} for {}", tickers.len(), file);

        for ticker in tickers {
            rebalance(&context, &ticker).await?;
        }
    }
    Ok(())
}
