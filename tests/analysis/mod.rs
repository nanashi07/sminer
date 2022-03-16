use crate::persist::read_from_file;
use chrono::Utc;
use log::info;
use sminer::{
    analysis::init_dispatcher,
    init_log,
    vo::{biz::Ticker, core::AppContext},
    Result,
};

// cargo test --package sminer --test tests -- analysis::test_replay --exact --nocapture --ignored
#[tokio::test]
#[ignore = "manually run only, replay from file"]
async fn test_replay() -> Result<()> {
    init_log("INFO").await?;
    let context = AppContext::new();
    init_dispatcher(&context.sender, &context.persistence).await?;
    // FIXME: temp sollution
    context.persistence.init_mongo().await?;

    let files = vec![
        "tickers20220309",
        "tickers20220310",
        "tickers20220311",
        "tickers20220314",
        "tickers20220315",
    ];
    for file in files {
        info!("Loading tickers: {}", file);
        let tickers: Vec<Ticker> = read_from_file(file)?
            .iter()
            .map(|line| {
                let ticker: Ticker = serde_json::from_str(line).unwrap();
                ticker
            })
            .collect();

        let total = tickers.len();
        let mut handl_count = 0;
        let mut seconds = Utc::now().timestamp() / 60;

        info!("Loaded tickers: {} for {}", total, file);

        for ticker in tickers {
            context.dispatch(&ticker).await?;
            handl_count = handl_count + 1;

            if seconds < Utc::now().timestamp() / 60 {
                info!("Hanlding process {}/{} for {}", handl_count, total, file);
                seconds = seconds + 1;
            }
        }
        info!("Tickers: {} replay done", file);
    }
    Ok(())
}
