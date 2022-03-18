use sminer::{
    analysis::{replay, ReplayMode},
    init_log,
    vo::core::{AppContext, Config},
    Result,
};
use tokio::runtime::Runtime;

// cargo test --package sminer --test tests -- analysis::test_replay --exact --nocapture --ignored
#[test]
#[ignore = "manually run only, replay from file"]
fn test_replay() -> Result<()> {
    let rt = Runtime::new()?;
    let _: Result<()> = rt.block_on(async {
        init_log("INFO").await?;
        let context = AppContext::new().init(&Config::new()).await?;

        let files = vec![
            "tickers20220309",
            // "tickers20220310",
            // "tickers20220311",
            // "tickers20220314",
            // "tickers20220315",
            // "tickers20220316",
        ];
        let _delay_for_mongo: u64 = 20;
        let _delay_for_es: u64 = 10;
        for file in files {
            context.persistence.drop_collection(file).await?;
            context.persistence.drop_index(file).await?;
            replay(&context, &format!("tmp/{}", &file), ReplayMode::Sync).await?
        }
        Ok(())
    });
    Ok(())
}
