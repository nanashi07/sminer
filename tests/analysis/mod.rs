use sminer::{
    analysis::{init_dispatcher, replay},
    init_log,
    vo::core::AppContext,
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
        let context = AppContext::new();
        init_dispatcher(&context.sender, &context.persistence).await?;
        // FIXME: temp sollution
        context.persistence.init_mongo().await?;

        let files = vec![
            "tickers20220309",
            // "tickers20220310",
            // "tickers20220311",
            // "tickers20220314",
            // "tickers20220315",
        ];
        let _delay_for_mongo: u64 = 20;
        let _delay_for_es: u64 = 10;
        for file in files {
            replay(&context, &format!("tmp/{}", &file), _delay_for_mongo).await?
        }
        Ok(())
    });
    Ok(())
}
