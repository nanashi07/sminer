use log::error;
use sminer::{
    analysis::{replay, ReplayMode},
    init_log,
    vo::core::{AppConfig, AppContext},
    Result,
};
use tokio::runtime::Runtime;

fn take_digitals(str: &str) -> String {
    str.chars().filter(|c| c.is_numeric()).collect::<String>()
}

// cargo test --package sminer --test tests -- analysis::test_replay --exact --nocapture --ignored
#[test]
#[ignore = "manually run only, replay from file"]
fn test_replay() -> Result<()> {
    let rt = Runtime::new()?;
    let result: Result<()> = rt.block_on(async {
        init_log("INFO").await?;
        let context = AppContext::new(AppConfig::load("config.yaml")?)
            .init()
            .await?;

        let files = vec![
            "tickers20220309",
            // "tickers20220310",
            // "tickers20220311",
            // "tickers20220314",
            // "tickers20220315",
            // "tickers20220316",
        ];
        for file in files {
            context.persistence.drop_collection(file).await?;
            context.persistence.drop_index(&take_digitals(file)).await?;
            replay(&context, &format!("tmp/{}", &file), ReplayMode::Sync).await?
        }
        Ok(())
    });
    if let Err(err) = result {
        error!("{}", err);
    }
    Ok(())
}

#[test]
#[ignore = "manually run only, replay from file"]
fn test_replay_async() -> Result<()> {
    let rt = Runtime::new()?;
    let result: Result<()> = rt.block_on(async {
        init_log("INFO").await?;
        let context = AppContext::new(AppConfig::load("config.yaml")?)
            .init()
            .await?;

        let files = vec![
            "tickers20220309-nospy",
            // "tickers20220310",
            // "tickers20220311",
            // "tickers20220314",
            // "tickers20220315",
            // "tickers20220316",
        ];

        // default delay value
        let _delay_for_mongo: u64 = 20;
        let _delay_for_es: u64 = 10;

        for file in files {
            context.persistence.drop_collection(file).await?;
            context.persistence.drop_index(&take_digitals(file)).await?;
            replay(
                &context,
                &format!("tmp/{}", &file),
                ReplayMode::Async { delay: 1000 },
            )
            .await?
        }
        Ok(())
    });
    if let Err(err) = result {
        error!("{}", err);
    }
    Ok(())
}
