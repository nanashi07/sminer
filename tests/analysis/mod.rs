use log::error;
use sminer::{
    analysis::{replay, ReplayMode},
    init_log,
    persist::es::take_digitals,
    vo::core::{AppConfig, AppContext},
    Result,
};
use tokio::runtime::Runtime;

// cargo test --package sminer --test tests -- analysis::test_replay --exact --nocapture --ignored
#[test]
#[ignore = "manually run only, replay from file"]
fn test_replay() -> Result<()> {
    let rt = Runtime::new()?;
    let result: Result<()> = rt.block_on(async {
        init_log("INFO").await?;
        let mut config = AppConfig::load("config.yaml")?;
        // disable mongodb persistence
        config.data_source.mongodb.enabled = false;
        // disable elasticsearch persistence
        config.data_source.elasticsearch.enabled = false;
        let context = AppContext::new(config).init().await?;

        let files = vec![
            "tickers20220309.LABU",
            // "tickers20220310",
            // "tickers20220311",
            // "tickers20220314",
            // "tickers20220315",
            // "tickers20220316",
        ];
        for file in files {
            if &context.config.data_source.mongodb.enabled == &true {
                context.persistence.drop_collection(file).await?;
            }
            if &context.config.data_source.elasticsearch.enabled == &true {
                context.persistence.drop_index(&take_digitals(file)).await?;
            }
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
        let mut config = AppConfig::load("config.yaml")?;
        // disable mongodb persistence
        config.data_source.mongodb.enabled = false;
        // disable elasticsearch persistence
        config.data_source.elasticsearch.enabled = false;
        let context = AppContext::new(config).init().await?;

        let files = vec![
            "tickers20220309",
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
            if &context.config.data_source.mongodb.enabled == &true {
                context.persistence.drop_collection(file).await?;
            }
            if &context.config.data_source.elasticsearch.enabled == &true {
                context.persistence.drop_index(&take_digitals(file)).await?;
            }
            replay(
                &context,
                &format!("tmp/{}", &file),
                ReplayMode::Async { delay: 50 },
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
