use log::error;
use sminer::{
    analysis::{replay, ReplayMode},
    init_log,
    persist::es::take_digitals,
    vo::{
        biz::{MarketHoursType, Protfolio, QuoteType, TimeUnit},
        core::{AppConfig, AppContext, KEY_EXTRA_DISABLE_ELASTICSEARCH, KEY_EXTRA_DISABLE_MONGO},
    },
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
        config.extra_put(KEY_EXTRA_DISABLE_MONGO, "disabled");
        // disable elasticsearch persistence
        config.extra_put(KEY_EXTRA_DISABLE_ELASTICSEARCH, "disabled");
        let context = AppContext::new(config).init().await?;

        let files = vec![
            "tickers20220309",
            // "tickers20220310",
            // "tickers20220311",
            // "tickers20220314",
            // "tickers20220315",
            // "tickers20220316",
        ];
        for file in files {
            if context.config.mongo_enabled() {
                context.persistence.drop_collection(file).await?;
            }
            if context.config.elasticsearch_enabled() {
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
        config.extra_put(KEY_EXTRA_DISABLE_MONGO, "disabled");
        // disable elasticsearch persistence
        config.extra_put(KEY_EXTRA_DISABLE_ELASTICSEARCH, "disabled");
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
            if context.config.mongo_enabled() {
                context.persistence.drop_collection(file).await?;
            }
            if context.config.elasticsearch_enabled() {
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

#[test]
fn test_sort() {
    let mut protfolios = vec![
        Protfolio {
            id: "1".to_string(),
            price: 0.0,
            time: 10,
            quote_type: QuoteType::Etf,
            market_hours: MarketHoursType::RegularMarket,
            volume: 0,
            change: 0.0,
            change_rate: 0.0,
            unit: TimeUnit::find("MovingMinuteOne").unwrap(),
            unit_time: 10,
            period_type: 0,
            max_price: 0.0,
            min_price: 0.0,
            open_price: 0.0,
            close_price: 0.0,
            sample_size: 0,
            slope: None,
        },
        Protfolio {
            id: "1".to_string(),
            price: 0.0,
            time: 20,
            quote_type: QuoteType::Etf,
            market_hours: MarketHoursType::RegularMarket,
            volume: 0,
            change: 0.0,
            change_rate: 0.0,
            unit: TimeUnit::find("MovingMinuteOne").unwrap(),
            unit_time: 20,
            period_type: 0,
            max_price: 0.0,
            min_price: 0.0,
            open_price: 0.0,
            close_price: 0.0,
            sample_size: 0,
            slope: None,
        },
    ];
    protfolios.sort_by(|x, y| x.unit_time.partial_cmp(&y.unit_time).unwrap());
    println!("asc: {:?}", &protfolios);

    protfolios.sort_by(|x, y| y.unit_time.partial_cmp(&x.unit_time).unwrap());
    println!("desc: {:?}", &protfolios);
}
