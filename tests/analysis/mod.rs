use log::{error, info, warn};
use sminer::{
    analysis::{replay, trade::rebound_at, ReplayMode},
    init_log,
    persist::es::{take_index_time, ticker_index_name},
    vo::{
        biz::{MarketHoursType, Protfolio, QuoteType, TimeUnit, TradeInfo},
        core::{AppConfig, AppContext},
    },
    Result,
};
use std::{
    fs::File,
    io::{BufRead, BufReader},
    path::Path,
};
use tokio::runtime::Runtime;

// cargo test --package sminer --test tests -- analysis::test_replay --exact --nocapture --ignored
#[test]
#[ignore = "manually run only, replay from file"]
fn test_replay() -> Result<()> {
    let rt = Runtime::new()?;
    let result: Result<()> = rt.block_on(async {
        init_log("INFO").await?;
        let config = AppConfig::load("config.yaml")?;
        let context = AppContext::new(config).init().await?;
        let config = context.config();
        let persistence = context.persistence();

        let files = vec![
            // "tmp/json/split.tickers20220309.LABU-LABD.json",
            // "tmp/json/split.tickers20220309.SOXL-SOXS.json",
            // "tmp/json/split.tickers20220309.SPXL-SPXS.json",
            // "tmp/json/split.tickers20220309.TNA-TZA.json",
            "tmp/json/split.tickers20220309.TQQQ-SQQQ.json",
            // "tmp/json/split.tickers20220309.UDOW-SDOW.json",
            // "tmp/json/split.tickers20220309.YINN-YANG.json",
        ];
        for file in files {
            if config.sync_mongo_enabled() {
                persistence.drop_collection(file).await?;
            }
            if config.sync_elasticsearch_enabled() {
                let index_time = take_index_time(file);
                let index_name = ticker_index_name(&index_time);
                persistence.delete_index(&index_name).await?;
            }
            replay(&context, file, ReplayMode::Sync).await?
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
fn test_rexxxplay_async() -> Result<()> {
    let rt = Runtime::new()?;
    let result: Result<()> = rt.block_on(async {
        init_log("INFO").await?;
        let config = AppConfig::load("config.yaml")?;
        //config.extra_put(KEY_EXTRA_PRCOESS_IN_ASYNC, "async_mode"); // enable for save data
        let context = AppContext::new(config).init().await?;
        let config = context.config();
        let persistence = context.persistence();

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
            if config.sync_mongo_enabled() {
                persistence.drop_collection(file).await?;
            }
            if config.sync_elasticsearch_enabled() {
                let index_time = take_index_time(file);
                let index_name = ticker_index_name(&index_time);
                persistence.delete_index(&index_name).await?;
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
            kind: 'p',
            quote_type: QuoteType::Etf,
            market_hours: MarketHoursType::RegularMarket,
            volume: 0,
            unit: TimeUnit::new("f10s", 10, 0),
            unit_time: 10,
            period_type: 0,
            max_price: 0.0,
            min_price: 0.0,
            open_price: 0.0,
            close_price: 0.0,
            sample_size: 0,
            slope: None,
            b_num: None,
        },
        Protfolio {
            id: "1".to_string(),
            price: 0.0,
            time: 20,
            kind: 'p',
            quote_type: QuoteType::Etf,
            market_hours: MarketHoursType::RegularMarket,
            volume: 0,
            unit: TimeUnit::new("f10s", 10, 0),
            unit_time: 20,
            period_type: 0,
            max_price: 0.0,
            min_price: 0.0,
            open_price: 0.0,
            close_price: 0.0,
            sample_size: 0,
            slope: None,
            b_num: None,
        },
    ];
    protfolios.sort_by(|x, y| x.unit_time.partial_cmp(&y.unit_time).unwrap());
    println!("asc: {:?}", &protfolios);

    protfolios.sort_by(|x, y| y.unit_time.partial_cmp(&x.unit_time).unwrap());
    println!("desc: {:?}", &protfolios);
}

// cargo test --package sminer --test tests -- analysis::test_slope_check --exact --nocapture
#[test]
#[ignore = "manually run only"]
fn test_slope_check() -> Result<()> {
    let rt = Runtime::new()?;
    let result: Result<()> = rt.block_on(async {
        init_log("INFO").await?;
        let config = AppConfig::load("config.yaml")?;
        let context = AppContext::new(config).init().await?;
        let _config = context.config();
        let _persistence = context.persistence();

        let files = ["tmp/tmp//trades/tickers20220309/trade-TQQQ.json"];

        for file in files {
            info!("Load data from {}", file);

            if !Path::new(file).exists() {
                warn!("File {} not exists", file);
                continue;
            }

            let f = File::open(file)?;
            let reader = BufReader::new(f);
            let trades: Vec<TradeInfo> = reader
                .lines()
                .into_iter()
                .map(|w| w.unwrap())
                .map(|line| serde_json::from_str::<TradeInfo>(&line).unwrap())
                .filter(|trade| matches!(trade.market_hours, MarketHoursType::RegularMarket))
                .collect();

            for trade in trades {
                for (unit, slopes) in trade.states {
                    let rebound = rebound_at(&unit, &slopes);
                    info!(
                        "trade: {} / {:5} at {}, trend: {:?}, rebount at {} ({}/{}), source: {:?}",
                        trade.id,
                        unit,
                        trade.time,
                        rebound.trend,
                        rebound.rebound_at,
                        rebound.up_count,
                        rebound.down_count,
                        &slopes
                    );
                }
            }
        }

        Ok(())
    });
    if let Err(err) = result {
        error!("{}", err);
    }
    Ok(())
}
