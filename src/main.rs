use chrono::{DateTime, Duration, TimeZone, Utc};
use clap::{Arg, ArgMatches, Command};
use log::{debug, info};
use sminer::{
    analysis::{replay, ReplayMode},
    init_log,
    persist::{
        es::{
            index_protfolios_from_file, index_tickers_from_file, take_index_time, ticker_index_name,
        },
        grafana::clear_annotations,
        mongo::{export, import},
    },
    provider::yahoo::consume,
    vo::core::{
        AppConfig, AppContext, KEY_EXTRA_ENABLE_DATA_TRUNCAT, KEY_EXTRA_PRCOESS_IN_ASYNC,
        KEY_EXTRA_PRINT_TRADE_META_END_TIME, KEY_EXTRA_PRINT_TRADE_META_START_TIME,
    },
    Result,
};
use std::{collections::HashSet, sync::Arc};

#[tokio::main(worker_threads = 500)]
async fn main() -> Result<()> {
    let cmd = command_args();
    let matches = cmd.get_matches();

    match matches.subcommand() {
        Some((name, sub_matches)) => {
            let level = sub_matches.value_of("log-level").unwrap();
            let config_file = sub_matches.value_of("config-file").unwrap();
            init_log(&level).await?;

            debug!("matches: {:?}", sub_matches);

            // init
            let mut config = AppConfig::load(config_file)?;

            match name {
                "consume" => {
                    perform_consume(&mut config, sub_matches).await?;
                }
                "replay" => {
                    perform_replay(&mut config, sub_matches).await?;
                }
                "import" => {
                    perform_import(&mut config, sub_matches).await?;
                }
                "export" => {
                    perform_export(&mut config, sub_matches).await?;
                }
                "index" => {
                    perform_index(&mut config, sub_matches).await?;
                }
                "annotate" => {
                    perform_annotate(&mut config, sub_matches).await?;
                }
                _ => {}
            }
        }
        None => {
            // cmd.clone().print_help()?;
            println!();
        }
    }

    Ok(())
}

async fn perform_consume(config: &mut AppConfig, _sub_matches: &ArgMatches) -> Result<()> {
    // add additional config
    config.extra_put(KEY_EXTRA_PRCOESS_IN_ASYNC, "async_mode");
    let context = AppContext::new(config.to_owned()).init().await?;
    let config = context.config();

    let symbols = config.symbols();
    let units = config.time_units();
    let uri = config.platform.yahoo.uri.as_str();

    info!("Loaded symbols: {:?}", &symbols);
    info!(
        "Loaded time units: {:?}",
        &units.iter().map(|u| u.name.clone()).collect::<Vec<_>>()
    );
    consume(&context, &uri, &symbols).await?;

    Ok(())
}

async fn perform_replay(config: &mut AppConfig, sub_matches: &ArgMatches) -> Result<()> {
    let mut from: Option<DateTime<Utc>> = None;
    let mut to: Option<DateTime<Utc>> = None;

    if let Some(start) = sub_matches.value_of("print-meta-start-at") {
        if let Ok(time) = DateTime::parse_from_rfc3339(start) {
            from = Some(time.with_timezone(&Utc));
        }
        if let Ok(timestamp) = start.parse::<i64>() {
            from = Some(Utc.timestamp_millis(timestamp));
        }
    }

    if let Some(end) = sub_matches.value_of("print-meta-end-at") {
        if let Ok(time) = DateTime::parse_from_rfc3339(end) {
            to = Some(time.with_timezone(&Utc));
        }
        if let Ok(timestamp) = end.parse::<i64>() {
            to = Some(Utc.timestamp_millis(timestamp));
        }
    }

    if let Some(start) = from {
        config.extra_put(
            KEY_EXTRA_PRINT_TRADE_META_START_TIME,
            &start.timestamp_millis().to_string(),
        )
    }
    if let Some(end) = to {
        config.extra_put(
            KEY_EXTRA_PRINT_TRADE_META_END_TIME,
            &end.timestamp_millis().to_string(),
        )
    }

    let start_time = Utc::now().timestamp_millis();
    config_truncat(config, sub_matches)?;
    let context = AppContext::new(config.to_owned()).init().await?;
    let config = context.config();

    info!(
        "Available time unit: {:?}",
        config
            .time_units()
            .iter()
            .map(|u| u.name.clone())
            .collect::<Vec<_>>()
    );

    let files: Vec<&str> = sub_matches.values_of("files").unwrap().collect();
    debug!("Input files: {:?}", files);

    for file in files {
        // TODO: try renew context for release resource
        // compare to original if speed up
        replay(&context, &file, ReplayMode::Sync).await?
    }

    info!(
        "Replay time cost: {}",
        Duration::milliseconds(Utc::now().timestamp_millis() - start_time)
    );

    Ok(())
}

async fn perform_import(config: &mut AppConfig, sub_matches: &ArgMatches) -> Result<()> {
    config_truncat(config, sub_matches)?;

    let context = AppContext::new(config.to_owned()).init().await?;

    let files: Vec<&str> = sub_matches.values_of("files").unwrap().collect();
    debug!("Input files: {:?}", files);
    for file in files {
        import(&context, &file).await?;
    }

    Ok(())
}

async fn perform_export(config: &mut AppConfig, sub_matches: &ArgMatches) -> Result<()> {
    let context = AppContext::new(config.to_owned()).init().await?;

    let files: Vec<&str> = sub_matches.values_of("collections").unwrap().collect();
    debug!("Target collections: {:?}", files);
    for file in files {
        export(&context, &file).await?;
    }

    Ok(())
}

async fn perform_index(config: &mut AppConfig, sub_matches: &ArgMatches) -> Result<()> {
    config_truncat(config, sub_matches)?;

    let context = AppContext::new(config.to_owned()).init().await?;
    let config = context.config();
    let persistence = context.persistence();

    let r#type = sub_matches.value_of("type").unwrap().to_lowercase();
    debug!("Input type: {}", &r#type);
    let files: Vec<&str> = sub_matches.values_of("files").unwrap().collect();
    debug!("Input files: {:?}", files);

    let mut drop_history: HashSet<String> = HashSet::new();

    match r#type.as_str() {
        "ticker" => {
            for file in files {
                // drop index when fit name at first time
                let index_time = take_index_time(file);
                let index_name = ticker_index_name(&index_time);
                if config.truncat_enabled() && !drop_history.contains(&index_name) {
                    persistence.delete_index(&index_name).await?;
                    drop_history.insert(index_name);
                }

                index_tickers_from_file(&context, file).await?;
            }
        }
        "protfolio" => {
            for file in files {
                // drop index when fit name at first time
                let index_time = take_index_time(file);
                let index_name = ticker_index_name(&index_time);
                if config.truncat_enabled() && !drop_history.contains(&index_name) {
                    persistence.delete_index(&index_name).await?;
                    drop_history.insert(index_name);
                }

                index_protfolios_from_file(&context, file).await?;
            }
        }
        _ => {}
    }

    Ok(())
}

async fn perform_annotate(config: &mut AppConfig, sub_matches: &ArgMatches) -> Result<()> {
    let mut from: Option<DateTime<Utc>> = None;
    let mut to: Option<DateTime<Utc>> = None;

    if let Some(start) = sub_matches.value_of("start") {
        if let Ok(time) = DateTime::parse_from_rfc3339(start) {
            from = Some(time.with_timezone(&Utc));
        }
        if let Ok(timestamp) = start.parse::<i64>() {
            from = Some(Utc.timestamp_millis(timestamp));
        }
    }

    if let Some(end) = sub_matches.value_of("end") {
        if let Ok(time) = DateTime::parse_from_rfc3339(end) {
            to = Some(time.with_timezone(&Utc));
        }
        if let Ok(timestamp) = end.parse::<i64>() {
            to = Some(Utc.timestamp_millis(timestamp));
        }
    }

    let tags: Vec<String> = sub_matches
        .values_of("tag")
        .unwrap_or_default()
        .map(|s| s.to_owned())
        .collect();

    info!(
        "Clear annotations, start: {:?}, end: {:?}, tags: {:?}",
        from, to, &tags
    );

    let context = AppContext::new(config.to_owned()).init().await?;
    let config = context.config();

    clear_annotations(Arc::clone(&config), from, to, &tags).await?;

    Ok(())
}

fn config_truncat(config: &mut AppConfig, sub_matches: &ArgMatches) -> Result<()> {
    let truncat_data = sub_matches.is_present("truncat")
        && sub_matches
            .value_of("truncat")
            .unwrap()
            .to_lowercase()
            .parse::<bool>()?;
    if truncat_data {
        config.extra_put(KEY_EXTRA_ENABLE_DATA_TRUNCAT, "truncat");
    }

    Ok(())
}

/// Create command line arguments
fn command_args<'help>() -> Command<'help> {
    let level = Arg::new("log-level")
        .short('l')
        .long("level")
        .ignore_case(true)
        .possible_values(["TRACE", "DEBUG", "INFO", "WARN", "ERROR"])
        .default_value("INFO")
        .help("Log level for standard output");

    let config_file = Arg::new("config-file")
        .short('f')
        .long("config")
        .default_value("config.yaml")
        .help("Path of config file");

    Command::new("sminer - Analysis and miner for stock infomation")
        .version("0.1.0")
        .author("Bruce Tsai")
        .subcommand_required(true)
        .subcommands(vec![
            Command::new("consume")
                .about("Consume message for analysis")
                .args(&[level.clone(), config_file.clone()]),
            Command::new("replay")
                .about("Replay message for analysis")
                .args(&[
                    level.clone(),
                    config_file.clone(),
                    Arg::new("truncat")
                        .short('k')
                        .long("truncat")
                        .possible_values(["true", "false"])
                        .default_value("true")
                        .ignore_case(true)
                        .help("Truncat existing data"),
                    Arg::new("print-meta-start-at")
                        .long("print-meta-start-at")
                        .takes_value(true)
                        .required(false)
                        .help("Print meta of trades, start time"),
                    Arg::new("print-meta-end-at")
                        .long("print-meta-end-at")
                        .takes_value(true)
                        .required(false)
                        .help("Print meta of trades, end time"),
                    Arg::new("files")
                        .takes_value(true)
                        .multiple_values(true)
                        .required(true)
                        .help("Source files to be replay"),
                ]),
            Command::new("import")
                .about("Import message into MongoDB collection")
                .args(&[
                    level.clone(),
                    config_file.clone(),
                    Arg::new("truncat")
                        .short('k')
                        .long("truncat")
                        .possible_values(["true", "false"])
                        .default_value("true")
                        .ignore_case(true)
                        .help("Truncat existing data"),
                    Arg::new("files")
                        .takes_value(true)
                        .multiple_values(true)
                        .required(true)
                        .help("Source files to be import"),
                ]),
            Command::new("export")
                .about("Export message from MongoDB collection")
                .args(&[
                    level.clone(),
                    config_file.clone(),
                    Arg::new("collections")
                        .takes_value(true)
                        .multiple_values(true)
                        .required(true)
                        .help("Collection to be export"),
                ]),
            Command::new("index")
                .about("Index message to Elasticsearch")
                .args(&[
                    level.clone(),
                    config_file.clone(),
                    Arg::new("truncat")
                        .short('k')
                        .long("truncat")
                        .possible_values(["true", "false"])
                        .default_value("false")
                        .ignore_case(true)
                        .help("Truncat existing data"),
                    Arg::new("type")
                        .short('t')
                        .long("type")
                        .possible_values(["ticker", "protfolio"])
                        .default_value("protfolio")
                        .ignore_case(true),
                    Arg::new("files")
                        .takes_value(true)
                        .multiple_values(true)
                        .required(true)
                        .help("Source files to be indexed"),
                ]),
            Command::new("annotate")
                .about("Remove annotations from grafana")
                .args(&[
                    level.clone(),
                    config_file.clone(),
                    Arg::new("start")
                        .short('s')
                        .long("start")
                        .takes_value(true)
                        .required(false)
                        .help("Start time"),
                    Arg::new("end")
                        .short('e')
                        .long("end")
                        .takes_value(true)
                        .required(false)
                        .help("End time"),
                    Arg::new("tag")
                        .short('t')
                        .long("tag")
                        .takes_value(true)
                        .required(false)
                        .multiple_occurrences(true),
                ]),
        ])
}
