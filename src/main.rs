use clap::{Arg, ArgMatches, Command};
use log::{debug, info};
use sminer::{
    analysis::{replay, ReplayMode},
    init_log,
    persist::{
        es::{
            index_protfolios_from_file, index_tickers_from_file, take_index_time, ticker_index_name,
        },
        mongo::{export, import},
    },
    provider::yahoo::consume,
    vo::{
        biz::TimeUnit,
        core::{AppConfig, AppContext, KEY_EXTRA_ENABLE_DATA_TRUNCAT, KEY_EXTRA_PRCOESS_IN_REPLAY},
    },
    Result,
};
use std::collections::HashSet;

#[tokio::main]
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
                    let context = AppContext::new(config).init().await?;
                    let config = context.config();

                    let symbols = config.symbols();
                    let uri = config.platform.yahoo.uri.as_str();

                    info!("Loaded symbols: {:?}", &symbols);
                    consume(&context, &uri, &symbols, Option::None).await?;
                }
                "replay" => {
                    // add additional config
                    config.extra_put(KEY_EXTRA_PRCOESS_IN_REPLAY, "replay");
                    config_truncat(&mut config, sub_matches)?;

                    info!(
                        "Available time unit: {:?}",
                        TimeUnit::values()
                            .iter()
                            .map(|u| u.name.clone())
                            .collect::<Vec<_>>()
                    );

                    let context = AppContext::new(config).init().await?;
                    let config = context.config();

                    let files: Vec<&str> = sub_matches.values_of("files").unwrap().collect();
                    debug!("Input files: {:?}", files);

                    for file in files {
                        // delete mongo tickers
                        if config.sync_mongo_enabled() {
                            context.persistence.drop_collection(file).await?;
                        }
                        // delete elasticsearch tickers
                        if config.sync_elasticsearch_enabled() {
                            let index_time = take_index_time(file);
                            let index_name = ticker_index_name(&index_time);
                            context.persistence.delete_index(&index_name).await?;
                        }
                        replay(&context, &file, ReplayMode::Sync).await?
                    }
                }
                "import" => {
                    config_truncat(&mut config, sub_matches)?;

                    let context = AppContext::new(config).init().await?;

                    let files: Vec<&str> = sub_matches.values_of("files").unwrap().collect();
                    debug!("Input files: {:?}", files);
                    for file in files {
                        import(&context, &file).await?;
                    }
                }
                "export" => {
                    let context = AppContext::new(config).init().await?;

                    let files: Vec<&str> = sub_matches.values_of("collections").unwrap().collect();
                    debug!("Target collections: {:?}", files);
                    for file in files {
                        export(&context, &file).await?;
                    }
                }
                "index" => {
                    config_truncat(&mut config, sub_matches)?;

                    let context = AppContext::new(config).init().await?;
                    let config = context.config();
                    let persistence = context.persistence();

                    let r#type = sub_matches.value_of("type").unwrap().to_lowercase();
                    debug!("Input type: {}", &r#type);
                    let files: Vec<&str> = sub_matches.values_of("files").unwrap().collect();
                    debug!("Input files: {:?}", files);

                    let mut drop_history: HashSet<String> = HashSet::new();

                    match r#type.as_str() {
                        "tickers" => {
                            for file in files {
                                // drop index when fit name at first time
                                let index_time = take_index_time(file);
                                let index_name = ticker_index_name(&index_time);
                                if config.truncat_enabled() && drop_history.contains(&index_name) {
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
                                if config.truncat_enabled() && drop_history.contains(&index_name) {
                                    persistence.delete_index(&index_name).await?;
                                    drop_history.insert(index_name);
                                }

                                index_protfolios_from_file(&context, file).await?;
                            }
                        }
                        _ => {}
                    }
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
                        .possible_values(["tickers", "protfolio"])
                        .default_value("protfolio")
                        .ignore_case(true),
                    Arg::new("files")
                        .takes_value(true)
                        .multiple_values(true)
                        .required(true)
                        .help("Source files to be indexed"),
                ]),
        ])
}
