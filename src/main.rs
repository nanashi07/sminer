use clap::{Arg, Command};
use log::{debug, info};
use sminer::{
    analysis::{replay, ReplayMode},
    init_log,
    persist::{
        es::{index_protfolios_from_file, index_tickers_from_file, take_digitals},
        mongo::{export, import},
    },
    provider::yahoo::consume,
    vo::{
        biz::TimeUnit,
        core::{
            AppConfig, AppContext, KEY_EXTRA_DISABLE_ELASTICSEARCH, KEY_EXTRA_DISABLE_MONGO,
            KEY_EXTRA_DISABLE_TRUNCAT,
        },
    },
    Result,
};

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

                    let symbols = context.config.symbols();
                    let uri = &context.config.platform.yahoo.uri;

                    info!("Loaded symbols: {:?}", &symbols);
                    consume(&context, &uri, &symbols, Option::None).await?;
                }
                "replay" => {
                    // override first init
                    let disable_transfer_to_mongo = sub_matches.is_present("no-mongo");
                    let disable_transfer_to_elasticsearch =
                        sub_matches.is_present("no-elasticsearch");
                    info!(
                        "Transfer MongoDB: {}, transfer Elasticsearch: {}",
                        disable_transfer_to_mongo, disable_transfer_to_elasticsearch
                    );

                    if disable_transfer_to_mongo {
                        config.extra_put(KEY_EXTRA_DISABLE_MONGO, "disabled");
                    }
                    if disable_transfer_to_elasticsearch {
                        config.extra_put(KEY_EXTRA_DISABLE_ELASTICSEARCH, "disabled");
                    }
                    // config.extra_put("truncat", "true")

                    info!("Available tickers: {:?}", &config.symbols());
                    info!(
                        "Available time unit: {:?}",
                        TimeUnit::values()
                            .iter()
                            .map(|u| u.name.clone())
                            .collect::<Vec<_>>()
                    );

                    let context = AppContext::new(config).init().await?;

                    let files: Vec<&str> = sub_matches.values_of("files").unwrap().collect();
                    debug!("Input files: {:?}", files);

                    for file in files {
                        if context.config.mongo_enabled() {
                            context.persistence.drop_collection(file).await?;
                        }
                        if context.config.elasticsearch_enabled() {
                            context.persistence.drop_index(&take_digitals(file)).await?;
                        }
                        replay(&context, &file, ReplayMode::Sync).await?
                    }
                }
                "import" => {
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
                    let keep_exists_data = sub_matches.is_present("keep-data");
                    if keep_exists_data {
                        config.extra_put(KEY_EXTRA_DISABLE_TRUNCAT, "disabled");
                    }
                    let context = AppContext::new(config).init().await?;

                    let r#type = sub_matches.value_of("type").unwrap().to_lowercase();
                    debug!("Input type: {}", &r#type);
                    let files: Vec<&str> = sub_matches.values_of("files").unwrap().collect();
                    debug!("Input files: {:?}", files);

                    match r#type.as_str() {
                        "tickers" => {
                            for file in files {
                                index_tickers_from_file(&context, file).await?;
                            }
                        }
                        "protfolio" => {
                            for file in files {
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
                    Arg::new("files")
                        .takes_value(true)
                        .multiple_values(true)
                        .required(true)
                        .help("Source files to be replay"),
                    Arg::new("no-mongo")
                        .short('m')
                        .long("no-mongo")
                        .takes_value(false)
                        .help("Disable transfer to MongoDB"),
                    Arg::new("no-elasticsearch")
                        .short('e')
                        .long("no-elasticsearch")
                        .takes_value(false)
                        .help("Disable transfer to Elasticsearch"),
                ]),
            Command::new("import")
                .about("Import message into MongoDB collection")
                .args(&[
                    level.clone(),
                    config_file.clone(),
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
                    Arg::new("type")
                        .short('t')
                        .long("type")
                        .possible_values(["tickers", "protfolio"])
                        .default_value("protfolio")
                        .ignore_case(true),
                    Arg::new("keep-data")
                        .short('k')
                        .long("keep-data")
                        .takes_value(false)
                        .ignore_case(true),
                    Arg::new("files")
                        .takes_value(true)
                        .multiple_values(true)
                        .required(true)
                        .help("Source files to be indexed"),
                ]),
        ])
}
