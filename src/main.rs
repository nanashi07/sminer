use clap::{Arg, Command};
use log::{debug, info};
use sminer::{
    analysis::{replay, ReplayMode},
    init_log,
    persist::{
        es::{index_tickers, take_digitals},
        mongo::{export, import},
    },
    provider::yahoo::consume,
    vo::core::{AppConfig, AppContext},
    Result,
};

#[tokio::main]
async fn main() -> Result<()> {
    let cmd = command_args();
    let matches = cmd.get_matches();

    match matches.subcommand() {
        Some((name, sub_matches)) => {
            let level = sub_matches.value_of("log-level").unwrap();
            init_log(&level).await?;

            debug!("matches: {:?}", sub_matches);

            // init
            let config = AppConfig::load("config.yaml")?;
            let context = AppContext::new(config).init().await?;

            match name {
                "consume" => {
                    let symbols = context.config.symbols();
                    let uri = &context.config.platform.yahoo.uri;

                    info!("Loaded symbols: {:?}", &symbols);
                    consume(&context, &uri, &symbols, Option::None).await?;
                }
                "replay" => {
                    let files: Vec<&str> = sub_matches.values_of("files").unwrap().collect();
                    debug!("Input files: {:?}", files);
                    // let mongo = sub_matches.is_present("mongo");
                    // let elasticsearch = sub_matches.is_present("elasticsearch");
                    // config.data_source.mongodb.enabled = mongo;

                    for file in files {
                        if &context.config.data_source.mongodb.enabled == &true {
                            context.persistence.drop_collection(file).await?;
                        }
                        if &context.config.data_source.elasticsearch.enabled == &true {
                            context.persistence.drop_index(&take_digitals(file)).await?;
                        }
                        replay(&context, &file, ReplayMode::Sync).await?
                    }
                }
                "import" => {
                    let files: Vec<&str> = sub_matches.values_of("files").unwrap().collect();
                    debug!("Input files: {:?}", files);
                    for file in files {
                        import(&context, &file).await?;
                    }
                }
                "export" => {
                    let files: Vec<&str> = sub_matches.values_of("collections").unwrap().collect();
                    debug!("Target collections: {:?}", files);
                    for file in files {
                        export(&context, &file).await?;
                    }
                }
                "index" => {
                    let r#type = sub_matches.value_of("type").unwrap().to_lowercase();
                    debug!("Input type: {}", &r#type);
                    let files: Vec<&str> = sub_matches.values_of("files").unwrap().collect();
                    debug!("Input files: {:?}", files);

                    match r#type.as_str() {
                        "tickers" => {
                            for file in files {
                                index_tickers(&context, file).await?;
                            }
                        }
                        "protfolio" => {
                            // for file in files {
                            //     // export(&context, &file).await?;
                            // }
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

    Command::new("sminer - Analysis and miner for stock infomation")
        .version("0.1.0")
        .author("Bruce Tsai")
        .subcommand_required(true)
        .subcommands(vec![
            Command::new("consume")
                .about("Consume message for analysis")
                .args(&[level.clone()]),
            Command::new("replay")
                .about("Replay message for analysis")
                .args(&[
                    level.clone(),
                    Arg::new("files")
                        .takes_value(true)
                        .multiple_values(true)
                        .required(true)
                        .help("Source files to be replay"),
                    Arg::new("mongo")
                        .short('m')
                        .long("mongo")
                        .takes_value(false)
                        .help("Enable transfer to MongoDB"),
                    Arg::new("elasticsearch")
                        .short('e')
                        .long("elasticsearch")
                        .takes_value(false)
                        .help("Enable transfer to Elasticsearch"),
                ]),
            Command::new("import")
                .about("Import message into MongoDB collection")
                .args(&[
                    level.clone(),
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
