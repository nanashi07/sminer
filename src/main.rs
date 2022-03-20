use clap::{Arg, Command};
use log::debug;
use sminer::{
    analysis::{replay, ReplayMode},
    init_log,
    vo::core::{AppConfig, AppContext},
    Result,
};

#[tokio::main]
async fn main() -> Result<()> {
    let mut cmd = command_args();
    let matches = cmd.clone().get_matches();
    debug!("matches: {:?}", matches);

    match matches.subcommand_name() {
        Some(name) => {
            let level: String = matches.value_of_t("level")?;
            init_log(&level).await?;

            debug!("Subommand: {}", &name);

            // init
            let config = AppConfig::load("config.yaml")?;
            let context = AppContext::new(config).init().await?;

            match name {
                "replay" => {
                    let files: Vec<&str> = matches.values_of("files").unwrap().collect();
                    debug!("Input files: {:?}", files);

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
                    // TODO
                }
                "export" => {
                    // TODO
                }
                "index" => {
                    // TODO
                }
                _ => {}
            }
        }
        None => {
            cmd.print_help()?;
            println!();
        }
    }

    Ok(())
}
fn take_digitals(str: &str) -> String {
    str.chars().filter(|c| c.is_numeric()).collect::<String>()
}
/// Create command line arguments
fn command_args<'help>() -> Command<'help> {
    let level = Arg::new("level")
        .short('l')
        .long("level")
        .possible_values(["TRACE", "DEBUG", "INFO", "WARN", "ERROR"])
        .default_value("INFO")
        .help("Log level for standard output");

    Command::new("sminer - Analysis and miner for stock infomation")
        .version("0.1.0")
        .author("Bruce Tsai")
        .subcommand_required(true)
        .subcommands(vec![
            Command::new("replay")
                .about("Replay message for analysis")
                .args(&[
                    level.clone(),
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
                    Arg::new("files")
                        .takes_value(true)
                        .multiple_values(true)
                        .required(true)
                        .help("Collection to be export"),
                ]),
            Command::new("index")
                .about("Index message to Elasticsearch")
                .args(&[
                    level.clone(),
                    Arg::new("files")
                        .takes_value(true)
                        .multiple_values(true)
                        .required(true)
                        .help("Source files to be indexed"),
                ]),
        ])
}
