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
    let cmd = command_args();
    let args = cmd.clone().get_matches();
    debug!("args: {:?}", args);

    if let Some(matches) = args.subcommand_matches("replay") {
        let files: Vec<&str> = matches.values_of("files").unwrap().collect();

        init_log("INFO").await?;
        let config = AppConfig::load("config.yaml")?;
        let context = AppContext::new(config).init().await?;

        for file in files {
            if &context.config.data_source.mongodb.enabled == &true {
                context.persistence.drop_collection(file).await?;
            }
            if &context.config.data_source.elasticsearch.enabled == &true {
                context.persistence.drop_index(&take_digitals(file)).await?;
            }
            replay(&context, &format!("tmp/{}", &file), ReplayMode::Sync).await?
        }

        return Ok(());
    }

    Ok(())
}
fn take_digitals(str: &str) -> String {
    str.chars().filter(|c| c.is_numeric()).collect::<String>()
}
/// Create command line arguments
fn command_args<'help>() -> Command<'help> {
    Command::new("sminer - Analysis and miner for stock infomation")
        .version("0.1.0")
        .author("Bruce Tsai")
        .subcommand_required(true)
        .subcommand(
            Command::new("replay").args(&[
                Arg::new("files")
                    .takes_value(true)
                    .multiple_values(true)
                    .required(true)
                    .help("Source files to be replay"),
                Arg::new("Store data to Mongo")
                    .long("mongo")
                    .possible_values(["true", "false"])
                    .default_value("true"),
            ]),
        )
        .args(&[
            Arg::new("Log level")
                .short('l')
                .long("level")
                .possible_values(["TRACE", "DEBUG", "INFO", "WARN", "ERROR"])
                .default_value("INFO")
                .help("Log level for standard output"),
            Arg::new("tests")
                .short('q')
                .long("qq")
                .help("Source files to be replay"),
        ])
}
