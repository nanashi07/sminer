use clap::{Arg, Command};
use log::debug;
use sminer::Result;

#[tokio::main]
async fn main() -> Result<()> {
    let cmd = command_args();
    let args = cmd.clone().get_matches();
    debug!("args: {:?}", args);
    Ok(())
}

/// Create command line arguments
fn command_args<'help>() -> Command<'help> {
    Command::new("sminer - Analysis and miner for stock infomation")
        .version("0.1.0")
        .author("Bruce Tsai")
        .subcommand(
            Command::new("replay").args(&[
                Arg::new("Source files")
                    .short('s')
                    .long("source")
                    .takes_value(true)
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
