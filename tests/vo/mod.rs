use config::Config;
use log::info;
use sminer::{init_log, vo::core::AppConfig, Result};

#[tokio::test]
async fn test_load_config() -> Result<()> {
    init_log("DEBUG").await?;
    let settings = Config::builder()
        .add_source(config::File::with_name("./config.yaml"))
        .set_default("analysis.output.baseFolder", "tmp")?
        .set_default("dataSource.mongodb.target", "yahoo")?
        .build()?;
    info!("settings = {:?}", &settings);

    println!("");

    let config: AppConfig = settings.try_deserialize::<AppConfig>()?;
    info!("config = {:?}", &config);

    Ok(())
}
