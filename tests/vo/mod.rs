use config::Config;
use log::info;
use sminer::{init_log, vo::core::AppConfig, Result};

#[tokio::test]
async fn test_load_config() -> Result<()> {
    init_log("DEBUG").await?;
    let settings = Config::builder()
        // Add in `./Settings.toml`
        .add_source(config::File::with_name("./config.yaml"))
        // Add in settings from the environment (with a prefix of APP)
        // Eg.. `APP_DEBUG=1 ./target/app` would set the `debug` key
        // .add_source(config::Environment::with_prefix("APP"))
        .build()?;
    info!("settings = {:?}", &settings);

    let config: AppConfig = settings.try_deserialize::<AppConfig>()?;

    info!("config = {:?}", &config);

    Ok(())
}
