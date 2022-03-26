use crate::{
    vo::core::{AppConfig, AssetContext},
    Result,
};
use log::{info, warn};
use std::sync::Arc;

pub fn prepare_trade(
    asset: Arc<AssetContext>,
    _config: Arc<AppConfig>,
    message_id: i64,
) -> Result<()> {
    if let Some(lock) = asset.search_slope(message_id) {
        let value = lock.read().unwrap();
        info!("Trade info: {:?}", value);
        // TODO: check trade
    } else {
        warn!("No trade info for message ID: {} found!", &message_id);
    }
    Ok(())
}
