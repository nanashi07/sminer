use crate::{vo::biz::Ticker, Result};
use log::debug;

pub async fn rebalance(ticker: &Ticker) -> Result<()> {
    debug!("{:?}", ticker);
    // TODO: save mongo
    // TODO: save es
    // TODO: analysis
    Ok(())
}
