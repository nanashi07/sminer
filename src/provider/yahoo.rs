use crate::{
    provider::decoder::deserialize_yahoo_message,
    vo::{
        biz::{MarketHoursType, SubscribeCommand, Ticker},
        core::AppContext,
    },
    Result,
};
use chrono::Utc;
use log::{debug, error, info, warn};
use std::{error::Error, fmt::Display, net::TcpStream, sync::Arc, time::Duration};
use tokio::time::sleep;
use websocket::{
    header::{Headers, UserAgent},
    native_tls::TlsStream,
    sync::Client,
    ClientBuilder, Message, OwnedMessage,
};

#[derive(Debug)]
pub enum HandleResult {
    NexMessage,
    LiveCheck(Vec<u8>),
}

#[derive(Debug)]
struct SourceError;

impl Display for SourceError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "Handle message source error")
    }
}
impl Error for SourceError {}
unsafe impl Sync for SourceError {}
unsafe impl Send for SourceError {}

pub async fn create_websocket_client(address: &str) -> Result<Client<TlsStream<TcpStream>>> {
    let mut headers = Headers::new();
    headers.set(UserAgent("Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/99.0.4844.51 Safari/537.36".to_owned()));
    let client = ClientBuilder::new(address)
        .unwrap()
        .custom_headers(&headers)
        .connect_secure(None)
        .unwrap();
    Ok(client)
}

pub async fn send_subscribe(
    symbols: &Vec<String>,
    client: &mut Client<TlsStream<TcpStream>>,
) -> Result<()> {
    let command = SubscribeCommand {
        subscribe: symbols.into_iter().map(|s| s.to_string()).collect(),
    };
    info!("Subscribe yahoo finance ticker = {:?}", &command);
    let subscribe = Message::text(serde_json::to_string(&command).unwrap());
    debug!("Websocket message: {:?}", &subscribe);
    let send_result = client.send_message(&subscribe);
    debug!("Subscribe result = {:?}", send_result);
    Ok(())
}

pub async fn consume(context: &Arc<AppContext>, addr: &str, symbols: &Vec<String>) -> Result<()> {
    let mut client = create_websocket_client(addr).await?;
    send_subscribe(symbols, &mut client).await?;

    let mut connected = true;
    let asset = context.asset();

    // TODO: recover from previous process

    loop {
        if connected {
            match handle_message(&Arc::clone(&context), &mut client).await {
                Ok(HandleResult::NexMessage) => {
                    continue;
                }
                Ok(HandleResult::LiveCheck(data)) => {
                    pong(&mut client, data).await?;
                }
                Err(err) => {
                    error!("Handle Yahoo Finance! message error: {:?}", err);
                    client.shutdown().unwrap_or_default();
                    connected = false;
                }
            }
        } else {
            // delay connect for few millis
            let reconnect_delay = 200;
            sleep(Duration::from_millis(reconnect_delay)).await;
            // reconnect
            info!("Reconnecting websocket: {}", addr);
            client = create_websocket_client(addr).await?;
            send_subscribe(&symbols, &mut client).await?;
            connected = true;
        }

        if asset.consumer_closable(Utc::now().timestamp_millis()) {
            info!(
                "Reach the expected end time {:?}, stop receiving message from Yahoo Finance!",
                Utc::now().to_rfc3339()
            );
            break;
        }
    }
    Ok(())
}

async fn pong(client: &mut Client<TlsStream<TcpStream>>, data: Vec<u8>) -> Result<HandleResult> {
    client.send_message(&OwnedMessage::Pong(data))?;
    debug!("Send pong to Yahoo Finance!");
    Ok(HandleResult::NexMessage)
}

async fn handle_message(
    context: &Arc<AppContext>,
    client: &mut Client<TlsStream<TcpStream>>,
) -> Result<HandleResult> {
    match client.recv_message()? {
        OwnedMessage::Text(text) => {
            let now = Utc::now().timestamp_millis();
            debug!("Receive: {}", text);

            let message = deserialize_yahoo_message(&text)?;
            debug!("Deserialize: {:?}", &message);

            let time_diff = now - message.time;
            let mut value = Ticker::from(message);
            value.time_diff = time_diff;

            // check time
            if true || value.market_hours == MarketHoursType::RegularMarket {
                if time_diff > 1000 && time_diff < 2000 {
                    info!(
                        "time diff 1~2s, [{}] {:?} = {}",
                        &value.id, &value.market_hours, &value.time_diff
                    );
                } else if time_diff >= 2000 && time_diff < 5000 {
                    warn!(
                        "time diff 2~5s, [{}] {:?} = {}",
                        &value.id, &value.market_hours, &value.time_diff
                    );
                } else if time_diff >= 5000 {
                    error!(
                        "time diff > 5s, [{}] {:?} = {}",
                        &value.id, &value.market_hours, &value.time_diff
                    );
                }
            }

            // dispatch ticker
            context.dispatch(&value).await?;
            if log::log_enabled!(log::Level::Debug) {
                debug!("Ticker: {}", serde_json::to_string(&value).unwrap());
            }
        }
        OwnedMessage::Binary(_) => {
            warn!("Receive binary from Yahoo Finance!");
        }
        OwnedMessage::Close(close_data) => {
            warn!("Receive close ({:?}) from Yahoo Finance!", close_data);
            return Err(Box::new(SourceError {}));
        }
        OwnedMessage::Ping(data) => {
            debug!("Receive ping from Yahoo Finance!");
            return Ok(HandleResult::LiveCheck(data));
        }
        OwnedMessage::Pong(_) => {
            warn!("Receive pong from Yahoo Finance!");
        }
    }

    Ok(HandleResult::NexMessage)
}
