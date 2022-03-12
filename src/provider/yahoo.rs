use chrono::TimeZone;
use chrono::Utc;
use log::{debug, error, info, warn};
use std::net::TcpStream;
use websocket::native_tls::TlsStream;
use websocket::ClientBuilder;
use websocket::{sync::Client, Message, OwnedMessage};

use crate::provider::decoder::deserialize_yahoo_message;
use crate::vo::{SubscribeCommand, Ticker};
use crate::Result;

#[derive(Debug)]
pub enum HandleResult {
    NexMessage,
    LiveCheck(Vec<u8>),
}

pub async fn create_websocket_client(address: &str) -> Result<Client<TlsStream<TcpStream>>> {
    let client = ClientBuilder::new(address)
        .unwrap()
        .connect_secure(None)
        .unwrap();
    Ok(client)
}

pub async fn send_subscribe(
    symbols: Vec<&str>,
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

pub async fn consume(addr: &str, symbols: Vec<&str>, end_time: Option<i64>) -> Result<()> {
    let mut client = create_websocket_client(addr).await?;
    send_subscribe(symbols, &mut client).await?;
    loop {
        match handle_message(&mut client).await {
            Ok(HandleResult::NexMessage) => {
                continue;
            }
            Ok(HandleResult::LiveCheck(data)) => {
                pong(&mut client, data).await?;
            }
            Err(err) => {
                error!("Handle Yahoo Finance! message error: {:?}", err);
            }
        }
        if let Some(time) = end_time {
            if Utc::now().timestamp() > time {
                info!(
                    "Reach the expected end time {:?}, stop receiving message from Yahoo Finance!",
                    Utc.timestamp(time, 0)
                );
                break;
            }
        }
    }
    Ok(())
}

async fn pong(client: &mut Client<TlsStream<TcpStream>>, data: Vec<u8>) -> Result<HandleResult> {
    client.send_message(&OwnedMessage::Pong(data))?;
    debug!("Send pong to Yahoo Finance!");
    Ok(HandleResult::NexMessage)
}

async fn handle_message(client: &mut Client<TlsStream<TcpStream>>) -> Result<HandleResult> {
    match client.recv_message() {
        Ok(message) => match message {
            OwnedMessage::Text(text) => {
                debug!("Receive: {}", text);
                let message = deserialize_yahoo_message(&text)?;
                debug!("Deserialize: {:?}", &message);
                let value = Ticker::from(message);
                value.save_to_mongo().await?;
                info!("Ticker: {}", serde_json::to_string(&value).unwrap());
            }
            OwnedMessage::Binary(_) => {
                warn!("Receive binary from Yahoo Finance!");
            }
            OwnedMessage::Close(close_data) => {
                warn!("Receive close {:?} from Yahoo Finance!", close_data);
            }
            OwnedMessage::Ping(data) => {
                debug!("Receive ping from Yahoo Finance!");
                return Ok(HandleResult::LiveCheck(data));
            }
            OwnedMessage::Pong(_) => {
                warn!("Receive pong from Yahoo Finance!");
            }
        },
        Err(err) => {
            error!("Receive Yahoo Finance! error = {:?}", err);
        }
    }

    Ok(HandleResult::NexMessage)
}
