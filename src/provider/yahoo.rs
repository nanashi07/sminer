use std::net::TcpStream;
use websocket::native_tls::TlsStream;
use websocket::{sync::Client, Message, OwnedMessage};

use crate::decoder::deserialize_yahoo_message;
use crate::persist::save_one;
use crate::vo::{SubscribeCommand, Ticker};

#[derive(Debug)]
pub enum HandleResult {
    NexMessage,
    LiveCheck(Vec<u8>),
}

pub fn send_subscribe(symbols: Vec<&str>, client: &mut Client<TlsStream<TcpStream>>) {
    let yoo = SubscribeCommand {
        subscribe: symbols.into_iter().map(|s| s.to_string()).collect(),
    };
    println!("build message = {:?}", &yoo);
    let subscribe = Message::text(serde_json::to_string(&yoo).unwrap());
    println!("websocket message: {:?}", &subscribe);
    let send_result = client.send_message(&subscribe);
    println!("send result = {:?}", send_result);
}

pub async fn handle_message(client: &mut Client<TlsStream<TcpStream>>) -> HandleResult {
    match client.recv_message() {
        Ok(message) => {
            match message {
                OwnedMessage::Text(text) => {
                    // println!("message = {}", text);
                    let message = deserialize_yahoo_message(&text);
                    println!("{:?}", &message);
                    let value = Ticker::from(message);
                    save_one(&value).await.unwrap();
                    println!("{}", serde_json::to_string(&value).unwrap());
                }
                OwnedMessage::Binary(_) => {
                    println!("receive binary");
                }
                OwnedMessage::Close(close_data) => {
                    println!("receive close {:?}", close_data);
                }
                OwnedMessage::Ping(data) => {
                    println!("receive ping");
                    return HandleResult::LiveCheck(data);
                }
                OwnedMessage::Pong(_) => {
                    println!("receive pong");
                }
            }
        }
        Err(error) => {
            println!("Receive message error {:?}", error);
        }
    }

    HandleResult::NexMessage
}
