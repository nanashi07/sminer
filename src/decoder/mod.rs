use base64::decode;
use prost::Message;

use crate::proto::YahooTicker;

pub fn deserialize_yahoo_message(yahoo_message: &str) -> YahooTicker {
    // decode
    let debuf = decode(yahoo_message).unwrap();
    // conver to slice
    let buf = &debuf[..];
    // decode form protobuf
    return YahooTicker::decode(buf).unwrap();
}
