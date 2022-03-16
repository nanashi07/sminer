use crate::proto::yahoo::YahooTicker;
use crate::Result;
use base64::decode;
use prost::Message;

pub fn deserialize_yahoo_message(yahoo_message: &str) -> Result<YahooTicker> {
    // decode
    let debuf = decode(yahoo_message).unwrap();
    // conver to slice
    let buf = &debuf[..];
    // decode form protobuf
    Ok(YahooTicker::decode(buf)?)
}
