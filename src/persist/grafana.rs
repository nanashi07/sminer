use crate::Result;
use hyper::{Body, Client};
use log::info;
use serde_json::{json, Value};

// https://grafana.com/docs/grafana/latest/http_api/annotations/

pub fn list_annotations() {}
pub async fn add_annotation() -> Result<()> {
    let value: Value = json!({
        "dashboardId": 1 as i32,
        "panelId": 2 as i32,
        "time": 1646852056000 as i64,
        "text": "222"
    });
    let json = value.to_string();
    info!("body = {}", json);
    let request = hyper::Request::builder()
        .uri("http://admin:password@localhost:8091/api/annotations")
        .method("POST")
        .header("Content-Type", "application/json")
        .body(Body::from(json))
        .unwrap();

    let client = Client::new();
    let response = client.request(request).await?;

    info!("response = {:?}", response);

    Ok(())
}
pub fn remove_annotation() {}
