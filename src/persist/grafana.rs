use std::{collections::HashMap, thread};

use crate::Result;
use chrono::{DateTime, Utc};
use hyper::{Body, Client, Method};
use hyper_tls::HttpsConnector;
use log::{debug, info};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};

// https://grafana.com/docs/grafana/latest/http_api/annotations/

const DASHBOARD_ID: i64 = 4;
const URI_GRAFANA: &str = "http://localhost:8091/api/annotations";
const TOKEN_GRAFANA: &str = "Basic YWRtaW46cGFzc3dvcmQ=";
// .uri("http://admin:password@localhost:8091/api/annotations")

pub async fn list_annotations(
    from: Option<DateTime<Utc>>,
    to: Option<DateTime<Utc>>,
    dashboard_id: Option<i64>,
    panel_id: Option<i64>,
    tags: &Vec<String>,
) -> Result<Vec<Annotation>> {
    let mut params: Vec<(&str, String)> = Vec::new();
    if let Some(time) = from {
        params.push(("from", time.timestamp_millis().to_string()));
    }
    if let Some(time) = to {
        params.push(("to", time.timestamp_millis().to_string()));
    }
    if let Some(id) = dashboard_id {
        params.push(("dashboardId", id.to_string()));
    }
    if let Some(id) = panel_id {
        params.push(("dashboardId", id.to_string()));
    }
    for tag in tags {
        params.push(("tags", tag.to_owned()));
    }

    let query = params
        .iter()
        .map(|(name, value)| format!("{}={}", name, value)) // TODO: encode
        .collect::<Vec<String>>()
        .join("&");

    debug!("query: {}", &query);

    let request = hyper::Request::builder()
        .uri(format!("{}?{}", URI_GRAFANA, &query))
        .method(Method::GET)
        .header("Authorization", TOKEN_GRAFANA)
        .body(Body::empty())?;

    let client = Client::new();
    let response = client.request(request).await?;

    debug!("response = {:?}", &response);

    let buf = hyper::body::to_bytes(response.into_body()).await?;
    let annotations: Vec<Annotation> = serde_json::from_slice(&buf)?;

    debug!("body = {:?}", &annotations);

    Ok(annotations)
}

pub async fn add_annotation(
    time: &DateTime<Utc>,
    text: &str,
    tags: &Vec<String>,
    dashboard_id: i64,
    panel_id: i64,
) -> Result<()> {
    let value: Value = json!({
        "dashboardId": dashboard_id,
        "panelId": panel_id,
        "time": time.timestamp_millis(),
        "tags": tags,
        "text": text
    });
    debug!("add annotation body = {:?}", value);

    let request = hyper::Request::builder()
        .uri(URI_GRAFANA)
        .method(Method::POST)
        .header("Authorization", TOKEN_GRAFANA)
        .header("Content-Type", "application/json")
        .body(Body::from(value.to_string()))?;

    let https_connector = HttpsConnector::new();
    let client = Client::builder().build(https_connector);

    // let client = Client::new();
    let response = client.request(request).await?;

    debug!("add annotation response = {:?}", response);

    Ok(())
}

pub async fn remove_annotation(id: i32) -> Result<()> {
    let request = hyper::Request::builder()
        .uri(format!("{}/{}", URI_GRAFANA, &id))
        .method(Method::DELETE)
        .header("Authorization", TOKEN_GRAFANA)
        .body(Body::empty())?;

    let client = Client::new();
    let response = client.request(request).await?;

    debug!("remove annotation response = {:?}", response);

    Ok(())
}

pub fn add_order_annotation(
    symbol: String,
    time: DateTime<Utc>,
    text: String,
    tags: Vec<String>,
) -> Result<()> {
    let panel_map: HashMap<&str, i64> = [
        // ("TQQQ", 2),
        // ("SQQQ", 5),
        // ("SOXL", 3),
        // ("SOXS", 4),
        // ("SPXL", 6),
        // ("SPXS", 7),
        // ("LABU", 9),
        // ("LABD", 8),
        // ("TNA", 10),
        // ("TZA", 11),
        // ("YINN", 14),
        // ("YANG", 15),
        // ("UDOW", 12),
        // ("SDOW", 13),
        ("TQQQ", 1),
        ("SQQQ", 2),
        ("SOXL", 1),
        ("SOXS", 2),
        ("SPXL", 1),
        ("SPXS", 2),
        ("LABU", 1),
        ("LABD", 2),
        ("TNA", 1),
        ("TZA", 2),
        ("YINN", 1),
        ("YANG", 2),
        ("UDOW", 1),
        ("SDOW", 2),
    ]
    .iter()
    .cloned()
    .collect();

    let panel_id = *panel_map.get(symbol.as_str()).unwrap();

    // async to sync, need a new thread
    let handler = thread::spawn(move || {
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_time()
            .enable_io()
            .build()
            .unwrap();
        rt.block_on(add_annotation(&time, &text, &tags, DASHBOARD_ID, panel_id))
            .unwrap();
    });

    handler.join().unwrap();

    Ok(())
}

pub async fn clear_annotations(
    from: Option<DateTime<Utc>>,
    to: Option<DateTime<Utc>>,
    tags: &Vec<String>,
) -> Result<()> {
    let mut count = 1;
    while count > 0 {
        let annotations = list_annotations(from, to, None, None, tags).await?;
        count = annotations.len();
        for annotation in annotations {
            info!("Remove annotation: {}", &annotation.id);
            remove_annotation(annotation.id).await?;
        }
    }

    Ok(())
}
#[derive(Debug, Serialize, Deserialize)]
pub struct Annotation {
    pub id: i32,
    #[serde(rename = "alertId")]
    pub alert_id: i32,
    #[serde(rename = "alertName")]
    pub alert_name: String,
    #[serde(rename = "dashboardId")]
    pub dashboard_id: i32,
    #[serde(rename = "panelId")]
    pub panel_id: i32,
    #[serde(rename = "userId")]
    pub user_id: i32,
    #[serde(rename = "newState")]
    pub new_state: String,
    #[serde(rename = "prevState")]
    pub prev_state: String,
    pub created: i64,
    pub updated: i64,
    pub time: i64,
    #[serde(rename = "timeEnd")]
    pub time_end: i64,
    pub text: String,
    pub tags: Vec<String>,
    pub login: String,
    pub email: String,
    #[serde(rename = "avatarUrl")]
    pub avatar_url: String,
}
