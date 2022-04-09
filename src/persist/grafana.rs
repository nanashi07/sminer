use crate::{vo::core::AppConfig, Result};
use chrono::{DateTime, Utc};
use hyper::{Body, Client, Method};
use hyper_tls::HttpsConnector;
use log::{debug, info};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use std::{collections::HashMap, sync::Arc, thread};

// https://grafana.com/docs/grafana/latest/http_api/annotations/

pub async fn get_dashboard(config: Arc<AppConfig>, uid: &str) -> Result<Dashboard> {
    let request = hyper::Request::builder()
        .uri(format!(
            "{}/api/dashboards/uid/{}",
            config.data_source.grafana.uri, uid
        ))
        .method(Method::GET)
        .header(
            "Authorization",
            config.data_source.grafana.auth.as_ref().unwrap(),
        )
        .body(Body::empty())?;

    let client = Client::new();
    let response = client.request(request).await?;

    debug!("response = {:?}", &response);

    let buf = hyper::body::to_bytes(response.into_body()).await?;
    let response: DashboardResponse = serde_json::from_slice(&buf)?;

    debug!("body = {:?}", &response);

    Ok(response.dashboard)
}

pub async fn list_annotations(
    config: Arc<AppConfig>,
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
        .uri(format!(
            "{}/api/annotations?{}",
            config.data_source.grafana.uri, &query
        ))
        .method(Method::GET)
        .header(
            "Authorization",
            config.data_source.grafana.auth.as_ref().unwrap(),
        )
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
    config: Arc<AppConfig>,
    time: &DateTime<Utc>,
    text: &str,
    tags: &Vec<String>,
    dashboard_id: i64,
    panel_id: i64,
) -> Result<()> {
    if !config.data_source.grafana.enabled {
        debug!("grafana is disabled");
        return Ok(());
    }

    let value: Value = json!({
        "dashboardId": dashboard_id,
        "panelId": panel_id,
        "time": time.timestamp_millis(),
        "tags": tags,
        "text": text
    });
    debug!("add annotation body = {:?}", value);

    let request = hyper::Request::builder()
        .uri(format!(
            "{}/api/annotations",
            config.data_source.grafana.uri
        ))
        .method(Method::POST)
        .header(
            "Authorization",
            config.data_source.grafana.auth.as_ref().unwrap(),
        )
        .header("Content-Type", "application/json")
        .body(Body::from(value.to_string()))?;

    let https_connector = HttpsConnector::new();
    let client = Client::builder().build(https_connector);

    // let client = Client::new();
    let response = client.request(request).await?;

    debug!("add annotation response = {:?}", response);

    Ok(())
}

pub async fn remove_annotation(config: Arc<AppConfig>, id: i32) -> Result<()> {
    let request = hyper::Request::builder()
        .uri(format!(
            "{}/api/annotations/{}",
            config.data_source.grafana.uri, &id
        ))
        .method(Method::DELETE)
        .header(
            "Authorization",
            config.data_source.grafana.auth.as_ref().unwrap(),
        )
        .body(Body::empty())?;

    let client = Client::new();
    let response = client.request(request).await?;

    debug!("remove annotation response = {:?}", response);

    Ok(())
}

pub fn add_order_annotation(
    config: Arc<AppConfig>,
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
        rt.block_on(async {
            let c = Arc::clone(&config);
            let uid = c.data_source.grafana.target.as_ref().unwrap();
            if !c.extra_present("grafana_dashboard_id") {
                let dashboard = get_dashboard(Arc::clone(&config), &uid).await.unwrap();
                c.extra_put("grafana_dashboard_id", &format!("{}", dashboard.id));
            }

            let dashboard_id: i64 = c
                .extra_get("grafana_dashboard_id")
                .unwrap()
                .parse()
                .unwrap();

            add_annotation(
                Arc::clone(&config),
                &time,
                &text,
                &tags,
                dashboard_id,
                panel_id,
            )
            .await
            .unwrap();
        });
    });

    handler.join().unwrap();

    Ok(())
}

pub async fn clear_annotations(
    config: Arc<AppConfig>,
    from: Option<DateTime<Utc>>,
    to: Option<DateTime<Utc>>,
    tags: &Vec<String>,
) -> Result<()> {
    let mut count = 1;
    while count > 0 {
        let annotations = list_annotations(Arc::clone(&config), from, to, None, None, tags).await?;
        count = annotations.len();
        for annotation in annotations {
            info!("Remove annotation: {}", &annotation.id);
            remove_annotation(Arc::clone(&config), annotation.id).await?;
        }
    }

    Ok(())
}

#[derive(Debug, Serialize, Deserialize)]
pub struct Dashboard {
    pub id: i64,
    pub uid: String,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct DashboardResponse {
    pub dashboard: Dashboard,
    pub meta: DashboardMeta,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct DashboardMeta {}

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
