use crate::Result;
use chrono::{DateTime, Utc};
use hyper::{Body, Client, Method};
use log::debug;
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};

// https://grafana.com/docs/grafana/latest/http_api/annotations/

const URI_GRAFANA: &str = "http://localhost:8091/api/annotations";
const AUTH: &str = "Basic YWRtaW46cGFzc3dvcmQ=";
// .uri("http://admin:password@localhost:8091/api/annotations")

pub async fn list_annotations(
    from: &DateTime<Utc>,
    to: &DateTime<Utc>,
    dashboard_id: Option<i64>,
    panel_id: Option<i64>,
    tags: Option<Vec<String>>,
) -> Result<Vec<Annotation>> {
    let mut params = vec![
        ("from", from.timestamp_millis().to_string()),
        ("to", to.timestamp_millis().to_string()),
    ];
    if let Some(id) = dashboard_id {
        params.push(("dashboardId", id.to_string()));
    }
    if let Some(id) = panel_id {
        params.push(("dashboardId", id.to_string()));
    }
    if let Some(values) = tags {
        for tag in values {
            params.push(("tags", tag));
        }
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
        .header("Authorization", AUTH)
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
    tags: &Vec<&str>,
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
    debug!("body = {:?}", value);

    let request = hyper::Request::builder()
        .uri(URI_GRAFANA)
        .method(Method::POST)
        .header("Authorization", AUTH)
        .header("Content-Type", "application/json")
        .body(Body::from(value.to_string()))?;

    let client = Client::new();
    let response = client.request(request).await?;

    debug!("response = {:?}", response);

    Ok(())
}

pub async fn remove_annotation(id: i32) -> Result<()> {
    let request = hyper::Request::builder()
        .uri(format!("{}/{}", URI_GRAFANA, &id))
        .method(Method::DELETE)
        .header("Authorization", AUTH)
        .body(Body::empty())?;

    let client = Client::new();
    let response = client.request(request).await?;

    debug!("response = {:?}", response);

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
