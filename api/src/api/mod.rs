use log::{debug, warn};
use reqwest::blocking::Response;

//

use crate::structs::{APIClient, AppConfig};

//

pub mod binance;
pub mod coinmarketcap;
pub mod kraken;

//
//
//

fn _get(url: &str, client: &APIClient) -> Result<Response, reqwest::Error> {
    debug!("get - {}", url);
    client
        .client
        .get(url)
        .headers(client.headers.clone())
        .send()
}

//

pub fn client_get<'a>(
    name: &str,
    config: &'a AppConfig,
    headers: reqwest::header::HeaderMap,
) -> Result<APIClient<'a>, Box<dyn std::error::Error>> {
    let api_client_config = config
        .api_clients
        .get(name)
        .ok_or(format!("{} config not found", name))?;

    Ok(APIClient {
        client: reqwest::blocking::Client::builder()
            // .default_headers(headers)
            .timeout(std::time::Duration::from_secs(
                crate::ti_s(&config.api_timeout)? as u64,
            ))
            .connect_timeout(std::time::Duration::from_secs(
                crate::ti_s(&config.api_timeout)? as u64,
            ))
            .build()?,
        headers,
        url: &api_client_config.url,
        limit_requests: api_client_config.limit_requests,
        limit_period: &api_client_config.limit_period,
        limit_status_codes: &api_client_config.limit_status_codes,
        api_retries: config.api_retries,
        throttler: Vec::new(),
    })
}
//

pub fn request_get(
    client: &mut APIClient,
    url: &str,
) -> Result<String, Box<dyn std::error::Error>> {
    client.throttler_sleep()?;

    let time_start = crate::utc_ms()?;
    if let Ok(x) = retry::retry(
        retry::delay::Exponential::from_millis(10)
            .map(retry::delay::jitter)
            .take(client.api_retries),
        || match _get(url, &client) {
            Ok(response) if response.status().is_success() => retry::OperationResult::Ok(response),
            Ok(response)
                if client
                    .limit_status_codes
                    .contains(&response.status().as_u16()) =>
            {
                warn!("requests rate limit reached {}", url);
                retry::OperationResult::Err("request rate limit reached")
            }
            _ => retry::OperationResult::Retry(url),
        },
    ) {
        client.throttler_push()?;
        debug!(
            "get in {:.3}s - {:?} - {:?}",
            crate::td(time_start)?,
            &x.status(),
            url,
        );
        Ok(x.text()?)
    } else {
        warn!(
            "api get failed in {:.3}s - {:?}",
            crate::td(time_start)?,
            url
        );
        Err("api get failed".into())
    }
}
