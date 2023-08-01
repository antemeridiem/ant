pub struct APIClient<'a> {
    pub client: reqwest::blocking::Client,
    pub headers: reqwest::header::HeaderMap,
    pub url: &'a str,
    pub limit_requests: usize,
    pub limit_period: &'a str,
    pub limit_status_codes: &'a Vec<u16>,
    pub api_retries: usize,
    pub throttler: Vec<u64>,
}

impl APIClient<'_> {
    pub fn throttler_push(&mut self) -> Result<(), Box<dyn std::error::Error>> {
        self.throttler.push(crate::utc_ms()?);
        Ok(())
    }
    pub fn throttler_sleep(&mut self) -> Result<(), Box<dyn std::error::Error>> {
        while self.throttler.len() >= self.limit_requests {
            std::thread::sleep(std::time::Duration::from_millis(
                crate::utc_ms()? - self.throttler[0],
            ));
            let threshold = crate::utc_ms()? - crate::ti_ms(self.limit_period)?;
            self.throttler.retain(|x| *x >= threshold)
        }

        Ok(())
    }
}

//

#[derive(Clone, Debug, serde::Deserialize, serde::Serialize)]
pub struct AppConfig {
    pub interval: String,
    pub api_retries: usize,
    pub api_timeout: String,
    pub history: HistoryConfig,
    pub trades: TradesConfig,
    pub api_clients: std::collections::HashMap<String, APIClientConfig>,
}

//

#[derive(Clone, Debug, serde::Deserialize, serde::Serialize)]
pub struct APIClientConfig {
    pub url: String,
    pub limit_requests: usize,
    pub limit_period: String,
    pub limit_status_codes: Vec<u16>,
}

//

#[derive(Clone, Debug, serde::Deserialize, serde::Serialize)]
pub struct HistoryConfig {
    pub spot_only: bool,
    pub quote_only: bool,
    pub quotes: Vec<String>,
    pub tradable_only: bool,
    pub fiat_removed: bool,
    pub stablecoins_removed: bool,
}

//

#[derive(Clone, Debug, serde::Deserialize, serde::Serialize)]
pub struct TradesConfig {
    pub quote_only: bool,
    pub quotes: Vec<String>,
    pub limit: u64,
    pub recvwindow: u64,
}

//

#[derive(Clone, Debug)]
pub struct DirEntry {
    pub path: std::path::PathBuf,
    pub stem: String,
    pub extension: String,
}

//

#[derive(Default)]
pub struct Pair {
    pub spot: bool,
    pub status: String,
    pub target: String,
    pub target_precision: usize,
    pub quote: String,
    pub quote_precision: usize,
    pub filter_quantity_min: f64,
    pub filter_quantity_max: f64,
    pub filter_step_size: f64,
    pub filter_notional_min: f64,
}
