use log::info;
use reqwest::header::{HeaderMap, HeaderName, HeaderValue};
use std::collections::HashMap;

//

use crate::structs::APIClient;
use crate::structs::AppConfig;

//

pub struct API<'a> {
    pub client: APIClient<'a>,
}

//

impl API<'_> {
    pub fn new(config: &AppConfig) -> Result<API, Box<dyn std::error::Error>> {
        let mut headers = HeaderMap::new();
        headers.insert(
            HeaderName::from_static("x-cmc_pro_api_key"),
            HeaderValue::from_str(&std::env::var("COINMARKETCAP_API_KEY")?)?,
        );
        headers.insert(
            HeaderName::from_static("accepts"),
            HeaderValue::from_static("application/json"),
        );

        Ok(API {
            client: crate::api::client_get("coinmarketcap", config, headers)?,
        })
    }

    //

    pub fn fiat_get(&mut self) -> Result<(), Box<dyn std::error::Error>> {
        let url = format!("{}/v1/fiat/map?limit=5000", self.client.url);
        let response = crate::api::request_get(&mut self.client, &url)?;
        let response_json: serde_json::Value = serde_json::from_str(&response)?;
        let response_data: Vec<serde_json::Value> =
            serde_json::from_value(response_json["data"].clone())?;
        let fiat_data = response_data
            .into_iter()
            .map(|x| {
                Ok((
                    x.get("symbol")
                        .ok_or("symbol not found")?
                        .as_str()
                        .ok_or("str not found")?
                        .to_string(),
                    x,
                ))
            })
            .collect::<Result<HashMap<String, serde_json::Value>, Box<dyn std::error::Error>>>()?;
        info!("number of fiat: {}", fiat_data.len());
        crate::config_write_json(&fiat_data, &crate::paths::file_fiat())?;

        Ok(())
    }

    //

    pub fn stablecoins_get(&mut self) -> Result<(), Box<dyn std::error::Error>> {
        let url = format!(
            "{}/v1/cryptocurrency/category?id=604f2753ebccdd50cd175fc1&limit=1000",
            self.client.url
        );
        let response = crate::api::request_get(&mut self.client, &url)?;
        let response_json: serde_json::Value = serde_json::from_str(&response)?;
        let response_data: Vec<serde_json::Value> =
            serde_json::from_value(response_json["data"]["coins"].clone())?;
        let stablecoins_data = response_data
            .into_iter()
            .map(|x| {
                Ok((
                    x.get("symbol")
                        .ok_or("symbol not found")?
                        .as_str()
                        .ok_or("str not found")?
                        .to_string(),
                    x,
                ))
            })
            .collect::<Result<HashMap<String, serde_json::Value>, Box<dyn std::error::Error>>>()?;
        info!("number of stablecoins: {}", stablecoins_data.len());
        crate::config_write_json(&stablecoins_data, &crate::paths::file_stablecoins())?;

        Ok(())
    }
}
