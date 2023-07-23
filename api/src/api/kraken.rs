use log::info;
use reqwest::header::{HeaderMap, HeaderName, HeaderValue};
use std::collections::HashMap;

//

use crate::structs::APIClient;
use crate::structs::AppConfig;

//

// pub fn signature_get() {
//     // https://github.com/cbeck88/krakenrs/blob/master/src/kraken_rest_client.rs
// }

pub struct API<'a> {
    pub client: APIClient<'a>,
}

//

impl API<'_> {
    pub fn new(config: &AppConfig) -> Result<API, Box<dyn std::error::Error>> {
        let mut headers = HeaderMap::new();
        headers.insert(
            HeaderName::from_static("api-key"),
            HeaderValue::from_str(&std::env::var("KRAKEN_API_KEY")?)?,
        );

        Ok(API {
            client: crate::api::client_get("kraken", config, headers)?,
        })
    }

    //

    pub fn pairs_get(&mut self) -> Result<(), Box<dyn std::error::Error>> {
        let url = format!("{}/public/AssetPairs", self.client.url);
        let response = crate::api::request_get(&mut self.client, &url)?;
        let response_json: serde_json::Value = serde_json::from_str(&response)?;
        let pairs_data: HashMap<String, serde_json::Value> =
            serde_json::from_value(response_json["result"].clone())?;
        info!("number of pairs: {}", pairs_data.len());
        crate::config_write_json(&pairs_data, &crate::paths::file_pairs_kraken())?;

        Ok({})
    }
}

//
