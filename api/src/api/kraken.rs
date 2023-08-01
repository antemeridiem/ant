use base64::{
    alphabet,
    engine::{self, general_purpose},
    Engine as _,
};
use hmac::{Hmac, Mac};
use log::{debug, info};
use polars::prelude::*;
use reqwest::header::{HeaderMap, HeaderName, HeaderValue};
use sha2::Digest;
use std::collections::HashMap;

//

use crate::structs::APIClient;
use crate::structs::AppConfig;

//


pub struct API<'a> {
    pub label: &'a str,
    pub client: APIClient<'a>,
}

//

impl API<'_> {
    pub fn new(config: &AppConfig) -> Result<API, Box<dyn std::error::Error>> {
        let mut headers = HeaderMap::new();
        headers.insert(
            HeaderName::from_static("api-key"),
            // "API-Key",
            HeaderValue::from_str(&std::env::var("KRAKEN_API_KEY")?)?,
        );
        let label = "kraken";

        Ok(API {
            label,
            client: crate::api::client_get(label, config, headers)?,
        })
    }

    //

    pub fn pairs_get(&mut self) -> Result<(), Box<dyn std::error::Error>> {
        let url = format!("{}/0/public/AssetPairs", self.client.url);
        let response = crate::api::request_get(&mut self.client, crate::api::Request::Get(&url))?;
        let response_json: serde_json::Value = serde_json::from_str(&response)?;
        let pairs_data: HashMap<String, serde_json::Value> =
            serde_json::from_value(response_json["result"].clone())?;
        info!("number of pairs: {}", pairs_data.len());
        crate::config_write_json(&pairs_data, &crate::paths::file_pairs_kraken())?;

        Ok({})
    }

    //

    pub fn trades_get(&mut self) -> Result<(), Box<dyn std::error::Error>> {
        let dir_path = crate::paths::dir_trades().join(self.label);
        debug!(
            "{} trades target directory: {}",
            self.label,
            &dir_path.as_path().display()
        );
        crate::paths::dir_create(&dir_path);
        let file_path = dir_path.join(format!("trades.feather"));
        debug!("trades file_path is {}", file_path.as_path().display());

        let mut ts_last = 0.0;
        let mut trades_previous = LazyFrame::default();
        if file_path.exists() {
            trades_previous = crate::feather_read(&file_path)?;
            ts_last = crate::column_maxu(trades_previous.clone(), "time")? as f64 / 1000.0
        };

        let argument = if ts_last > 0.0 { "start" } else { "end" };
        let mut trades_new = Vec::new();
        let mut n_trades_new: u64 = 0;

        debug!("{}", ts_last);
        let mut batch = self.trades_batch_get(
            argument,
            if ts_last > 0.0 {
                ts_last
            } else {
                crate::utc_ms()? as f64 / 1000.0
            },
        )?;
        n_trades_new += batch.height() as u64;
        while batch.height() > 0 {
            debug!("{:?}", batch.clone());
            trades_new.push(
                batch
                    .clone()
                    .lazy()
                    .with_column(lit(crate::utc_ms()?).alias("recorded_at")),
            );
            {
                let ts_last_new = if argument == "start" {
                    crate::column_maxu(batch.clone().lazy(), "time")? as f64 / 1000.0
                } else {
                    crate::column_minu(batch.clone().lazy(), "time")? as f64 / 1000.0
                };
                if (ts_last == ts_last_new) & (batch.height() == 1) {
                    break;
                }
            }
            batch = self.trades_batch_get(argument, ts_last)?;
            n_trades_new += batch.height() as u64;
        }

        if trades_new.len() > 0 {
            trades_new.insert(0, trades_previous);
            let mut output = concat(trades_new, true, true)?
                .unique_stable(
                    Some(Vec::from(["tradetxid".to_string()])),
                    UniqueKeepStrategy::First,
                )
                .sort_by_exprs([col("time"), col("tradetxid")], [false, false], false)
                .collect()?;
            crate::feather_write(&mut output, &file_path)?;
        }
        info!("number of new trades for kraken is {}", n_trades_new);

        Ok(())
    }

    //

    pub fn trades_batch_get(
        &mut self,
        argument: &str,
        ts: f64,
    ) -> Result<DataFrame, Box<dyn std::error::Error>> {
        let nonce = crate::utc_ms()?;
        let uri = format!("/0/private/TradesHistory");
        let params = format!("nonce={}&trades=false&{}={}", nonce, argument, ts);
        self.signature_get(&uri, &params, nonce)?;

        let url = format!("{}{}", self.client.url, uri);
        let response =
            crate::api::request_get(&mut self.client, crate::api::Request::Post((&url, &params)))?;

        Ok(trades_deserialize(&response)?)
    }

    //

    fn signature_get(
        &mut self,
        uri: &str,
        params: &str,
        nonce: u64,
    ) -> Result<(), Box<dyn std::error::Error>> {
        let key_secret = std::env::var("KRAKEN_API_SECRET")?;
        let key_secret_decoded = general_purpose::STANDARD.decode(key_secret)?;
        let mut hmac = Hmac::<sha2::Sha512>::new_from_slice(&key_secret_decoded)?;
        let params_sha2 = {
            let mut output = sha2::Sha256::default();
            output.update(nonce.to_string());
            output.update(&params);
            output.finalize()
        };
        hmac.update(uri.as_bytes());
        hmac.update(&params_sha2);
        let hmac = hmac.finalize().into_bytes();
        let signature = general_purpose::STANDARD.encode(hmac);

        self.client.headers.insert(
            HeaderName::from_static("api-sign"),
            // "API-Sign",
            HeaderValue::from_str(&signature)?,
        );

        Ok(())
    }
}

//

#[derive(serde::Deserialize)]
struct Trade {
    #[serde(alias = "ordertxid")]
    ordertxid: String,
    #[serde(default, alias = "postxid")]
    postxid: String,
    #[serde(alias = "pair")]
    pair: String,
    #[serde(alias = "time")]
    time: f64,
    #[serde(alias = "type")]
    r#type: String,
    #[serde(alias = "ordertype")]
    ordertype: String,
    #[serde(alias = "price")]
    price: String,
    #[serde(alias = "cost")]
    cost: String,
    #[serde(alias = "fee")]
    fee: String,
    #[serde(alias = "vol")]
    vol: String,
    #[serde(alias = "margin")]
    margin: String,
    #[serde(alias = "leverage")]
    leverage: String,
    #[serde(alias = "misc")]
    misc: String,
    #[serde(alias = "trade_id")]
    trade_id: u64,
}

//

fn trades_deserialize(response: &str) -> Result<DataFrame, Box<dyn std::error::Error>> {
    let rows: serde_json::Value = serde_json::from_str(&response)?;
    let rows: HashMap<String, Trade> = serde_json::from_value(rows["result"]["trades"].clone())?;

    let mut tradetxid: Vec<String> = Vec::new();
    let mut ordertxid: Vec<String> = Vec::new();
    let mut postxid: Vec<String> = Vec::new();
    let mut pair: Vec<String> = Vec::new();
    let mut time: Vec<u64> = Vec::new();
    let mut r#type: Vec<String> = Vec::new();
    let mut ordertype: Vec<String> = Vec::new();
    let mut price: Vec<String> = Vec::new();
    let mut cost: Vec<String> = Vec::new();
    let mut fee: Vec<String> = Vec::new();
    let mut vol: Vec<String> = Vec::new();
    let mut margin: Vec<String> = Vec::new();
    let mut leverage: Vec<String> = Vec::new();
    let mut misc: Vec<String> = Vec::new();
    let mut trade_id: Vec<u64> = Vec::new();

    for (key, row) in rows.iter() {
        tradetxid.push(key.clone());
        ordertxid.push(row.ordertxid.clone());
        postxid.push(row.postxid.clone());
        pair.push(row.pair.clone());
        time.push((row.time * 1000.0) as u64);
        r#type.push(row.r#type.clone());
        ordertype.push(row.ordertype.clone());
        price.push(row.price.clone());
        cost.push(row.cost.clone());
        fee.push(row.fee.clone());
        vol.push(row.vol.clone());
        margin.push(row.margin.clone());
        leverage.push(row.leverage.clone());
        misc.push(row.misc.clone());
        trade_id.push(row.trade_id);
    }

    Ok(
        DataFrame::new(Vec::from([
            Series::new("tradetxid", tradetxid),
            Series::new("ordertxid", ordertxid),
            Series::new("postxid", postxid),
            Series::new("pair", pair),
            Series::new("time", time),
            Series::new("type", r#type),
            Series::new("ordertype", ordertype),
            Series::new("price", price),
            Series::new("cost", cost),
            Series::new("fee", fee),
            Series::new("vol", vol),
            Series::new("margin", margin),
            Series::new("leverage", leverage),
            Series::new("misc", misc),
            Series::new("trade_id", trade_id),
        ]))?
        .sort(["time"], false)?, // .lazy()
                                 // .with_columns([
                                 //     col("price").cast(DataType::Decimal(None, Some(8))),
                                 //     col("cost").cast(DataType::Decimal(None, Some(8))),
                                 //     col("fee").cast(DataType::Decimal(None, Some(8))),
                                 //     col("vol").cast(DataType::Decimal(None, Some(8))),
                                 //     col("margin").cast(DataType::Decimal(None, Some(8))),
                                 //     col("leverage").cast(DataType::Decimal(None, Some(8))),
                                 // ])
                                 // .collect()?
    )
}
