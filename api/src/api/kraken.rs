use base64::{engine::general_purpose, Engine as _};
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
    pub config_app: &'a AppConfig,
    pub pairs: HashMap<String, crate::structs::Pair>,
}

//

impl API<'_> {
    pub fn new(config: &AppConfig) -> Result<API, Box<dyn std::error::Error>> {
        let mut headers = HeaderMap::new();
        headers.insert(
            HeaderName::from_static("api-key"),
            HeaderValue::from_str(&std::env::var("KRAKEN_API_KEY")?)?,
        );
        let label = "kraken";
        let mut api = API {
            label,
            client: crate::api::client_get(label, config, headers)?,
            config_app: config,
            pairs: HashMap::new(),
        };
        api.pairs_get()?;

        Ok(api)
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

        // TODO: deserialize pairs data

        // self.pairs = pairs;
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

        let mut batch = self.trades_batch_get(
            argument,
            if ts_last > 0.0 {
                ts_last
            } else {
                crate::utc_ms()? as f64 / 1000.0
            },
        )?;
        while batch.height() > 0 {
            n_trades_new += batch.height() as u64;
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
                } else {
                    ts_last = ts_last_new;
                }
            }
            batch = self.trades_batch_get(argument, ts_last)?;
        }

        if trades_new.len() > 0 {
            trades_new.insert(0, trades_previous);
            let mut output = concat(trades_new, true, true)?
                .unique_stable(
                    Some(Vec::from(["txid".to_string()])),
                    UniqueKeepStrategy::First,
                )
                .sort_by_exprs([col("time"), col("txid")], [false, false], false)
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




    pub fn withdrawals_get(&mut self) -> Result<(), Box<dyn std::error::Error>> {
        let dir_path = crate::paths::dir_withdrawals().join(self.label);
        debug!(
            "{} withdrawals target directory: {}",
            self.label,
            &dir_path.as_path().display()
        );
        crate::paths::dir_create(&dir_path);
        let file_path = dir_path.join(format!("withdrawals.feather"));
        debug!("withdrawals file_path is {}", file_path.as_path().display());

        let mut ts_last = 0.0;
        let mut withdrawals_previous = LazyFrame::default();
        if file_path.exists() {
            withdrawals_previous = crate::feather_read(&file_path)?;
            ts_last = crate::column_maxu(withdrawals_previous.clone(), "time")? as f64 / 1000.0
        };

        let argument = if ts_last > 0.0 { "start" } else { "end" };
        let mut withdrawals_new = Vec::new();
        let mut n_withdrawals_new: u64 = 0;

        let mut batch = self.withdrawals_batch_get(
            argument,
            if ts_last > 0.0 {
                ts_last
            } else {
                crate::utc_ms()? as f64 / 1000.0
            },
        )?;
        while batch.height() > 0 {
            n_withdrawals_new += batch.height() as u64;
            withdrawals_new.push(
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
                } else {
                    ts_last = ts_last_new;
                }
            }
            batch = self.withdrawals_batch_get(argument, ts_last)?;
        }

        if withdrawals_new.len() > 0 {
            withdrawals_new.insert(0, withdrawals_previous);
            let mut output = concat(withdrawals_new, true, true)?
                .unique_stable(
                    Some(Vec::from(["ledger_id".to_string()])),
                    UniqueKeepStrategy::First,
                )
                .sort_by_exprs([col("time"), col("ledger_id")], [false, false], false)
                .collect()?;
            crate::feather_write(&mut output, &file_path)?;
        }
        info!("number of new withdrawals for kraken is {}", n_withdrawals_new);

        Ok(())
    }

    //

    pub fn withdrawals_batch_get(
        &mut self,
        argument: &str,
        ts: f64,
    ) -> Result<DataFrame, Box<dyn std::error::Error>> {
        let nonce = crate::utc_ms()?;
        let uri = format!("/0/private/Ledgers");
        let params = format!(
            "nonce={}&trades=false&type=withdrawal&{}={}",
            nonce, argument, ts
        );
        self.signature_get(&uri, &params, nonce)?;

        let url = format!("{}{}", self.client.url, uri);
        let response =
            crate::api::request_get(&mut self.client, crate::api::Request::Post((&url, &params)))?;

        Ok(withdrawals_deserialize(&response)?)
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

    let mut txid: Vec<String> = Vec::new();
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
        txid.push(key.clone());
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
            Series::new("txid", txid),
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

//

#[derive(serde::Deserialize)]
struct Withdrawal {
    #[serde(alias = "refid")]
    refid: String,
    #[serde(default, alias = "time")]
    time: f64,
    #[serde(alias = "type")]
    r#type: String,
    #[serde(alias = "subtype")]
    subtype: String,
    #[serde(alias = "aclass")]
    aclass: String,
    #[serde(alias = "asset")]
    asset: String,
    #[serde(alias = "amount")]
    amount: String,
    #[serde(alias = "fee")]
    fee: String,
    #[serde(alias = "balance")]
    balance: String,
}

//

fn withdrawals_deserialize(response: &str) -> Result<DataFrame, Box<dyn std::error::Error>> {
    let rows: serde_json::Value = serde_json::from_str(&response)?;
    let rows: HashMap<String, Withdrawal> =
        serde_json::from_value(rows["result"]["ledger"].clone())?;

    let mut ledger_id: Vec<String> = Vec::new();
    let mut refid: Vec<String> = Vec::new();
    let mut time: Vec<u64> = Vec::new();
    let mut r#type: Vec<String> = Vec::new();
    let mut subtype: Vec<String> = Vec::new();
    let mut aclass: Vec<String> = Vec::new();
    let mut asset: Vec<String> = Vec::new();
    let mut amount: Vec<String> = Vec::new();
    let mut fee: Vec<String> = Vec::new();
    let mut balance: Vec<String> = Vec::new();

    for (key, row) in rows.iter() {
        ledger_id.push(key.clone());
        refid.push(row.refid.clone());
        time.push((row.time * 1000.0) as u64);
        r#type.push(row.r#type.clone());
        subtype.push(row.subtype.clone());
        aclass.push(row.aclass.clone());
        asset.push(row.asset.clone());
        amount.push(row.amount.clone());
        fee.push(row.fee.clone());
        balance.push(row.balance.clone());
    }

    Ok(
        DataFrame::new(Vec::from([
            Series::new("ledger_id", ledger_id),
            Series::new("refid", refid),
            Series::new("time", time),
            Series::new("type", r#type),
            Series::new("subtype", subtype),
            Series::new("aclass", aclass),
            Series::new("asset", asset),
            Series::new("amount", amount),
            Series::new("fee", fee),
            Series::new("balance", balance),
        ]))?
        .sort(["time"], false)?, // .lazy()
                                 // .with_columns([
                                 //     col("amount").cast(DataType::Decimal(None, Some(8))),
                                 //     col("fee").cast(DataType::Decimal(None, Some(8))),
                                 //     col("balance").cast(DataType::Decimal(None, Some(8))),
                                 // ])
                                 // .collect()?
    )
}
