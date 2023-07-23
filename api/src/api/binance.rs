use log::{debug, info};
use polars::prelude::*;
use reqwest::header::{HeaderMap, HeaderName, HeaderValue};
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

#[derive(Debug, serde::Deserialize)]
struct Pair {
    #[serde(alias = "baseAsset")]
    target: String,
    //
    #[serde(alias = "baseAssetPrecision")]
    target_precision: usize,
    //
    #[serde(alias = "quoteAsset")]
    quote: String,
    //
    #[serde(alias = "quoteAssetPrecision")]
    quote_precision: usize,
    //
    #[serde(alias = "quoteAsset")]
    filters: Vec<HashMap<String, serde_json::Value>>,
    //
    #[serde(alias = "isSpotTradingAllowed")]
    spot: bool,
    //
    #[serde(alias = "status")]
    status: String,
}

//

impl API<'_> {
    pub fn new(config: &AppConfig) -> Result<API, Box<dyn std::error::Error>> {
        let mut headers = HeaderMap::new();
        headers.insert(
            HeaderName::from_static("x-mbx-apikey"),
            HeaderValue::from_str(&std::env::var("BINANCE_API_KEY")?)?,
        );

        let label = "binance";
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
        let url = format!("{}/exchangeInfo", self.client.url);
        let response = crate::api::request_get(&mut self.client, &url)?;
        let response_json: serde_json::Value = serde_json::from_str(&response)?;
        let response_data: Vec<serde_json::Value> =
            serde_json::from_value(response_json["symbols"].clone())?;
        let pairs_data = response_data
            .into_iter()
            .map(|x| {
                (
                    x.get("symbol")
                        .expect("symbol not found")
                        .as_str()
                        .expect("str not found")
                        .to_string(),
                    x,
                )
            })
            .collect::<HashMap<String, serde_json::Value>>();
        info!("number of pairs: {}", pairs_data.len());
        crate::config_write_json(&pairs_data, &crate::paths::file_pairs_binance())?;

        let pairs_binance = pairs_data
            .into_iter()
            .map(|(k, v)| Ok((k, serde_json::from_value(v)?)))
            .collect::<Result<HashMap<String, Pair>, Box<dyn std::error::Error>>>()?;
        let mut pairs = HashMap::new();

        for (pair_binance_label, pair_binance_data) in pairs_binance {
            let mut pair_data = crate::structs::Pair {
                spot: pair_binance_data.spot,
                status: pair_binance_data.status,
                target: pair_binance_data.target,
                target_precision: pair_binance_data.target_precision,
                quote: pair_binance_data.quote,
                quote_precision: pair_binance_data.quote_precision,
                ..Default::default()
            };

            for filter in pair_binance_data.filters.iter() {
                match filter
                    .get("filterType")
                    .ok_or("filter type not found")?
                    .as_str()
                    .ok_or("str not found")?
                {
                    "LOT_SIZE" => {
                        pair_data.filter_quantity_min = filter
                            .get("minQty")
                            .ok_or("min quantity not found")?
                            .as_str()
                            .ok_or("minqty as_str failed")?
                            .parse::<f64>()?;
                        pair_data.filter_quantity_max = filter
                            .get("maxQty")
                            .ok_or("max quantity not found")?
                            .as_str()
                            .ok_or("maxqty as_str failed")?
                            .parse::<f64>()?;
                        pair_data.filter_step_size = filter
                            .get("stepSize")
                            .ok_or("step size not found")?
                            .as_str()
                            .ok_or("stepsize as_str failed")?
                            .parse::<f64>()?;
                    }
                    "NOTIONAL" => {
                        pair_data.filter_notional_min = filter
                            .get("minNotional")
                            .ok_or("minnotional not found")?
                            .as_str()
                            .ok_or("minnotional as_str failed")?
                            .parse::<f64>()?
                    }
                    _ => {}
                }
            }

            pairs.insert(pair_binance_label, pair_data);
        }
        info!("number of pairs: {}", pairs.len());
        self.pairs = pairs;

        Ok(())
    }

    //

    pub fn history_get(&mut self) -> Result<(), Box<dyn std::error::Error>> {
        info!("{} history started", self.label);

        let dir_path = crate::paths::dir_klines().join(self.label);
        debug!(
            "{} history target directory: {}",
            self.label,
            &dir_path.as_path().display()
        );
        crate::paths::dir_create(&dir_path);

        let fiat: HashMap<String, serde_json::Value> =
            crate::json_read(&crate::paths::file_fiat())?;
        let stablecoins: HashMap<String, serde_json::Value> =
            crate::json_read(&&crate::paths::file_stablecoins())?;

        let pairs = self
            .pairs
            .iter()
            .filter(|(_, v)| {
                if self.config_app.history.spot_only {
                    v.spot
                } else {
                    true
                }
            })
            .filter(|(_, v)| {
                if self.config_app.history.quote_only {
                    v.quote == self.config_app.quote
                } else {
                    true
                }
            })
            .filter(|(_, v)| {
                if self.config_app.history.tradable_only {
                    v.status == "TRADING"
                } else {
                    true
                }
            })
            .filter(|(_, v)| {
                if self.config_app.history.fiat_removed {
                    !fiat.contains_key(&v.target)
                } else {
                    true
                }
            })
            .filter(|(_, v)| {
                if self.config_app.history.stablecoins_removed {
                    !stablecoins.contains_key(&v.target)
                } else {
                    true
                }
            })
            .map(|(k, _)| k.clone())
            .collect::<Vec<String>>();

        debug!("number of pairs to get history for: {}", pairs.len());
        for pair in pairs {
            self.pair_history_get(&pair)?;
        }

        info!("Mature pairs");
        crate::pairs_mature(self.label)?;
        info!("History finished");

        Ok(())
    }

    //

    fn pair_history_get(&mut self, pair: &str) -> Result<(), Box<dyn std::error::Error>> {
        let file_path = crate::paths::dir_klines()
            .join(self.label)
            .join(format!("{pair}.feather"));
        debug!("{} file_path is {}", pair, file_path.as_path().display());

        let lf = if file_path.is_file() {
            info!("previous data exists for {}", pair);
            let lf_old = crate::feather_read(&file_path)?;
            let ts_last_available = crate::column_maxu(lf_old.clone(), "ts")? as u32;
            debug!("last available for {} is {}", pair, ts_last_available);
            let lf_new = self.klines_history_get(pair, ts_last_available)?.select([
                col("ts"),
                col("open"),
                col("high"),
                col("low"),
                col("close"),
                col("quote asset volume"),
                col("number of trades"),
            ]);
            concat([lf_old, lf_new], true, true)?
                .unique_stable(
                    Some(Vec::from(["ts".to_string()])),
                    UniqueKeepStrategy::First,
                )
                .sort("ts", Default::default())
        } else {
            debug!("previous data does not exists for {}", pair);
            self.klines_history_get(pair, 0)?.select([
                col("ts"),
                col("open"),
                col("high"),
                col("low"),
                col("close"),
                col("quote asset volume"),
                col("number of trades"),
            ])
        };
        let mut df = crate::timestamps_missing(lf, &self.config_app.interval)?
            .sort("ts", Default::default())
            .collect()?;

        crate::feather_write(&mut df, &file_path)?;
        info!("finished {}: {} x {}", pair, &df.shape().0, &df.shape().1);

        Ok(())
    }

    //

    fn klines_history_get(
        &mut self,
        pair: &str,
        ts_last_available: u32,
    ) -> Result<LazyFrame, Box<dyn std::error::Error>> {
        let mut lf = self.klines_get(pair, Some(1000), None)?.lazy();

        let mut df = lf.clone().select([col("ts")]).collect()?;

        let mut tss = df
            .column("ts")?
            .u32()?
            .clone()
            .into_iter()
            .map(|x| Ok(x.ok_or("ts not found")?))
            .collect::<Result<Vec<u32>, Box<dyn std::error::Error>>>()?;

        let mut ts = tss[0];

        info!(
            "{} {} from {}, nrows {}",
            pair,
            self.config_app.interval,
            crate::unix_s_to_time(ts)?,
            df.shape().0
        );

        while (df.shape().0 > 1) & !tss.contains(&ts_last_available) {
            let chunk = self
                .klines_get(pair, Some(1000), Some(ts as u64 * 1000))?
                .lazy();

            df = chunk.clone().select([col("ts")]).collect()?;

            tss = df
                .column("ts")?
                .u32()?
                .clone()
                .into_iter()
                .map(|x| Ok(x.ok_or("ts not found")?))
                .collect::<Result<Vec<u32>, Box<dyn std::error::Error>>>()?;

            ts = tss[0];

            info!(
                "{} {} from {}, nrows {}",
                pair,
                self.config_app.interval,
                crate::unix_s_to_time(ts)?,
                df.shape().0
            );

            lf = concat([lf, chunk], true, true)?;
        } // while

        Ok(lf
            .unique_stable(
                Some(Vec::from(["ts".to_string()])),
                UniqueKeepStrategy::First,
            )
            .sort("ts", Default::default()))
    }

    //

    pub fn klines_get(
        &mut self,
        pair: &str,
        limit: Option<usize>,
        end_time: Option<u64>,
    ) -> Result<DataFrame, Box<dyn std::error::Error>> {
        //

        let mut url = format!(
            "{}/klines?symbol={}&interval={}",
            self.client.url, pair, self.config_app.interval,
        );
        if let Some(x) = limit {
            url = format!("{}&limit={}", url, x);
        }
        if let Some(x) = end_time {
            url = format!("{}&endTime={}", url, x);
        }
        let response = crate::api::request_get(&mut self.client, &url)?;
        Ok(klines_deserialize(&response)?
            .lazy()
            .with_column(lit(pair).alias("pair"))
            .collect()?)

        //
    }
}

//

fn klines_deserialize(response: &str) -> Result<DataFrame, Box<dyn std::error::Error>> {
    let rows: Vec<(
        u64,    // Kline open time
        String, // Open price
        String, // High price
        String, // Low price
        String, // Close price
        String, // Volume
        u64,    // Kline Close time
        String, // Quote asset volume
        u32,    // Number of trades
        String, // Taker buy base asset volume
        String, // Taker buy quote asset volume
        String, // Unused field, ignore.
    )> = serde_json::from_str(&response)?;

    let mut ts: Vec<u32> = Vec::new();
    let mut open: Vec<f32> = Vec::new();
    let mut high: Vec<f32> = Vec::new();
    let mut low: Vec<f32> = Vec::new();
    let mut close: Vec<f32> = Vec::new();
    let mut volume: Vec<f32> = Vec::new();
    let mut trades: Vec<u32> = Vec::new();

    for row in rows.iter() {
        ts.push((row.0 / 1000).try_into()?);
        open.push(row.1.parse()?);
        high.push(row.2.parse()?);
        low.push(row.3.parse()?);
        close.push(row.4.parse()?);
        volume.push(row.7.parse()?);
        trades.push(row.8);
    }

    Ok(DataFrame::new(Vec::from([
        Series::new("ts", ts),
        Series::new("open", open),
        Series::new("high", high),
        Series::new("low", low),
        Series::new("close", close),
        Series::new("quote asset volume", volume),
        Series::new("number of trades", trades),
    ]))?)
}
