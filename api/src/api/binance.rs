use hmac::{Hmac, Mac};
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
        let url = format!("{}/api/v3/exchangeInfo", self.client.url);
        let response = crate::api::request_get(&mut self.client, crate::api::Request::Get(&url))?;
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
                    self.config_app.history.quotes.contains(&v.quote)
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

        let n_pairs = pairs.len();
        debug!("number of pairs to get history for: {}", n_pairs);
        for (index, pair) in pairs.iter().enumerate() {
            info!("{} / {} - {}", index + 1, n_pairs, pair);
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
        let mut df = crate::timestamps_missing(lf, &self.config_app.history.interval)?
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
            self.config_app.history.interval,
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
                self.config_app.history.interval,
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
            "{}/api/v3/klines?symbol={}&interval={}",
            self.client.url, pair, self.config_app.history.interval,
        );
        if let Some(x) = limit {
            url = format!("{}&limit={}", url, x);
        }
        if let Some(x) = end_time {
            url = format!("{}&endTime={}", url, x);
        }
        let response = crate::api::request_get(&mut self.client, crate::api::Request::Get(&url))?;
        Ok(klines_deserialize(&response)?
            .lazy()
            .with_column(lit(pair).alias("pair"))
            .collect()?)

        //
    }

    //

    pub fn trades_get(&mut self) -> Result<(), Box<dyn std::error::Error>> {
        info!("{} trades started", self.label);

        let dir_path = crate::paths::dir_trades().join(self.label);
        debug!(
            "{} trades target directory: {}",
            self.label,
            &dir_path.as_path().display()
        );
        crate::paths::dir_create(&dir_path);

        let file_path_conversions =
            crate::paths::dir_trades().join(format!("conversions-{}.csv", self.label));
        let (conversion_pairs, conversions) = if file_path_conversions.exists() {
            let mut schema: Schema = Schema::new();
            schema.with_column("id".into(), DataType::UInt64);
            schema.with_column("orderid".into(), DataType::UInt64);
            schema.with_column("price".into(), DataType::Utf8);
            schema.with_column("qty".into(), DataType::Utf8);
            schema.with_column("quoteqty".into(), DataType::Utf8);
            schema.with_column("commission".into(), DataType::Utf8);
            schema.with_column("time".into(), DataType::UInt32);
            schema.with_column("recorded_at".into(), DataType::UInt32);
            let conversions = crate::csv_read(&file_path_conversions, Some(schema))?;
            let conversion_pairs = conversions
                .clone()
                .collect()?
                .column("symbol")?
                .utf8()?
                .into_iter()
                .map(|x| Ok(x.ok_or("pair not found")?.to_string()))
                .collect::<Result<Vec<String>, Box<dyn std::error::Error>>>()?;
            info!("conversion pairs are {:?}", conversion_pairs);
            (conversion_pairs, conversions)
        } else {
            (Vec::default(), LazyFrame::default())
        };

        let pairs = self
            .pairs
            .iter()
            .filter(|(_, v)| {
                if self.config_app.trades.quote_only {
                    self.config_app.trades.quotes.contains(&v.quote)
                } else {
                    true
                }
            })
            .map(|(k, _)| k.clone())
            .collect::<Vec<String>>();

        let n_pairs = pairs.len();
        debug!("number of pairs to get trades for: {}", n_pairs);
        for (index, pair) in pairs.iter().enumerate() {
            info!("{} / {} - {}", index + 1, n_pairs, pair);
            self.trades_pair_get(&pair, &conversion_pairs, &conversions)?;
        }

        Ok(())
    }

    //

    fn trades_pair_get(
        &mut self,
        pair: &str,
        conversion_pairs: &Vec<String>,
        conversions: &LazyFrame,
    ) -> Result<(), Box<dyn std::error::Error>> {
        let file_path = crate::paths::dir_trades()
            .join(self.label)
            .join(format!("{pair}.feather"));
        debug!("{} file_path is {}", pair, file_path.as_path().display());

        let mut from_id = 0;
        let mut trades_previous = LazyFrame::default();
        if file_path.exists() {
            trades_previous = crate::feather_read(&file_path)?;
            from_id = crate::column_maxu(trades_previous.clone(), "id")?;
        };

        let mut trades_new = Vec::new();
        let mut n_trades_new: u64 = 0;

        let mut batch = self.trades_pair_batch_get(pair, from_id)?;
        n_trades_new += batch.height() as u64;
        while batch.height() > 0 {
            trades_new.push(
                batch
                    .clone()
                    .lazy()
                    .with_column(lit(crate::utc_ms()?).alias("recorded_at")),
            );
            from_id = crate::column_maxu(batch.clone().lazy(), "id")? + 1;
            batch = self.trades_pair_batch_get(pair, from_id)?;
            n_trades_new += batch.height() as u64;
        }

        if trades_new.len() > 0 {
            trades_new.insert(0, trades_previous);
            let mut output = concat(trades_new, true, true)?.collect()?;
            if conversion_pairs.contains(&pair.to_string()) {
                info!("conversion trades for {}", pair);
                output = concat(
                    Vec::from([
                        output.lazy(),
                        conversions.clone().filter(col("symbol").eq(lit(pair))),
                    ]),
                    true,
                    true,
                )?
                .collect()?;
            }
            output = output
                .lazy()
                .unique_stable(None, UniqueKeepStrategy::First)
                .sort_by_exprs([col("time"), col("id")], [false, false], false)
                .collect()?;
            crate::feather_write(&mut output, &file_path)?;
        }
        info!("number of new trades for {} is {}", pair, n_trades_new);

        Ok(())
    }

    //

    fn trades_pair_batch_get(
        &mut self,
        pair: &str,
        from_id: u64,
    ) -> Result<DataFrame, Box<dyn std::error::Error>> {
        let params = Vec::from([
            format!("symbol={}", pair),
            format!("fromId={}", from_id),
            format!("limit={}", self.config_app.trades.limit),
            format!("recvWindow={}", self.config_app.trades.recvwindow),
            format!("timestamp={}", crate::utc_ms()?),
        ])
        .join("&");
        let signature = signature_get(&params)?;
        let url = format!(
            "{}/api/v3/myTrades?{}&signature={}",
            self.client.url, params, signature
        );
        let response = crate::api::request_get(&mut self.client, crate::api::Request::Get(&url))?;
        // println!("{}", response.clone());

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

        let mut ts_last = crate::date_to_unix_ms(&self.config_app.withdrawals.ts_start)?;
        let mut withdrawals_previous = LazyFrame::default();
        if file_path.exists() {
            withdrawals_previous = crate::feather_read(&file_path)?;
            ts_last = crate::column_maxu(withdrawals_previous.clone(), "applytime")?
        };

        let mut withdrawals_new = Vec::new();
        let mut n_withdrawals_new: u64 = 0;

        let mut batch = self.withdrawals_batch_get(ts_last)?;
        while ts_last < crate::utc_ms()? {
            if batch.height() > 0 {
                n_withdrawals_new += batch.height() as u64;
                withdrawals_new.push(
                    batch
                        .clone()
                        .lazy()
                        .with_column(lit(crate::utc_ms()?).alias("recorded_at")),
                );
                let ts_last_new = crate::column_maxu(batch.clone().lazy(), "applytime")?;
                if ts_last != ts_last_new {
                    ts_last = ts_last_new;
                } else {
                    ts_last += 1;
                }
            } else {
                ts_last += crate::ti_ms(&self.config_app.withdrawals.ts_window)?;
            }
            batch = self.withdrawals_batch_get(ts_last)?;
        }

        if withdrawals_new.len() > 0 {
            withdrawals_new.insert(0, withdrawals_previous);
            let mut output = concat(withdrawals_new, true, true)?
                .unique_stable(
                    Some(Vec::from(["id".to_string()])),
                    UniqueKeepStrategy::First,
                )
                .sort_by_exprs([col("applytime"), col("id")], [false, false], false)
                .collect()?;
            crate::feather_write(&mut output, &file_path)?;
        }
        info!(
            "number of new withdrawals for binance is {}",
            n_withdrawals_new
        );

        Ok(())
    }

    //

    fn withdrawals_batch_get(
        &mut self,
        ts_start: u64,
    ) -> Result<DataFrame, Box<dyn std::error::Error>> {
        let params = Vec::from([
            format!("status={}", self.config_app.withdrawals.status),
            format!("limit={}", self.config_app.withdrawals.limit),
            format!("recvWindow={}", self.config_app.withdrawals.recvwindow),
            format!("startTime={}", ts_start),
            format!(
                "endTime={}",
                ts_start + crate::ti_ms(&self.config_app.withdrawals.ts_window)?
            ),
            format!("timestamp={}", crate::utc_ms()?),
        ])
        .join("&");
        let signature = signature_get(&params)?;
        let url = format!(
            "{}/sapi/v1/capital/withdraw/history?{}&signature={}",
            self.client.url, params, signature
        );
        let response = crate::api::request_get(&mut self.client, crate::api::Request::Get(&url))?;

        Ok(withdrawals_deserialize(&response)?)
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

//

#[derive(serde::Deserialize)]
struct Trade {
    #[serde(alias = "symbol")]
    symbol: String,
    #[serde(alias = "id")]
    id: u64,
    #[serde(alias = "orderId")]
    orderid: u64,
    #[serde(alias = "orderListId")]
    orderlistid: i64,
    #[serde(alias = "price")]
    price: String,
    #[serde(alias = "qty")]
    qty: String,
    #[serde(alias = "quoteQty")]
    quoteqty: String,
    #[serde(alias = "commission")]
    commission: String,
    #[serde(alias = "commissionAsset")]
    commissionasset: String,
    #[serde(alias = "time")]
    time: u64,
    #[serde(alias = "isBuyer")]
    isbuyer: bool,
    #[serde(alias = "isMaker")]
    ismaker: bool,
    #[serde(alias = "isBestMatch")]
    isbestmatch: bool,
}

//

fn trades_deserialize(response: &str) -> Result<DataFrame, Box<dyn std::error::Error>> {
    let rows: Vec<Trade> = serde_json::from_str(&response)?;

    let mut symbol: Vec<String> = Vec::new();
    let mut id: Vec<u64> = Vec::new();
    let mut orderid: Vec<u64> = Vec::new();
    let mut orderlistid: Vec<i64> = Vec::new();
    let mut price: Vec<String> = Vec::new();
    let mut qty: Vec<String> = Vec::new();
    let mut quoteqty: Vec<String> = Vec::new();
    let mut commission: Vec<String> = Vec::new();
    let mut commissionasset: Vec<String> = Vec::new();
    let mut time: Vec<u64> = Vec::new();
    let mut isbuyer: Vec<bool> = Vec::new();
    let mut ismaker: Vec<bool> = Vec::new();
    let mut isbestmatch: Vec<bool> = Vec::new();

    for row in rows.iter() {
        symbol.push(row.symbol.clone());
        id.push(row.id);
        orderid.push(row.orderid);
        orderlistid.push(row.orderlistid);
        price.push(row.price.clone());
        qty.push(row.qty.clone());
        quoteqty.push(row.quoteqty.clone());
        commission.push(row.commission.clone());
        commissionasset.push(row.commissionasset.clone());
        time.push(row.time);
        isbuyer.push(row.isbuyer);
        ismaker.push(row.ismaker);
        isbestmatch.push(row.isbestmatch);
    }

    Ok(
        DataFrame::new(Vec::from([
            Series::new("symbol", symbol),
            Series::new("id", id),
            Series::new("orderid", orderid),
            Series::new("orderlistid", orderlistid),
            Series::new("price", price),
            Series::new("qty", qty),
            Series::new("quoteqty", quoteqty),
            Series::new("commission", commission),
            Series::new("commissionasset", commissionasset),
            Series::new("time", time),
            Series::new("isbuyer", isbuyer),
            Series::new("ismaker", ismaker),
            Series::new("isbestmatch", isbestmatch),
        ]))?, // .lazy()
              // .with_columns([
              //     col("price").cast(DataType::Decimal(None, Some(8))),
              //     col("qty").cast(DataType::Decimal(None, Some(8))),
              //     col("quoteqty").cast(DataType::Decimal(None, Some(8))),
              //     col("commission").cast(DataType::Decimal(None, Some(8))),
              // ])
              // .collect()?
    )
}

//

#[derive(serde::Deserialize)]
struct Withdrawal {
    #[serde(alias = "id")]
    id: String,
    #[serde(alias = "amount")]
    amount: String,
    #[serde(alias = "transactionFee")]
    transactionfee: String,
    #[serde(alias = "coin")]
    coin: String,
    #[serde(alias = "status")]
    status: i64,
    #[serde(alias = "address")]
    address: String,
    #[serde(alias = "txId")]
    txid: String,
    #[serde(alias = "applyTime")]
    applytime: String,
    #[serde(alias = "network")]
    network: String,
    #[serde(alias = "transferType")]
    transfertype: i64,
    #[serde(default, alias = "withdrawOrderId")]
    withdraworderid: String,
    #[serde(alias = "info")]
    info: String,
    #[serde(alias = "confirmNo")]
    confirmno: i64,
    #[serde(alias = "walletType")]
    wallettype: i64,
    #[serde(alias = "txKey")]
    txkey: String,
    #[serde(alias = "completeTime")]
    completetime: String,
}

//

fn withdrawals_deserialize(response: &str) -> Result<DataFrame, Box<dyn std::error::Error>> {
    let rows: Vec<Withdrawal> = serde_json::from_str(&response)?;

    let mut id: Vec<String> = Vec::new();
    let mut amount: Vec<String> = Vec::new();
    let mut transactionfee: Vec<String> = Vec::new();
    let mut coin: Vec<String> = Vec::new();
    let mut status: Vec<i64> = Vec::new();
    let mut address: Vec<String> = Vec::new();
    let mut txid: Vec<String> = Vec::new();
    let mut applytime: Vec<u64> = Vec::new();
    let mut network: Vec<String> = Vec::new();
    let mut transfertype: Vec<i64> = Vec::new();
    let mut withdraworderid: Vec<String> = Vec::new();
    let mut info: Vec<String> = Vec::new();
    let mut confirmno: Vec<i64> = Vec::new();
    let mut wallettype: Vec<i64> = Vec::new();
    let mut txkey: Vec<String> = Vec::new();
    let mut completetime: Vec<u64> = Vec::new();

    for row in rows.iter() {
        id.push(row.id.clone());
        amount.push(row.amount.clone());
        transactionfee.push(row.transactionfee.clone());
        coin.push(row.coin.clone());
        status.push(row.status);
        address.push(row.address.clone());
        txid.push(row.txid.clone());
        applytime.push(crate::date_to_unix_ms(&row.applytime)?);
        network.push(row.network.clone());
        transfertype.push(row.transfertype);
        withdraworderid.push(row.withdraworderid.clone());
        info.push(row.info.clone());
        confirmno.push(row.confirmno);
        wallettype.push(row.wallettype);
        txkey.push(row.txkey.clone());
        completetime.push(crate::date_to_unix_ms(&row.completetime)?);
    }

    Ok(
        DataFrame::new(Vec::from([
            Series::new("id", id),
            Series::new("amount", amount),
            Series::new("transactionfee", transactionfee),
            Series::new("coin", coin),
            Series::new("status", status),
            Series::new("address", address),
            Series::new("txid", txid),
            Series::new("applytime", applytime),
            Series::new("network", network),
            Series::new("transfertype", transfertype),
            Series::new("withdraworderid", withdraworderid),
            Series::new("info", info),
            Series::new("confirmno", confirmno),
            Series::new("wallettype", wallettype),
            Series::new("txkey", txkey),
            Series::new("completetime", completetime),
        ]))?, // .lazy()
              // .with_columns([
              //     col("amount").cast(DataType::Decimal(None, Some(8))),
              //     col("transactionfee").cast(DataType::Decimal(None, Some(8))),
              // ])
              // .collect()?
    )
}

//

fn signature_get(request: &str) -> Result<String, Box<dyn std::error::Error>> {
    let key_secret = std::env::var("BINANCE_API_SECRET")?;
    let mut key_signed = Hmac::<sha2::Sha256>::new_from_slice(key_secret.as_bytes())?;
    key_signed.update(request.as_bytes());
    let signature = hex::encode(key_signed.finalize().into_bytes());

    Ok(format!("{}", signature))
}
