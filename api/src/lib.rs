use chrono::NaiveDateTime;
use log::debug;
use polars::prelude::*;
use std::collections::HashMap;
use std::path::PathBuf;

//

pub mod api;
pub mod diff;
pub mod paths;
pub mod structs;

//
//
//

pub fn csv_read(filepath: &PathBuf) -> Result<LazyFrame, Box<dyn std::error::Error>> {
    debug!("csv read - {}", filepath.as_path().display());
    Ok(LazyCsvReader::new(filepath).has_header(true).finish()?)
}

//

pub fn csv_write(
    df: &mut DataFrame,
    filepath: &PathBuf,
    precision: Option<usize>,
) -> Result<(), Box<dyn std::error::Error>> {
    debug!("csv write - {}", filepath.as_path().display());
    Ok(CsvWriter::new(std::fs::File::create(filepath)?)
        .has_header(true)
        .with_float_precision(precision)
        .finish(df)?)
}

//

pub fn feather_read(filepath: &PathBuf) -> Result<LazyFrame, Box<dyn std::error::Error>> {
    debug!("feather read - {}", filepath.as_path().display());
    Ok(LazyFrame::scan_ipc(
        filepath,
        ScanArgsIpc {
            memmap: false,
            ..Default::default()
        },
    )?)
}

//

pub fn feather_write(
    df: &mut DataFrame,
    filepath: &PathBuf,
) -> Result<(), Box<dyn std::error::Error>> {
    debug!("feather write - {}", filepath.as_path().display());
    Ok(IpcWriter::new(std::fs::File::create(filepath)?)
        .with_compression(Some(IpcCompression::LZ4))
        .finish(&mut df.as_single_chunk())?)
}

//

pub fn json_read<T>(filepath: &PathBuf) -> Result<T, Box<dyn std::error::Error>>
where
    for<'a> T: serde::Deserialize<'a>,
{
    debug!("json read - {}", filepath.as_path().display());
    Ok(serde_json::from_str(&std::fs::read_to_string(&filepath)?)?)
}

//

pub fn json_write<T>(data: &T, filepath: &PathBuf) -> Result<(), Box<dyn std::error::Error>>
where
    T: serde::Serialize,
{
    debug!("json write - {}", filepath.as_path().display());
    Ok(std::fs::write(
        filepath,
        serde_json::to_string_pretty(data)?,
    )?)
}

//

pub fn yaml_read<T>(filepath: &PathBuf) -> Result<T, Box<dyn std::error::Error>>
where
    for<'a> T: serde::Deserialize<'a>,
{
    debug!("yaml read - {}", filepath.as_path().display());
    Ok(serde_yaml::from_str(&std::fs::read_to_string(&filepath)?)?)
}

//

pub fn yaml_write<T>(data: &T, filepath: &PathBuf) -> Result<(), Box<dyn std::error::Error>>
where
    T: serde::Serialize,
{
    debug!("yaml write - {}", filepath.as_path().display());
    Ok(std::fs::write(filepath, serde_yaml::to_string(data)?)?)
}

//
//
//

pub fn config_write_json(
    data: &HashMap<String, serde_json::Value>,
    file_data: &PathBuf,
) -> Result<(), Box<dyn std::error::Error>> {
    let kind = file_data
        .file_stem()
        .ok_or("stem not found")?
        .to_str()
        .ok_or("string not found")?;
    let extension = file_data
        .extension()
        .ok_or("extension not found")?
        .to_str()
        .ok_or("string not found")?;
    let timestamp = unix_s_to_string(utc_s()?)?;
    if file_data.is_file() {
        let mut diff = crate::diff::Diff::new();
        let data_new = serde_json::to_value(data)?;
        let data_old: serde_json::Value = json_read(file_data)?;
        crate::diff::run_json(&data_new, &data_old, "", &mut diff)?;
        if !(diff.new.is_empty() & diff.old.is_empty() & diff.diff.is_empty()) {
            let dir_log = crate::paths::dir_logs().join(kind);
            crate::paths::dir_create(&dir_log);
            let file_log_diff = &dir_log.join(format!("{}.{}", timestamp, extension));
            json_write(&diff, file_log_diff)?;
            json_write(data, file_data)?;
        }
    } else {
        json_write(data, file_data)?;
    }

    Ok({})
}

//
//
//

fn utc() -> Result<std::time::Duration, Box<dyn std::error::Error>> {
    Ok(std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH)?)
}

//

pub fn utc_ms() -> Result<u64, Box<dyn std::error::Error>> {
    Ok(utc()?.as_millis() as u64)
}

//

pub fn utc_s() -> Result<u32, Box<dyn std::error::Error>> {
    Ok(utc()?.as_secs() as u32)
}

//
//
//

pub fn date_to_unix_s(date: &str) -> Result<u32, Box<dyn std::error::Error>> {
    Ok(
        NaiveDateTime::parse_from_str(&format!("{} 00:00:00", date), "%Y-%m-%d %H:%M:%S")?
            .timestamp() as u32,
    )
}

//

pub fn time_to_unix_s(datetime: NaiveDateTime) -> u32 {
    datetime.timestamp() as u32
}

//

pub fn time_to_unix_ms(datetime: NaiveDateTime) -> u64 {
    datetime.timestamp_millis() as u64
}

//

pub fn unix_ms_to_time(ts: u64) -> Result<NaiveDateTime, Box<dyn std::error::Error>> {
    Ok(NaiveDateTime::from_timestamp_millis(ts as i64).ok_or("datetime failed")?)
}

//

pub fn unix_s_to_time(ts: u32) -> Result<NaiveDateTime, Box<dyn std::error::Error>> {
    Ok(NaiveDateTime::from_timestamp_opt(ts as i64, 0).ok_or("datetime failed")?)
}

//

pub fn unix_ms_to_string(ts: u64) -> Result<String, Box<dyn std::error::Error>> {
    Ok(unix_ms_to_time(ts)?.format("%Y-%m-%d %H:%M:%S").to_string())
}

//

pub fn unix_s_to_string(ts: u32) -> Result<String, Box<dyn std::error::Error>> {
    Ok(unix_s_to_time(ts)?.format("%Y-%m-%d %H:%M:%S").to_string())
}

//

pub fn ti_s(interval: &str) -> Result<u32, Box<dyn std::error::Error>> {
    let period: &str = &interval[interval.len() - 1..];
    assert!(["s", "m", "h", "d", "w"].contains(&period));
    let length: u32 = interval.replace(&period, "").parse()?;
    Ok(match period {
        "s" => Some(length),
        "m" => Some(60 * length),
        "h" => Some(3_600 * length),
        "d" => Some(86_400 * length),
        "w" => Some(604_800 * length),
        _ => None,
    }
    .ok_or("period not found")?)
}

//

pub fn ti_ms(interval: &str) -> Result<u64, Box<dyn std::error::Error>> {
    Ok(1000 * ti_s(interval)? as u64)
}

//

pub fn td(time_start: u64) -> Result<f32, Box<dyn std::error::Error>> {
    Ok((utc_ms()? - time_start) as f32 / 1_000.0)
}

//
//
//

pub fn config_get() -> Result<crate::structs::AppConfig, Box<dyn std::error::Error>> {
    yaml_read(&crate::paths::file_config())
}

//

pub fn column_maxu(lf: LazyFrame, column_name: &str) -> Result<u64, Box<dyn std::error::Error>> {
    Ok(lf
        .select([col(column_name).cast(DataType::UInt64).max()])
        .collect()?
        .column(column_name)?
        .max::<u64>()
        .ok_or("max not found")?)
}

//

pub fn column_minu(lf: LazyFrame, column_name: &str) -> Result<u64, Box<dyn std::error::Error>> {
    Ok(lf
        .select([col(column_name).cast(DataType::UInt64).min()])
        .collect()?
        .column(column_name)?
        .min::<u64>()
        .ok_or("min not found")?)
}

//

pub fn pairs_mature(exchange: &str) -> Result<(), Box<dyn std::error::Error>> {
    let mut pairs = Vec::new();
    let mut tss = Vec::new();

    for dir_entry in crate::paths::dir_list(&crate::paths::dir_klines().join(exchange))? {
        if dir_entry.extension == "feather" {
            pairs.push(dir_entry.stem);
            tss.push(column_minu(feather_read(&dir_entry.path)?, "ts")?);
        }
    }

    let ts = utc_s()? - 3600 * 24 * 1;
    let result = DataFrame::new(Vec::from([
        Series::new("pair", pairs),
        Series::new("ts", tss),
    ]))?
    .lazy()
    .filter(col("ts").lt(lit(ts)))
    .select([col("pair").unique()])
    .collect()?
    .column("pair")?
    .utf8()?
    .into_iter()
    .map(|x| Ok(x.ok_or("item not found")?.to_string()))
    .collect::<Result<Vec<String>, Box<dyn std::error::Error>>>()?;

    json_write(
        &result,
        &paths::dir_config().join(format!("{}-pairs-mature.json", exchange)),
    )?;

    Ok(())
}

//

pub fn timestamps_missing(
    lf: LazyFrame,
    interval_base: &str,
) -> Result<LazyFrame, Box<dyn std::error::Error>> {
    let ts_max = column_maxu(lf.clone(), "ts")? as u32;
    let ts_min = column_minu(lf.clone(), "ts")? as u32;
    let ts = DataFrame::new(Vec::from([Series::new(
        "ts",
        (ts_min..ts_max + 1)
            .step_by(ti_s(interval_base)? as usize)
            .collect::<Vec<u32>>(),
    )]))?
    .lazy();
    Ok(ts.join(lf, [col("ts")], [col("ts")], JoinType::Left))
}
