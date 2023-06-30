use chrono::NaiveDateTime;
use log::debug;
use polars::prelude::*;
use std::path::PathBuf;

//
//
//

pub fn csv_read(filepath: &PathBuf) -> LazyFrame {
    debug!("csv read - {}", filepath.as_path().display());
    LazyCsvReader::new(filepath)
        .has_header(true)
        .finish()
        .expect("read failed")
}

//

pub fn csv_write(df: &mut DataFrame, filepath: &PathBuf, precision: Option<usize>) {
    debug!("csv write - {}", filepath.as_path().display());
    CsvWriter::new(std::fs::File::create(filepath).expect("file failed"))
        .has_header(true)
        .with_float_precision(precision)
        .finish(df)
        .expect("write failed");
}

//

pub fn feather_read(filepath: &PathBuf) -> LazyFrame {
    debug!("feather read - {}", filepath.as_path().display());
    LazyFrame::scan_ipc(
        filepath,
        ScanArgsIpc {
            memmap: false,
            ..Default::default()
        },
    )
    .expect("scan failed")
}

//

pub fn feather_write(df: &mut DataFrame, filepath: &PathBuf) {
    debug!("feather write - {}", filepath.as_path().display());
    IpcWriter::new(std::fs::File::create(filepath).expect("file failed"))
        .with_compression(Some(IpcCompression::LZ4))
        .finish(&mut df.as_single_chunk())
        .expect("write failed");
}

//

pub fn json_read<T>(filepath: &PathBuf) -> T
where
    for<'a> T: serde::Deserialize<'a>,
{
    debug!("json read - {}", filepath.as_path().display());
    serde_json::from_str(&std::fs::read_to_string(&filepath).expect("string failed"))
        .expect("serde failed")
}

//

pub fn json_write<T>(data: &T, filepath: &PathBuf)
where
    T: serde::Serialize,
{
    debug!("json write - {}", filepath.as_path().display());
    std::fs::write(
        filepath,
        serde_json::to_string_pretty(data).expect("string failed"),
    )
    .expect("write failed");
}

//

pub fn yaml_read<T>(filepath: &PathBuf) -> T
where
    for<'a> T: serde::Deserialize<'a>,
{
    debug!("yaml read - {}", filepath.as_path().display());
    serde_yaml::from_str(&std::fs::read_to_string(&filepath).expect("string failed"))
        .expect("serde failed")
}

//

pub fn yaml_write<T>(data: &T, filepath: &PathBuf)
where
    T: serde::Serialize,
{
    debug!("yaml write - {}", filepath.as_path().display());
    std::fs::write(
        filepath,
        serde_yaml::to_string(data).expect("string failed"),
    )
    .expect("write failed");
}

//
//
//

fn utc() -> std::time::Duration {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .expect("utc failed")
}

//

pub fn utc_ms() -> u64 {
    utc().as_millis() as u64
}

//

pub fn utc_s() -> u32 {
    utc().as_secs() as u32
}

//
//
//

pub fn date_to_unix_s(date: &str) -> u32 {
    NaiveDateTime::parse_from_str(&format!("{} 00:00:00", date), "%Y-%m-%d %H:%M:%S")
        .expect("parse failed")
        .timestamp() as u32
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

pub fn unix_ms_to_time(ts: u64) -> NaiveDateTime {
    NaiveDateTime::from_timestamp_millis(ts as i64).expect("datetime failed")
}

//

pub fn unix_s_to_time(ts: u32) -> NaiveDateTime {
    NaiveDateTime::from_timestamp_opt(ts as i64, 0).expect("datetime failed")
}

//

pub fn ti(interval: &str) -> u32 {
    let period: &str = &interval[interval.len() - 1..];
    assert!(["s", "m", "h", "d", "w"].contains(&period));
    let length: u32 = interval
        .replace(&period, "")
        .parse()
        .expect("parse failed");
    match period {
        "s" => Some(length),
        "m" => Some(60 * length),
        "h" => Some(3_600 * length),
        "d" => Some(86_400 * length),
        "w" => Some(604_800 * length),
        _ => None,
    }
    .expect("period not found")
}

//

pub fn td(time_start: u64) -> f32 {
    (utc_ms() - time_start) as f32 / 1_000.0
}
