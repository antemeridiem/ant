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
    serde_json::from_str(&std::fs::read_to_string(&filepath).expect("string failed")).expect("serde failed")
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
    serde_yaml::from_str(&std::fs::read_to_string(&filepath).expect("string failed")).expect("serde failed")
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
