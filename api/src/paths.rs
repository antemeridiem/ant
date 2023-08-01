use std::path::PathBuf;

fn dir_root() -> PathBuf {
    std::env::current_dir().expect("current_dir failed")
}

pub fn dir_create(dir_path: &PathBuf) {
    std::fs::create_dir_all(dir_path).expect("directory creation failed");
}

pub fn dir_list(
    dir_path: &PathBuf,
) -> Result<Vec<crate::structs::DirEntry>, Box<dyn std::error::Error>> {
    std::fs::read_dir(dir_path)?
        .map(|x| {
            Ok({
                let path = x?.path();
                crate::structs::DirEntry {
                    path: path.clone(),
                    stem: path
                        .clone()
                        .extension()
                        .ok_or("stem not found")?
                        .to_os_string()
                        .into_string()
                        .or(Err("string failed"))?,
                    extension: path
                        .clone()
                        .file_stem()
                        .ok_or("extension not found")?
                        .to_os_string()
                        .into_string()
                        .or(Err("string failed"))?,
                }
            })
        })
        .collect::<Result<Vec<crate::structs::DirEntry>, Box<dyn std::error::Error>>>()
}

//

pub fn dir_config() -> PathBuf {
    let dir_path = dir_root().join("config");
    dir_create(&dir_path);
    dir_path
}

pub fn dir_data() -> PathBuf {
    let dir_path = dir_root().join("data");
    dir_create(&dir_path);
    dir_path
}

pub fn dir_klines() -> PathBuf {
    let dir_path = dir_data().join("klines");
    dir_create(&dir_path);
    dir_path
}

pub fn dir_trades() -> PathBuf {
    let dir_path = dir_data().join("trades");
    dir_create(&dir_path);
    dir_path
}

pub fn dir_logs() -> PathBuf {
    let dir_path = dir_root().join("logs");
    dir_create(&dir_path);
    dir_path
}

pub fn dir_wip() -> PathBuf {
    let dir_path = dir_root().join("wip");
    dir_create(&dir_path);
    dir_path
}

//
//
//

pub fn file_config() -> PathBuf {
    dir_config().join("config.yaml")
}

pub fn file_fiat() -> PathBuf {
    dir_config().join("fiat.json")
}

pub fn file_pairs_binance() -> PathBuf {
    dir_config().join("pairs-binance.json")
}

pub fn file_pairs_kraken() -> PathBuf {
    dir_config().join("pairs-kraken.json")
}

pub fn file_stablecoins() -> PathBuf {
    dir_config().join("stablecoins.json")
}

//
//
//
