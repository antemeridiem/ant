
fn main() {
    env_logger::init();

    let config = api::yaml_read(&api::paths::file_config()).expect("config failed");

    // coinmarketcap
    {
        let mut api = api::api::coinmarketcap::API::new(&config).expect("api failed");
        api.fiat_get().expect("fiat failed");
        api.stablecoins_get().expect("stablecoins failed");
        println!("throttler: {:?}", api.client.throttler.len());
    }
    
    // binance
    {
        let mut api = api::api::binance::API::new(&config).expect("api failed");
        api.history_get().expect("history failed");
        println!("throttler: {:?}", api.client.throttler.len());
    }

    // kraken
    {
        let mut api = api::api::kraken::API::new(&config).expect("api failed");
        api.pairs_get().expect("pairs failed");
        println!("throttler: {:?}", api.client.throttler.len());
    }
}
