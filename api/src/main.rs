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
        if config.history.do_history {
            api.history_get().expect("history failed");
        }
        if config.trades.do_trades.contains(&api.label.to_string()) {
            api.trades_get().expect("trades failed");
        }
        if config
            .withdrawals
            .do_withdrawals
            .contains(&api.label.to_string())
        {
            api.withdrawals_get().expect("withdrawals failed");
        }
        println!("throttler: {:?}", api.client.throttler.len());
    }

    // kraken
    {
        let mut api = api::api::kraken::API::new(&config).expect("api failed");
        api.pairs_get().expect("pairs failed");

        if config.trades.do_trades.contains(&api.label.to_string()) {
            api.trades_get().expect("trades failed");
        }
        if config
            .withdrawals
            .do_withdrawals
            .contains(&api.label.to_string())
        {
            api.withdrawals_get().expect("withdrawals failed");
        }

        println!("throttler: {:?}", api.client.throttler.len());
    }
}
