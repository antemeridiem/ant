# ant - a worker app

Eventually it will be able to download historical klines, trades, withdrawals and calculate capital gains

Currently, the app:
- Downloads fiat and stabecoin data from coinmarketcap and keeps track of changes
- Downloads tradable pairs data from binance and keeps track of changes
- Downloads klines history from binanace (according to settings in the `config.yaml` file)


How to run:
- Build base image for the app:
```
docker build --no-cache -t app_image_base .
```
- Edit path to the `.env` file in the `.devcontainer/devcontainer.json` file or add required env variables by other means
- Then Open Folder in Container (Dev Containers) in VS Code

## api - define data connections and download data
```
cargo new api --bin --vcs none
RUST_LOG=debug cargo run --package api --bin api --release
RUST_LOG=info cargo run --package api --bin api --release
```

## tests - binary for tests
```
cargo new tests --bin --vcs none
RUST_LOG=debug cargo run --package tests --bin tests --release
RUST_LOG=info cargo run --package tests --bin tests --release
```
