
Build base image for the app:
```
docker build --no-cache -t app_image_base .
```

### api - define data connections and download data
```
cargo new api --bin --vcs none
RUST_LOG=debug cargo run --package api --bin api --release
RUST_LOG=info cargo run --package api --bin api --release
```

### tests - binary for tests
```
cargo new tests --bin --vcs none
RUST_LOG=debug cargo run --package tests --bin tests --release
RUST_LOG=info cargo run --package tests --bin tests --release
```

### utils - helper functions
```
cargo new utils --lib --vcs none
```

