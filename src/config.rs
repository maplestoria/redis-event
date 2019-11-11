pub struct Config {
    discard_rdb: bool,
    big_key_threshold: i32,
}

pub fn default() -> Config {
    Config {
        discard_rdb: false,
        big_key_threshold: 10000,
    }
}