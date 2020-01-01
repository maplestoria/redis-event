pub struct Config {
    pub discard_rdb: bool,
    pub aof: bool,
}

pub fn default() -> Config {
    Config {
        discard_rdb: false,
        aof: false
    }
}