fn main() {
    cc::Build::new()
        .file("deps/lzf_d.c")
        .compile("lzf_d");
}