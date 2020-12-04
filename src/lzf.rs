// lzf解压缩算法
pub(crate) fn decompress(input: &[u8], output: &mut [u8]) {
    unsafe {
        lzf_decompress(input.as_ptr(), input.len() as u32, output.as_mut_ptr(), output.len() as u32);
    }
}


#[link(name = "lzf_d")]
extern {
    fn lzf_decompress(in_data: *const u8, in_len: u32,
                      out_data: *mut u8, out_len: u32) -> u32;
}