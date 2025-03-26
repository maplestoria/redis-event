// lzf解压缩算法
pub(crate) fn decompress(input: &[u8], output: &mut [u8]) {
    unsafe {
        lzf_decompress(input.as_ptr(), input.len(), output.as_mut_ptr(), output.len());
    }
}

#[link(name = "lzf_d")]
unsafe extern "C" {
    unsafe fn lzf_decompress(in_data: *const u8, in_len: usize, out_data: *mut u8, out_len: usize) -> usize;
}
