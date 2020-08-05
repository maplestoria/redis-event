// lzf解压缩算法
pub(crate) fn decompress(input: &mut Vec<u8>, input_len: isize, output: &mut Vec<u8>, output_len: isize) {
    let mut iidx: isize = 0;
    let mut oidx: isize = 0;

    while iidx < input_len {
        let mut ctrl = input[iidx as usize] as isize;
        iidx += 1;

        if ctrl < (1 << 5) {
            ctrl += 1;

            if oidx + ctrl > output_len {
                return;
            }

            while ctrl > 0 {
                output[oidx as usize] = input[iidx as usize];
                oidx += 1;
                iidx += 1;
                ctrl -= 1;
            }
        } else {
            let mut length = ctrl >> 5;
            let mut reference = (oidx - ((ctrl & 0x1f) << 8) - 1) as isize;
            if length == 7 {
                length += input[iidx as usize] as isize;
                iidx += 1;
            }
            reference -= input[iidx as usize] as isize;
            iidx += 1;
            if ((oidx + length + 2) > output_len) || reference < 0 {
                return;
            }

            output[oidx as usize] = output[reference as usize];
            oidx += 1;
            reference += 1;
            output[oidx as usize] = output[reference as usize];
            oidx += 1;
            reference += 1;

            while length > 0 {
                output[oidx as usize] = output[reference as usize];
                oidx += 1;
                reference += 1;
                length -= 1;
            }
        }
    }
}
