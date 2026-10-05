#![no_main]
use libfuzzer_sys::fuzz_target;
fuzz_target!(|data: &[u8]| {
    let _ = i2pr_tc_core::bencode::parse(data, Default::default());
});
