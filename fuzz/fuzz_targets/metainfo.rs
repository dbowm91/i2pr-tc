#![no_main]
use libfuzzer_sys::fuzz_target;
fuzz_target!(|data: &[u8]| {
    let _ = i2pr_tc_core::metainfo::parse(data, Default::default());
});
