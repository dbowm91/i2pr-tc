#![no_main]
use libfuzzer_sys::fuzz_target;
fuzz_target!(|data: &[u8]| {
    let _ = i2pr_tc_core::wire::parse_handshake(data);
    let _ = i2pr_tc_core::wire::parse_frame(data, 2 * 1024 * 1024);
    let _ = i2pr_tc_core::wire::I2pPex::decode(data, data, 2048);
});
