#![no_main]
use i2pr_tc_core::dht::{BootstrapSnapshot, KrpcMessage, KrpcLimits};
use libfuzzer_sys::fuzz_target;

fuzz_target!(|data: &[u8]| {
    let _ = KrpcMessage::decode(data, KrpcLimits::default());
    let _ = BootstrapSnapshot::decode(data);
});
