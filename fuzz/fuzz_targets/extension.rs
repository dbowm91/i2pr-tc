#![no_main]
use libfuzzer_sys::fuzz_target;

fuzz_target!(|data: &[u8]| {
    let _ = i2pr_tc_core::extension::parse_ut_metadata(
        data,
        i2pr_tc_core::extension::MetadataLimits::default(),
    );
    let _ = i2pr_tc_core::extension::parse_extension_map(data, 128);
});
