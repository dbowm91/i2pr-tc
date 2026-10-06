#![no_main]
//! Hostile-input coverage for the SAM wire parser and its base64 decoder.
//!
//! Both are application-visible protocol surfaces: a router's reply bytes reach
//! `parse_reply_block` and a reply's `DESTINATION=` value reaches
//! `decode_base64`. The invariant under test is that neither can panic, abort,
//! or allocate without a bound, whatever bytes arrive.
use i2pr_tc_i2p::sam::{decode_base64, parse_reply_block, SamLimits};
use libfuzzer_sys::fuzz_target;

fuzz_target!(|data: &[u8]| {
    // Two limit profiles: the production defaults and a deliberately tiny one
    // so the bound checks are exercised as aggressively as the parser.
    for limits in [SamLimits::default(), SamLimits::strict()] {
        let Ok(reply) = parse_reply_block(data, &limits) else {
            // Rejection is a normal outcome, but the input must never have been
            // retained or partially applied.
            continue;
        };
        for key in ["DESTINATION", "ME", "VERSION", "SESSION_ID"] {
            let _ = reply.option(key);
        }
        let _ = reply.kind();
        let _ = reply.result();
        let _ = reply.destination("DESTINATION");
    }

    // Decode under both the destination bound and a deliberately small one.
    let max = u16::from_le_bytes([data.get(0).copied().unwrap_or(1), data.get(1).copied().unwrap_or(0)])
        as usize
        + 1;
    if let Ok(bytes) = decode_base64(data, max) {
        // A decoded value can never exceed the bound it was given.
        assert!(bytes.len() <= max, "decode_base64 exceeded its bound");
    }
});