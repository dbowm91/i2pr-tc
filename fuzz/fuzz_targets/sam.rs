#![no_main]
//! Hostile-input coverage for the SAM wire parser, its base64 decoder, and its
//! Destination verifier.
//!
//! All three are application-visible protocol surfaces: a router's reply bytes
//! reach `parse_reply_block`, a reply's `DESTINATION=` value reaches
//! `decode_base64`, and any base64 value a peer controls can be presented as a
//! Destination. The invariant under test is that none of them can panic, abort,
//! or allocate without a bound, whatever bytes arrive.
use i2pr_tc_i2p::sam::{
    decode_base64, decode_destination, encode_base64, parse_reply_block, SamLimits,
    SamReplyKind,
};
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
        for key in ["VALUE", "DESTINATION", "NAME", "VERSION"] {
            let _ = reply.option(key);
        }
        let _ = reply.kind();
        let _ = reply.result();
        let _ = reply.destination("VALUE");
    }

    // Decode under both the destination bound and a deliberately small one.
    let max = u16::from_le_bytes([
        data.get(0).copied().unwrap_or(1),
        data.get(1).copied().unwrap_or(0),
    ]) as usize
        + 1;
    if let Ok(bytes) = decode_base64(data, max) {
        // A decoded value can never exceed the bound it was given.
        assert!(bytes.len() <= max, "decode_base64 exceeded its bound");
    }

    // Whatever survives a decode must re-encode to exactly one spelling: an
    // encoder that could emit more than one form would let two peers disagree
    // about the same Destination.
    if let Ok(bytes) = decode_base64(data, max) {
        assert_eq!(
            decode_base64(encode_base64(&bytes).as_bytes(), max).ok(),
            Some(bytes.clone()),
            "base64 did not round-trip",
        );

        // Destination verification must be total: every accepted value has the
        // keys-first layout, and re-encoding an accepted Destination must not
        // change how it is read.
        let encoded = encode_base64(&bytes);
        if let Ok(destination) = decode_destination(&encoded) {
            assert_eq!(destination.as_bytes(), bytes.as_slice());
            assert_eq!(
                decode_destination(&encode_base64(destination.as_bytes()))
                    .map(|again| again.hash())
                    .ok(),
                Some(destination.hash()),
                "an accepted Destination did not round-trip",
            );
        }
    }

    // The 3.3 delivery reply forms carry no RESULT, and no parse of them may
    // depend on the input being well formed.
    for prefix in [
        &b"DATAGRAM RECEIVED SIZE="[..],
        b"RAW DATA RECEIVED RESULT=OK SIZE=",
        b"RAW DATA SEND RESULT=OK",
        b"DATAGRAM SEND RESULT=I2P_ERROR MESSAGE=\"Unknown STYLE\"",
        b"SESSION STATUS RESULT=DUPLICATED_ID",
        b"STREAM STATUS RESULT=CANT_REACH_PEER",
    ] {
        let mut reply = prefix.to_vec();
        reply.extend_from_slice(data);
        if let Ok(reply) = parse_reply_block(&reply, &SamLimits::default()) {
            match reply.kind() {
                SamReplyKind::Datagram | SamReplyKind::Raw => {
                    let _ = reply.is_delivery();
                    let _ = reply.number::<u32>("SIZE");
                    let _ = reply.number_or("PROTOCOL", 0u8);
                }
                _ => {
                    let _ = reply.require_ok();
                }
            }
            let _ = reply.destination("DESTINATION");
        }
    }
});