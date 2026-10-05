# i2pr-tc

A Rust BitTorrent client purpose-built for the I2P network and intended to run
as a managed native application alongside i2pr.

The project prioritizes:

- I2P-only peer and tracker operation;
- a small, auditable torrent backend before frontend work;
- resilience to long latency, disconnects, and intermittent peers;
- Transmission RPC compatibility for existing client tooling;
- a strict separation between torrent transport and software-update trust;
- eventual integration with i2pr through its managed-app/SAM capability
  boundary rather than unrestricted host networking.

The repository is currently in planning/bootstrap. See `plans/registry.md` on
the active planning branch for implementation handoff status.
