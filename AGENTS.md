# i2pr-tc agent guidance

This repository uses the planning conventions established by CodeGG. Planning documents are implementation contracts, not informal TODO lists.

## Authority order

When documents disagree, use this order unless a later accepted ADR explicitly supersedes it:

1. `plans/000-long-term-specification.md`
2. `plans/001-terminology-and-domain-model.md`
3. accepted ADRs under `docs/adr/`
4. `plans/002-long-term-roadmap.md`
5. subsystem roadmaps
6. the active milestone implementation plan
7. current repository evidence

Do not silently weaken a higher-level invariant to make an implementation plan easier.

## Non-negotiable product boundaries

- Production peer, tracker, discovery, and naming traffic is I2P-only.
- The managed production profile must not require unrestricted host networking, loopback access, public DNS, UPnP, NAT-PMP, LPD, or clearnet fallback.
- I2P peer identity is represented as Destination/Destination-hash semantics, not as canonical IP `SocketAddr` state.
- The torrent client may transport router-update artifacts, but it never owns trusted release metadata, update signing keys, rollback/freeze policy, platform selection, installation, or restart authority.
- Transmission compatibility is an adapter over the torrent service. It must not become the internal domain model.
- Frontend/UI work is downstream of the backend milestones registered here.

Implement only plans marked `ready` in `plans/registry.md`. A blocked plan may be inspected or refined, but its blocked production integration must not be bypassed.

Each milestone must create a closure record under `plans/closure/<subsystem>/NNN-status.md` with exact implementation commits, tests, compatibility/security evidence, unresolved findings, and a closure recommendation. Compilation alone is not closure.

Before implementation, re-check referenced i2pr contracts because the managed native-app runtime is under active development.
