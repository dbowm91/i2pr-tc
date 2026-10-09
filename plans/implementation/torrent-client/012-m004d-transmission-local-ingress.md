# Torrent Client M004-D — Host-Owned Transmission Local Ingress

Status: blocked on upstream Plans 387 and 388

Date: 2026-10-09

Parent milestone:
`plans/implementation/torrent-client/004-i2pr-managed-app-integration.md`

Primary class: integration capability + local exposure invariant.

Hard dependencies:

- M003 closed.
- M004-A closed.
- upstream Plan 387 closed — host-owned local-service ingress.
- upstream Plan 388 closed.

M004-C Secured closure is required before this contributes to final production
M004 closure, but the ingress adapter itself may be developed independently
once 387/388 close.

## 1. Objective

Expose the existing Transmission-compatible RPC endpoint to local desktop/tools
without giving the torrent process direct loopback/listener authority.

The host binds loopback. The app receives each accepted connection as a logical
managed-app stream and runs the existing transport-independent RPC handler over
that byte stream.

## 2. Capability use

The package requests the exact Plan-387 local-service capability and the
administrator explicitly grants it.

No use of:

- `brokered_tcp`;
- UnsafeDirect networking;
- app-owned `TcpListener`;
- wildcard/LAN/public bind;
- router administrator token.

If the local-service grant is absent, torrent operation continues without RPC.

## 3. Publication lifecycle

At application readiness:

1. request one named Transmission RPC local service;
2. receive the host-owned service id + bound loopback endpoint;
3. accept bounded incoming logical streams;
4. feed each stream into the existing M003 request/session-id logic.

On stop/revocation:

- unpublish listener;
- stop admission;
- cancel/drain active RPC connections within deadline;
- leave no listener behind.

A bind conflict is surfaced explicitly.

## 4. RPC security posture

Loopback origin is not authentication.

Retain M003's Transmission session-id/409 behavior and strict request/body
bounds. If authentication is later desired, it requires its own explicit plan.

Do not trust forwarding metadata supplied by a local client.

Connection floods and slow clients must be bounded by both Plan-387 host
ceilings and application-side RPC ceilings.

## 5. Work packages

WP1 — SDK adapter for Plan-387 publication/incoming streams.

WP2 — compose incoming logical streams into M003 RPC handler.

WP3 — lifecycle/revocation/bind-conflict behavior.

WP4 — compatibility tests with representative Transmission clients or fixtures.

WP5 — negative no-listener/no-LAN/no-direct-socket guards and closure.

## 6. Acceptance criteria

M004-D closes when:

1. host/runtime owns the only local listener;
2. a local independent client completes Transmission RPC over forwarded streams;
3. session-id negotiation and supported RPC semantics remain unchanged;
4. the app has no production listener/socket code;
5. grant absence leaves torrent backend functional but unpublished;
6. revocation/exit removes listener and active streams;
7. flood/slow-client ceilings hold;
8. routine verification passes.

## 7. Stop conditions

Stop if Plan 387 requires the app to receive a listener fd/socket, if
`brokered_tcp` must be overloaded, if loopback source identity becomes an auth
assumption, or if existing RPC code requires socket-specific behavior rather
than generic byte streams.

## 8. Closure evidence

Create `plans/closure/torrent-client/012-m004d-status.md` with capability and
endpoint evidence, independent RPC transcript, listener ownership proof,
revocation/load tests, no-direct-socket guard, and M004 final-readiness audit.
