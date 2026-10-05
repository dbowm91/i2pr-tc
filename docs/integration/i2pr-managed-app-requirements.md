# i2pr Managed-App Integration Requirements for i2pr-tc

Status: interface requirements; not claims that current i2pr implements them

Upstream line reviewed: `dbowm91/i2pr:codex/plan-345-native-app-runtime`.

The current managed-app contract is intentionally pre-runtime. These requirements identify the minimum successor surfaces needed by i2pr-tc without weakening ADR 0032.

## 1. SAM capability stream

Production i2pr-tc needs an app-scoped `sam` service stream through the managed capability channel.

Requirements:

- no application access to the router's host SAM listen socket is required;
- no router administrator credential is exposed;
- app principal/session ownership remains explicit;
- one app can maintain a long-lived SAM session with multiple peer/tracker operations;
- reconnect/cancellation/backpressure are representable;
- the app's I2P Destination is not router identity.

i2pr-tc should implement SAM over generic injected async byte streams so this adapter does not require a torrent-specific router protocol.

## 2. Published local service ingress

Transmission compatibility needs a host-facing endpoint without letting the app bind loopback.

Required semantics:

- administrator/host chooses whether the service is published;
- host owns bind address/port or Unix-domain equivalent;
- service is local-only by default;
- accepted streams are forwarded to the specific app principal;
- bounded concurrent connections and stream bytes;
- app cannot change bind scope;
- stop/restart tears down ingress deterministically;
- no capability implies no listener.

This should be generic enough for mail/IRC/other managed apps.

## 3. Persistent application data

The backend needs durable torrent metadata and potentially large downloaded data.

The first managed profile may use a private per-app persistent root with quota. The runtime should define persistence across app restart/update, ownership/permissions, quota limits, deletion/uninstall semantics, and whether paths are stable or opaque handles.

Arbitrary host download directories are not required initially. A future scoped-filesystem grant may add them without changing the torrent core.

## 4. Private host-to-app service invocation

Router update distribution needs a control path that is not user-facing Transmission RPC and does not hand the app administrator authority.

A suitable generic shape is a host-opened app service stream or bounded app-specific request/reply channel scoped to one app principal.

It must support request correlation, cancellation, typed failure, and bounded payloads/streams. The router remains caller/authority.

Do not overload `ui_message`, public Transmission RPC, or Proposal 170 administrator credentials for this purpose.

## 5. Artifact staging/export

The router must receive a completed update artifact without granting the torrent app arbitrary install-directory access.

Acceptable eventual patterns include an AppManager-owned staging directory capability, an opaque artifact handle exported from app-private storage, or a capability-scoped random-access file resource.

Whichever design i2pr selects must bind resource ownership to app/request, apply length/quota limits, survive or fail deterministically on process crash, and allow router-side verification before installation.

## 6. Dependency status

i2pr-tc M004 remains blocked until the production SAM gateway, local ingress, and persistent-data semantics are stable enough for a real secured launch.

M005 additionally waits on private host-to-app invocation and artifact handoff plus the router's ReleaseTarget/update-authority contract.

These blockers must not be bypassed with `UnsafeDirect` as the default production profile.
