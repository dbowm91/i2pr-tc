#!/usr/bin/env python3
"""Guard the dependency and source boundary for torrent foundation crates."""

from __future__ import annotations

import argparse
import re
import sys
import tempfile
import tomllib
from pathlib import Path


ALLOWED_DEPENDENCIES = {
    "i2pr-tc-core": {
        "sha1",
        "thiserror",
        "serde",
        "sha2",
        "hmac",
        "subtle",
        "zeroize",
    },
    "i2pr-tc-storage": {
        "i2pr-tc-core",
        "sha1",
        "thiserror",
        "serde",
        "serde_json",
    },
    "i2pr-tc-i2p": {
        "i2pr-tc-core",
        "i2pr-tc-storage",
        "async-trait",
        "sha1",
        "sha2",
        "thiserror",
        "tokio",
    },
    "i2pr-tc-transmission": {
        "base64",
        "fs2",
        "i2pr-tc-core",
        "i2pr-tc-storage",
        "serde",
        "serde_json",
        "thiserror",
        "tokio",
    },
}
FORBIDDEN_SOURCE = re.compile(
    r"(?:std|core)::net\b|tokio::net\b|(?:reqwest|hyper|ureq)::|"
    r"\b(?:TcpStream|TcpListener|UdpSocket)\b|std::process::Command|Command::new|"
    r"\bi2pr_(?:daemon|router|sam|proto|app|runtime)\b",
    re.IGNORECASE,
)
TRANSMISSION_DOMAIN = re.compile(r"\btransmission(?:_rpc)?\b", re.IGNORECASE)

# Crates whose network-visible identity must stay free of version entropy.
# `i2pr-tc-transmission` is excluded on purpose: its `session-id` is a
# loopback RPC CSRF token for a locally trusted frontend, not a value observed
# by a peer, tracker, or router.
FINGERPRINT_CRATES = {"i2pr-tc-core", "i2pr-tc-storage", "i2pr-tc-i2p"}
VERSION_FINGERPRINT = re.compile(
    r"i2pr[-_]tc[/ ]v?\d|\bCARGO_PKG_(?:VERSION|NAME)\b|1:v\d+:" , re.IGNORECASE
)
# An explicit, greppable exemption for a line that must mention the forbidden
# value in order to assert its absence.
EXEMPTION_MARKER = "boundary-guard:allow"
# A host-socket connector is permitted only in a test target that declares
# itself as such. The managed production profile never compiles these files, so
# this marker is what proves a direct local connector stays out of it.
TEST_ONLY_SOCKET_MARKER = "boundary-guard:test-only"
# Sockets and subprocesses, which are forbidden in library source but are the
# point of a test-only connector.
HOST_SOCKET = re.compile(
    r"(?:std|core)::net\b|tokio::net\b|\b(?:TcpStream|TcpListener|UdpSocket)\b|"
    r"std::process::Command|Command::new",
    re.IGNORECASE,
)


def check(root: Path) -> tuple[list[str], list[str]]:
    """Return the boundary violations and the declared test-only connectors.

    The connectors are returned rather than printed here because they are
    evidence: every declared test-only connector is a place where a host socket
    exists in this repository, and a reader of a verification run must be able
    to see the actual set rather than take a fixed list on trust. A new
    connector that is correctly declared still has to appear.
    """
    failures: list[str] = []
    test_only_connectors: list[str] = []
    for crate, allowed in ALLOWED_DEPENDENCIES.items():
        manifest = root / "crates" / crate / "Cargo.toml"
        if not manifest.is_file():
            failures.append(f"missing manifest: {manifest.relative_to(root)}")
            continue
        with manifest.open("rb") as stream:
            data = tomllib.load(stream)
        dependencies = set(data.get("dependencies", {}))
        unexpected = dependencies - allowed
        if unexpected:
            failures.append(f"{crate} has forbidden dependencies: {sorted(unexpected)}")
        source = manifest.parent / "src"
        rust_files = sorted(source.rglob("*.rs"))
        if not rust_files:
            failures.append(f"{crate} has no Rust source files")
        # Socket and subprocess rules apply to library source only. The
        # Transmission crate's `tests/` directory holds a documented test-only
        # loopback listener that is the point of that crate, so scanning it for
        # host sockets would report the intended design as a violation.
        for path in rust_files:
            for line_number, line in enumerate(path.read_text(encoding="utf-8").splitlines(), 1):
                relative = path.relative_to(root)
                if FORBIDDEN_SOURCE.search(line) or (
                    crate != "i2pr-tc-transmission" and TRANSMISSION_DOMAIN.search(line)
                ):
                    failures.append(f"{relative}:{line_number}: forbidden boundary reference")
                if (
                    crate in FINGERPRINT_CRATES
                    and VERSION_FINGERPRINT.search(line)
                    and EXEMPTION_MARKER not in line
                ):
                    failures.append(
                        f"{relative}:{line_number}: reintroduced version fingerprint"
                    )
        # Version fingerprints are also checked in integration tests, where an
        # HTTP golden test would otherwise be able to reintroduce one silently.
        # A host socket in an integration test is the one place a direct local
        # connector may exist, so it has to be declared as test-only.
        tests = manifest.parent / "tests"
        if tests.is_dir():
            for path in sorted(tests.rglob("*.rs")):
                text = path.read_text(encoding="utf-8")
                relative = path.relative_to(root)
                declared = TEST_ONLY_SOCKET_MARKER in text
                for line_number, line in enumerate(text.splitlines(), 1):
                    if crate in FINGERPRINT_CRATES and (
                        VERSION_FINGERPRINT.search(line) and EXEMPTION_MARKER not in line
                    ):
                        failures.append(
                            f"{relative}:{line_number}: reintroduced version fingerprint"
                        )
                    if HOST_SOCKET.search(line):
                        if declared:
                            test_only_connectors.append(f"{relative}:{line_number}")
                        else:
                            failures.append(
                                f"{relative}:{line_number}: host socket in a test target "
                                f"without a '{TEST_ONLY_SOCKET_MARKER}' declaration"
                            )
    return failures, test_only_connectors


def self_test() -> list[str]:
    cases = {
        "use std::net::TcpStream;": True,
        "let x = tokio::net::TcpListener::bind(addr);": True,
        "use reqwest::Client;": True,
        "let x = std::process::Command::new(\"curl\");": True,
        "use i2pr_daemon::Sam;": True,
        "pub struct TransmissionAdapter;": False,
        "use std::fs::File;": False,
        "pub struct TorrentService;": False,
    }
    failed = []
    for source, expected in cases.items():
        actual = FORBIDDEN_SOURCE.search(source) is not None
        if actual != expected:
            failed.append(f"boundary detector self-test mismatch for {source!r}")
    fingerprint_cases = {
        "request.push_str(\"User-Agent: i2pr-tc/0.1\");": True,
        "const V: &str = env!(\"CARGO_PKG_VERSION\");": True,
        "out.extend_from_slice(b\"1:v11:i2pr-tc/0.1e\");": True,
        "let client = \"i2pr-tc 0.1\";": True,
        "request.push_str(\"\\r\\nConnection: close\\r\\n\");": False,
        # boundary-guard:allow — asserting the absence of the removed value.
        'assert!(!request.contains("i2pr-tc/0.1")); // boundary-guard:allow': False,
    }
    for source, expected in fingerprint_cases.items():
        exempt = EXEMPTION_MARKER in source
        actual = VERSION_FINGERPRINT.search(source) is not None and not exempt
        if actual != expected:
            failed.append(f"version fingerprint self-test mismatch for {source!r}")
    with tempfile.TemporaryDirectory(prefix="i2pr-tc-boundary-test-") as tmp:
        root = Path(tmp)
        core = root / "crates/i2pr-tc-core"
        storage = root / "crates/i2pr-tc-storage"
        i2p = root / "crates/i2pr-tc-i2p"
        transmission = root / "crates/i2pr-tc-transmission"
        (core / "src").mkdir(parents=True)
        (storage / "src").mkdir(parents=True)
        (i2p / "src").mkdir(parents=True)
        (transmission / "src").mkdir(parents=True)
        (core / "Cargo.toml").write_text(
            "[package]\nname='i2pr-tc-core'\n[dependencies]\nsha1='0.10'\n",
            encoding="utf-8",
        )
        (storage / "Cargo.toml").write_text(
            "[package]\nname='i2pr-tc-storage'\n[dependencies]\n"
            "i2pr-tc-core={path='../i2pr-tc-core'}\nsha1='0.10'\n",
            encoding="utf-8",
        )
        (i2p / "Cargo.toml").write_text(
            "[package]\nname='i2pr-tc-i2p'\n[dependencies]\n"
            "i2pr-tc-core={path='../i2pr-tc-core'}\n"
            "i2pr-tc-storage={path='../i2pr-tc-storage'}\n"
            "async-trait='0.1'\nsha1='0.10'\nsha2='0.10'\n"
            "thiserror='2'\ntokio='1'\n",
            encoding="utf-8",
        )
        (transmission / "Cargo.toml").write_text(
            "[package]\nname='i2pr-tc-transmission'\n[dependencies]\n"
            "i2pr-tc-core={path='../i2pr-tc-core'}\n"
            "i2pr-tc-storage={path='../i2pr-tc-storage'}\n"
            "base64='0.22'\nfs2='0.4'\nserde='1'\nserde_json='1'\n"
            "thiserror='2'\ntokio='1'\n",
            encoding="utf-8",
        )
        (core / "src/lib.rs").write_text("use std::fs;\n", encoding="utf-8")
        (storage / "src/lib.rs").write_text("use std::fs;\n", encoding="utf-8")
        (i2p / "src/lib.rs").write_text("use std::fs;\n", encoding="utf-8")
        (transmission / "src/lib.rs").write_text("use std::fs;\n", encoding="utf-8")
        if check(root)[0]:
            failed.append("boundary guard rejected its clean control fixture")
        with (core / "Cargo.toml").open("a", encoding="utf-8") as stream:
            stream.write("tokio='1'\n")
        if not any("forbidden dependencies" in item for item in check(root)[0]):
            failed.append("boundary guard missed an added network-capable dependency")
        with (core / "Cargo.toml").open("w", encoding="utf-8") as stream:
            stream.write("[package]\nname='i2pr-tc-core'\n[dependencies]\nsha1='0.10'\n")
        (core / "src/lib.rs").write_text("use std::net::TcpStream;\n", encoding="utf-8")
        if not any("forbidden boundary reference" in item for item in check(root)[0]):
            failed.append("boundary guard missed a direct socket reference")
        (core / "src/lib.rs").write_text("use std::fs;\n", encoding="utf-8")
        (i2p / "src/peer.rs").write_text(
            "const HEADER: &str = \"User-Agent: i2pr-tc/0.1\";\n", encoding="utf-8"
        )
        if not any("reintroduced version fingerprint" in item for item in check(root)[0]):
            failed.append("boundary guard missed a reintroduced version fingerprint")
        (i2p / "src/peer.rs").write_text(
            "// assert the header stays gone // boundary-guard:allow\n", encoding="utf-8"
        )
        if any("reintroduced version fingerprint" in item for item in check(root)[0]):
            failed.append("boundary guard ignored an explicit exemption marker")
        (i2p / "src/peer.rs").unlink()
        # A direct local connector is allowed only in a declared test target.
        tests = i2p / "tests"
        tests.mkdir(exist_ok=True)
        connector = tests / "connector.rs"
        connector.write_text(
            "// boundary-guard:test-only\nuse tokio::net::TcpStream;\n", encoding="utf-8"
        )
        if any("without a" in item for item in check(root)[0]):
            failed.append("boundary guard rejected a declared test-only connector")
        # The declared connector has to be reported as evidence, not merely
        # tolerated: a run that silently drops it would let a new host socket
        # into a test target without ever appearing in the verification output.
        declared = [item.split(":", 1)[0] for item in check(root)[1]]
        if not any(item.endswith("connector.rs") for item in declared):
            failed.append("boundary guard did not report the declared test-only connector")
        connector.write_text("use tokio::net::TcpStream;\n", encoding="utf-8")
        if not any("without a" in item for item in check(root)[0]):
            failed.append("boundary guard missed an undeclared host socket in a test target")
    return failed


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--root", type=Path, default=Path(__file__).resolve().parents[1])
    parser.add_argument("--self-test", action="store_true")
    args = parser.parse_args()
    failures = self_test() if args.self_test else []
    checked, test_only_connectors = check(args.root.resolve())
    failures.extend(checked)
    if failures:
        print("\n".join(failures), file=sys.stderr)
        return 1
    print("foundation dependency and source boundaries passed")
    files = sorted({connector.split(":", 1)[0] for connector in test_only_connectors})
    if files:
        print(
            "declared test-only host connectors (absent from the managed production "
            f"profile): {', '.join(files)}"
        )
    else:
        print("no test-only host connectors are declared")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
