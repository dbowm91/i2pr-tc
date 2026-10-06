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
    "i2pr-tc-core": {"sha1", "thiserror", "serde"},
    "i2pr-tc-storage": {
        "i2pr-tc-core",
        "sha1",
        "thiserror",
        "serde",
        "serde_json",
    },
}
FORBIDDEN_SOURCE = re.compile(
    r"(?:std|core)::net\b|tokio::net\b|(?:reqwest|hyper|ureq)::|"
    r"\b(?:TcpStream|TcpListener|UdpSocket)\b|std::process::Command|Command::new|"
    r"\btransmission(?:_rpc)?\b|\bi2pr_(?:daemon|router|sam|proto|app|runtime)\b",
    re.IGNORECASE,
)


def check(root: Path) -> list[str]:
    failures: list[str] = []
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
        for path in rust_files:
            for line_number, line in enumerate(path.read_text(encoding="utf-8").splitlines(), 1):
                if FORBIDDEN_SOURCE.search(line):
                    relative = path.relative_to(root)
                    failures.append(f"{relative}:{line_number}: forbidden boundary reference")
    return failures


def self_test() -> list[str]:
    cases = {
        "use std::net::TcpStream;": True,
        "let x = tokio::net::TcpListener::bind(addr);": True,
        "use reqwest::Client;": True,
        "let x = std::process::Command::new(\"curl\");": True,
        "use i2pr_daemon::Sam;": True,
        "use std::fs::File;": False,
        "pub struct TorrentService;": False,
    }
    failed = []
    for source, expected in cases.items():
        actual = FORBIDDEN_SOURCE.search(source) is not None
        if actual != expected:
            failed.append(f"boundary detector self-test mismatch for {source!r}")
    with tempfile.TemporaryDirectory(prefix="i2pr-tc-boundary-test-") as tmp:
        root = Path(tmp)
        core = root / "crates/i2pr-tc-core"
        storage = root / "crates/i2pr-tc-storage"
        (core / "src").mkdir(parents=True)
        (storage / "src").mkdir(parents=True)
        (core / "Cargo.toml").write_text(
            "[package]\nname='i2pr-tc-core'\n[dependencies]\nsha1='0.10'\n",
            encoding="utf-8",
        )
        (storage / "Cargo.toml").write_text(
            "[package]\nname='i2pr-tc-storage'\n[dependencies]\n"
            "i2pr-tc-core={path='../i2pr-tc-core'}\nsha1='0.10'\n",
            encoding="utf-8",
        )
        (core / "src/lib.rs").write_text("use std::fs;\n", encoding="utf-8")
        (storage / "src/lib.rs").write_text("use std::fs;\n", encoding="utf-8")
        if check(root):
            failed.append("boundary guard rejected its clean control fixture")
        with (core / "Cargo.toml").open("a", encoding="utf-8") as stream:
            stream.write("tokio='1'\n")
        if not any("forbidden dependencies" in item for item in check(root)):
            failed.append("boundary guard missed an added network-capable dependency")
        with (core / "Cargo.toml").open("w", encoding="utf-8") as stream:
            stream.write("[package]\nname='i2pr-tc-core'\n[dependencies]\nsha1='0.10'\n")
        (core / "src/lib.rs").write_text("use std::net::TcpStream;\n", encoding="utf-8")
        if not any("forbidden boundary reference" in item for item in check(root)):
            failed.append("boundary guard missed a direct socket reference")
    return failed


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--root", type=Path, default=Path(__file__).resolve().parents[1])
    parser.add_argument("--self-test", action="store_true")
    args = parser.parse_args()
    failures = self_test() if args.self_test else []
    failures.extend(check(args.root.resolve()))
    if failures:
        print("\n".join(failures), file=sys.stderr)
        return 1
    print("foundation dependency and source boundaries passed")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
