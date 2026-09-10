#!/usr/bin/env python3
"""Real exec/SIGKILL tests of the production gate; no network or CloudKit."""
from __future__ import annotations
import json
import os
import time
import selectors
import subprocess
import sys
import tempfile
from pathlib import Path

probe = sys.argv[1]
children: list[subprocess.Popen[str]] = []
buffers: dict[int, bytearray] = {}

def start(mode: str, directory: Path) -> subprocess.Popen[str]:
    child = subprocess.Popen([probe, mode, str(directory)], stdin=subprocess.PIPE,
                             stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True, bufsize=1)
    children.append(child)
    buffers[child.pid] = bytearray()
    return child

def line(child: subprocess.Popen[str], expected: str | None = None) -> str:
    # Read raw pipe bytes with an explicit buffer. TextIOWrapper.read-ahead
    # can otherwise hide an already-emitted second line from select().
    buffer = buffers[child.pid]
    deadline = time.monotonic() + 15
    with selectors.DefaultSelector() as selector:
        selector.register(child.stdout, selectors.EVENT_READ)
        while b"\n" not in buffer:
            remaining = deadline - time.monotonic()
            assert remaining > 0 and selector.select(remaining), "Timed out waiting for subprocess"
            chunk = os.read(child.stdout.fileno(), 4096)
            assert chunk, "Subprocess exited without a complete response"
            buffer.extend(chunk)
    raw, _, remainder = buffer.partition(b"\n")
    buffers[child.pid] = bytearray(remainder)
    result = raw.decode().strip()
    assert result and not result.startswith("ERROR:"), result

    if expected is not None:
        assert result == expected, (result, expected)
    return result

def command(child: subprocess.Popen[str], value: str, expected: str) -> None:
    child.stdin.write(value + "\n")
    child.stdin.flush()
    line(child, expected)

def once(mode: str, directory: Path) -> str:
    result = subprocess.run([probe, mode, str(directory)], capture_output=True, text=True, timeout=15)
    assert result.returncode == 0, result.stdout + result.stderr
    return result.stdout.strip()

def kill(child: subprocess.Popen[str]) -> None:
    child.kill()
    child.wait(timeout=15)

def snapshot(directory: Path) -> dict:
    return json.loads(once("inspect", directory))

try:
    with tempfile.TemporaryDirectory(prefix="bigsync-processes-") as temporary:
        base = Path(temporary)
        # A settled request still holds the shared lease during its ack scope.
        directory = base / "peer-drain"
        peer = start("peer", directory); line(peer, "admitted")
        command(peer, "submit", "submitted")
        owner = start("owner", directory); line(owner, "fenced")
        assert once("try-admit", directory) == "blocked"
        assert once("try-recover", directory) == "busy"
        command(peer, "settle", "settled")
        assert snapshot(directory)["outstandingSubmissions"] == []
        assert not buffers[owner.pid], "Cutoff emitted completion while peer was admitted"
        with selectors.DefaultSelector() as selector:
            selector.register(owner.stdout, selectors.EVENT_READ)
            assert not selector.select(0.2), "Cutoff passed a still-admitted acknowledgement scope"
        command(peer, "release", "released"); line(owner, "drained")
        command(owner, "final", "final-settled")
        assert once("try-admit", directory) == "blocked"
        command(owner, "reserve", "recovery-required")
        command(owner, "abort", "abort-rejected")
        command(owner, "resolve", "resolved")
        assert once("try-admit", directory) == "admitted"
        print("PASS: peer drain, owner-only final batch, ack lifetime and reservation sealing")

        # SIGKILL releases a peer's actual shared lease, not an invented flag.
        directory = base / "peer-crash-before-submit"
        peer = start("peer", directory); line(peer, "admitted")
        owner = start("owner", directory); line(owner, "fenced")
        kill(peer); line(owner, "drained")
        command(owner, "abort", "aborted")
        print("PASS: peer death before submission releases OS admission")

        # A submitted-but-unsettled remote request cannot be certified by death.
        directory = base / "peer-crash-after-submit"
        peer = start("peer", directory); line(peer, "admitted")
        command(peer, "submit", "submitted"); kill(peer)
        owner = start("owner", directory); line(owner, "fenced"); line(owner, "unresolved")
        owner.wait(timeout=15)
        assert len(snapshot(directory)["outstandingSubmissions"]) == 1
        assert once("try-admit", directory) == "blocked"
        assert once("try-recover", directory) == "recoverable"
        assert once("recover", directory) == "recovered"
        assert snapshot(directory)["outstandingSubmissions"] == []
        print("PASS: submitted peer death stays fail-closed until explicit simulated settlement")

        # Owner death never makes durable preparing look like an open gate.
        directory = base / "owner-crash"
        owner = start("owner", directory); line(owner, "fenced"); line(owner, "drained")
        kill(owner)
        assert once("try-admit", directory) == "blocked"
        assert snapshot(directory)["barrier"]["phase"] == "preparing"
        assert once("recover", directory) == "recovered"
        assert once("try-admit", directory) == "admitted"
        print("PASS: owner death requires explicit durable recovery")

        directory = base / "reserved-owner-crash"
        owner = start("owner", directory); line(owner, "fenced"); line(owner, "drained")
        command(owner, "final", "final-settled"); command(owner, "reserve", "recovery-required")
        kill(owner)
        assert once("try-admit", directory) == "blocked"
        assert snapshot(directory)["barrier"]["phase"] == "recoveryRequired"
        assert once("recover", directory) == "recovered"
        print("PASS: post-reservation owner death does not reopen transport")
finally:
    for child in children:
        if child.poll() is None:
            child.kill()
        child.wait(timeout=15)
