"""Minimal client for weir's admin socket (Foundations spec §11), for the
chaos harness's admin_pause_resume trigger. Stdlib only.

It authenticates the SERVER the way weir-ctl does (§11 "Client-side server
authentication"): the path must be a 0600 socket in a parent that is not
group- or world-writable, and on Linux the peer must be root or our own
euid. A harness that sent `drain.pause` to an impostor and believed the
reply would report a pause that never happened.

`call` returns the response ENVELOPE: an `ok:false` answer is weir's verdict
on the request and is returned for the trigger to judge. `AdminError` is
raised only when there is no trustworthy verdict at all: a transport,
framing, identity or envelope fault, or a connection-level refusal
(`forbidden`, `busy`) sent before any request was read.
"""
import json
import os
import socket
import stat
import struct

PROTOCOL_VERSION = 1
#: spec §11: the 64 KiB line limit, enforced while reading.
MAX_LINE_BYTES = 64 * 1024
_RECV_CHUNK = 4096


class AdminError(Exception):
    """A transport, framing, identity or envelope fault, never an `ok:false`."""


def check_socket_path(path):
    """lstat checks before connecting: S_IFSOCK, mode 0600, and a parent that
    is not group- or world-writable (a squatter there could impersonate the
    control plane). Raises AdminError."""
    try:
        st = os.lstat(path)
    except FileNotFoundError:
        raise AdminError(f"no admin socket at {path}") from None
    except OSError as exc:
        raise AdminError(f"lstat {path}: {exc}") from None
    if not stat.S_ISSOCK(st.st_mode):
        raise AdminError(f"{path} is not a socket")
    mode = stat.S_IMODE(st.st_mode)
    if mode != 0o600:
        raise AdminError(f"{path} has mode {mode:o}, expected 600")
    parent_path = os.path.dirname(path) or "."
    try:
        parent = os.lstat(parent_path)
    except OSError as exc:
        raise AdminError(f"lstat {parent_path}: {exc}") from None
    if parent.st_mode & 0o022:
        raise AdminError(f"the parent of {path} is group- or world-writable")


def peer_uid(sock):
    """The peer's uid via SO_PEERCRED, or None where the platform has none."""
    opt = getattr(socket, "SO_PEERCRED", None)
    if opt is None:
        return None
    # struct ucred: pid_t (signed), uid_t and gid_t (unsigned).
    fmt = "iII"
    raw = sock.getsockopt(socket.SOL_SOCKET, opt, struct.calcsize(fmt))
    _pid, uid, _gid = struct.unpack(fmt, raw)
    return uid


def _read_line(sock, limit=MAX_LINE_BYTES):
    """One `\\n`-terminated line, without the newline. Bounded: refuses once
    `limit` bytes have arrived with no newline, rather than buffering a
    runaway peer forever."""
    buf = bytearray()
    while True:
        chunk = sock.recv(_RECV_CHUNK)
        if not chunk:
            raise AdminError("admin socket closed before a full response line")
        buf += chunk
        nl = buf.find(b"\n")
        if nl >= 0:
            if nl > limit:
                raise AdminError(f"admin response exceeds {limit} bytes")
            return bytes(buf[:nl])
        if len(buf) > limit:
            raise AdminError(f"admin response exceeds {limit} bytes without a newline")


def _check_envelope(resp, request_id):
    if not isinstance(resp, dict):
        raise AdminError("response is not a JSON object")
    if resp.get("v") != PROTOCOL_VERSION:
        raise AdminError(f"unexpected protocol version {resp.get('v')!r}")
    error = resp.get("error")
    if resp.get("id") is None and resp.get("ok") is False and isinstance(error, dict):
        # The accept loop's refusals (`forbidden`, `busy`) carry `id: null`:
        # they answer the CONNECTION, not this request, which was never read.
        raise AdminError(
            f"admin socket refused the connection: {error.get('code')}: "
            f"{error.get('message')}")
    if resp.get("id") != request_id:
        raise AdminError(f"response id {resp.get('id')!r} != request id {request_id!r}")
    if "instance" not in resp:
        raise AdminError(
            "response has no `instance` block (spec §11: every response carries one)")
    if not isinstance(resp.get("ok"), bool):
        raise AdminError(f"response `ok` is {resp.get('ok')!r}, not a boolean")


def call(path, verb, args=None, request_id=1, timeout_secs=10.0, expected_uids=None):
    """One request, one response, on a fresh connection. Returns the parsed
    envelope (`ok:false` included). `expected_uids` defaults to {0, our euid}.
    Raises AdminError on any transport, framing, identity or envelope fault."""
    check_socket_path(path)
    allowed = expected_uids if expected_uids is not None else {0, os.geteuid()}
    with socket.socket(socket.AF_UNIX, socket.SOCK_STREAM) as s:
        s.settimeout(timeout_secs)
        try:
            s.connect(path)
        except OSError as exc:
            raise AdminError(f"connect {path}: {exc}") from None
        try:
            uid = peer_uid(s)
        except OSError as exc:
            raise AdminError(f"peer credentials of {path}: {exc}") from None
        if uid is not None and uid not in allowed:
            raise AdminError(
                f"identity mismatch: peer uid {uid} not in {sorted(allowed)}")
        req = {"v": PROTOCOL_VERSION, "id": request_id, "verb": verb, "args": args or {}}
        try:
            s.sendall(json.dumps(req).encode() + b"\n")
            line = _read_line(s)
        except socket.timeout:
            raise AdminError(f"no response within {timeout_secs:g}s") from None
        except OSError as exc:
            raise AdminError(f"admin socket {path}: {exc}") from None
    try:
        resp = json.loads(line)
    except ValueError as exc:
        raise AdminError(f"response is not JSON: {exc}") from None
    _check_envelope(resp, request_id)
    return resp
