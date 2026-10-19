"""Tests for the harness's admin-socket client, against a real AF_UNIX server
in a thread (works on macOS and Linux; no root, no daemon)."""
import json
import os
import shutil
import socket
import tempfile
import threading
import unittest

import admin

INSTANCE = {"pid": 1, "started_at_ms": 0, "wab_dir": "/w", "socket_path": "/s",
            "wab_dir_dev": 1, "wab_dir_ino": 2}


class Server:
    """Accepts one connection, records the request line, replies `reply`.

    `reply=None` holds the connection open without answering until close().
    """

    def __init__(self, reply, mode=0o600):
        # /tmp keeps the path well under sun_path's 104-byte limit on macOS.
        self.dir = tempfile.mkdtemp(prefix="wa", dir="/tmp")
        self.path = os.path.join(self.dir, "a.sock")
        self.sock = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        self.sock.bind(self.path)
        os.chmod(self.path, mode)
        self.sock.listen(1)
        # A client refused before connecting (mode, parent) never arrives;
        # the timeout ends this thread instead of leaving it in accept().
        self.sock.settimeout(5.0)
        self.reply = reply
        self.request = None
        self.released = threading.Event()
        self.thread = threading.Thread(target=self._serve, daemon=True)
        self.thread.start()

    def _serve(self):
        try:
            conn, _ = self.sock.accept()
        except OSError:
            return
        with conn:
            buf = b""
            while b"\n" not in buf:
                chunk = conn.recv(4096)
                if not chunk:
                    return
                buf += chunk
            self.request = json.loads(buf.split(b"\n")[0])
            if self.reply is None:
                self.released.wait(5.0)
                return
            try:
                conn.sendall(self.reply)
            except OSError:
                pass  # the client hung up mid-reply (the oversize test)

    def close(self):
        self.released.set()
        self.sock.close()
        shutil.rmtree(self.dir, ignore_errors=True)


def envelope(**kw):
    base = {"v": 1, "id": 1, "instance": INSTANCE, "ok": True, "result": {}}
    base.update(kw)
    return (json.dumps(base) + "\n").encode()


class TestAdminCall(unittest.TestCase):
    def _with(self, reply, mode=0o600):
        s = Server(reply, mode)
        self.addCleanup(s.close)
        return s

    def test_a_request_is_one_v1_json_line_and_the_envelope_comes_back(self):
        s = self._with(envelope(result={"paused": True}))
        resp = admin.call(s.path, "drain.pause", {"wait_ms": 5000})
        self.assertTrue(resp["ok"])
        self.assertEqual(resp["result"], {"paused": True})
        self.assertEqual(s.request, {"v": 1, "id": 1, "verb": "drain.pause",
                                     "args": {"wait_ms": 5000}})

    def test_an_error_response_is_returned_not_raised(self):
        s = self._with(envelope(ok=False, error={"code": "shutting_down",
                                                 "message": "m", "retryable": False}))
        resp = admin.call(s.path, "drain.pause")
        self.assertFalse(resp["ok"])
        self.assertEqual(resp["error"]["code"], "shutting_down")

    def test_a_response_without_an_instance_block_is_refused(self):
        bad = (json.dumps({"v": 1, "id": 1, "ok": True, "result": {}}) + "\n").encode()
        s = self._with(bad)
        with self.assertRaises(admin.AdminError):
            admin.call(s.path, "status")

    def test_a_mismatched_id_is_refused(self):
        s = self._with(envelope(id=99))
        with self.assertRaises(admin.AdminError):
            admin.call(s.path, "status")

    def test_a_wrong_protocol_version_is_refused(self):
        s = self._with(envelope(v=2))
        with self.assertRaises(admin.AdminError):
            admin.call(s.path, "status")

    def test_a_response_that_is_not_a_json_object_is_refused(self):
        s = self._with(b"[1, 2]\n")
        with self.assertRaises(admin.AdminError):
            admin.call(s.path, "status")

    def test_a_response_without_a_boolean_ok_is_refused(self):
        s = self._with(envelope(ok="yes"))
        with self.assertRaises(admin.AdminError):
            admin.call(s.path, "status")

    def test_a_busy_refusal_of_the_connection_is_raised_with_its_code(self):
        line = (json.dumps({"v": 1, "id": None, "instance": INSTANCE, "ok": False,
                            "error": {"code": "busy", "message": "m",
                                      "retryable": True}}) + "\n").encode()
        s = self._with(line)
        with self.assertRaises(admin.AdminError) as cm:
            admin.call(s.path, "drain.pause")
        self.assertIn("busy", str(cm.exception))

    def test_a_forbidden_refusal_without_an_instance_is_raised_with_its_code(self):
        line = (json.dumps({"v": 1, "id": None, "ok": False,
                            "error": {"code": "forbidden", "message": "forbidden",
                                      "retryable": False}}) + "\n").encode()
        s = self._with(line)
        with self.assertRaises(admin.AdminError) as cm:
            admin.call(s.path, "drain.pause")
        self.assertIn("forbidden", str(cm.exception))

    def test_an_oversized_line_is_refused_without_buffering_forever(self):
        s = self._with(b"x" * (admin.MAX_LINE_BYTES + 4096))
        with self.assertRaises(admin.AdminError) as cm:
            admin.call(s.path, "status")
        self.assertIn("exceeds", str(cm.exception))

    def test_a_connection_closed_before_a_full_line_is_refused(self):
        s = self._with(b'{"v":1')
        with self.assertRaises(admin.AdminError) as cm:
            admin.call(s.path, "status")
        self.assertIn("closed before", str(cm.exception))

    def test_a_server_that_never_answers_times_out_as_an_admin_error(self):
        s = self._with(None)
        with self.assertRaises(admin.AdminError) as cm:
            admin.call(s.path, "status", timeout_secs=0.2)
        self.assertIn("no response", str(cm.exception))

    def test_a_socket_that_is_not_0600_is_refused_before_connecting(self):
        s = self._with(envelope(), mode=0o666)
        with self.assertRaises(admin.AdminError) as cm:
            admin.call(s.path, "status")
        self.assertIn("mode", str(cm.exception))

    def test_a_group_writable_parent_is_refused(self):
        s = self._with(envelope())
        os.chmod(s.dir, 0o770)
        self.addCleanup(os.chmod, s.dir, 0o700)
        with self.assertRaises(admin.AdminError):
            admin.call(s.path, "status")

    def test_a_missing_path_is_an_admin_error(self):
        d = tempfile.mkdtemp(prefix="wa", dir="/tmp")
        self.addCleanup(shutil.rmtree, d, True)
        with self.assertRaises(admin.AdminError):
            admin.call(os.path.join(d, "absent.sock"), "status")

    def test_a_regular_file_is_not_a_socket(self):
        d = tempfile.mkdtemp(prefix="wa", dir="/tmp")
        self.addCleanup(shutil.rmtree, d, True)
        p = os.path.join(d, "f")
        open(p, "w").close()
        os.chmod(p, 0o600)
        with self.assertRaises(admin.AdminError):
            admin.call(p, "status")

    @unittest.skipUnless(hasattr(socket, "SO_PEERCRED"), "Linux peer credentials")
    def test_a_peer_with_an_unexpected_uid_is_an_identity_mismatch(self):
        s = self._with(envelope())
        with self.assertRaises(admin.AdminError) as cm:
            admin.call(s.path, "status", expected_uids={os.geteuid() + 1})
        self.assertIn("identity mismatch", str(cm.exception))

    @unittest.skipUnless(hasattr(socket, "SO_PEERCRED"), "Linux peer credentials")
    def test_peer_uid_is_our_own_euid_for_a_server_we_run(self):
        s = self._with(envelope())
        with socket.socket(socket.AF_UNIX, socket.SOCK_STREAM) as c:
            c.connect(s.path)
            self.assertEqual(admin.peer_uid(c), os.geteuid())
            c.sendall(b'{"v":1,"id":1,"verb":"status","args":{}}\n')


if __name__ == "__main__":
    unittest.main()
