import os
import re
import shutil
import subprocess
import threading
import time
import ipaddress
from collections import deque
from urllib.parse import urlparse

PUBLIC_URL_RE = re.compile(r"https?://[^\s\"'<>]+")
HOST_RE = re.compile(r"\b([a-z0-9-]+\.(?:lhr\.life|localhost\.run))\b", flags=re.IGNORECASE)
ANSI_ESCAPE_RE = re.compile(r"\x1B\[[0-?]*[ -/]*[@-~]")

DISALLOWED_PUBLIC_HOSTS = {"localhost.run", "admin.localhost.run"}
ALLOWED_PUBLIC_SUFFIXES = (".lhr.life", ".localhost.run")
LOCAL_HOSTS = {"127.0.0.1", "localhost", "::1"}


def is_valid_public_url(raw_url: str) -> bool:
    try:
        parsed = urlparse(str(raw_url or "").strip())
    except ValueError:
        return False
    if parsed.scheme not in ("http", "https"):
        return False
    host = (parsed.hostname or "").lower()
    if not host:
        return False
    if host in DISALLOWED_PUBLIC_HOSTS:
        return False
    return host.endswith(ALLOWED_PUBLIC_SUFFIXES)


def extract_public_url(line: str):
    text = str(line or "")
    for raw in PUBLIC_URL_RE.findall(text):
        candidate = raw.rstrip("),.;]")
        if is_valid_public_url(candidate):
            return candidate

    host_match = HOST_RE.search(text)
    if host_match:
        host = host_match.group(1).lower()
        candidate = f"https://{host}"
        if is_valid_public_url(candidate):
            return candidate
    return None


def _normalize_host(value: str) -> str:
    return str(value or "").split(",")[0].strip().split(":")[0].lower().strip("[]")


def _is_local_like_host(host: str) -> bool:
    safe_host = str(host or "").strip().strip("[]").lower()
    if not safe_host:
        return False
    if safe_host in LOCAL_HOSTS:
        return True
    if safe_host.endswith(".local"):
        return True
    try:
        ip_value = ipaddress.ip_address(safe_host)
        return bool(ip_value.is_loopback or ip_value.is_private or ip_value.is_link_local)
    except ValueError:
        return False


def is_local_request(request) -> bool:
    host = _normalize_host(str(request.host or ""))
    forwarded_host = _normalize_host(str(request.headers.get("X-Forwarded-Host", "")))
    return _is_local_like_host(host) or (_is_local_like_host(forwarded_host) if forwarded_host else False)


class JournalTunnelManager:
    def __init__(self) -> None:
        self.lock = threading.Lock()
        self.process = None
        self.reader_thread = None
        self.public_url = None
        self.error_message = None
        self.log_lines = deque(maxlen=80)
        self.closing = False
        self.next_refresh_at = None
        self.allow_reconnect = False
        self.reconnecting = False
        self.max_reconnect_attempts = 5
        self.local_port = 5000
        self.local_host = "127.0.0.1"
        self.on_change = None
        try:
            refresh_seconds = int(os.environ.get("TUNNEL_REFRESH_SECONDS", "300"))
        except ValueError:
            refresh_seconds = 300
        self.refresh_interval_seconds = max(60, refresh_seconds)
        self.refresh_thread = threading.Thread(target=self._refresh_loop, daemon=True)
        self.refresh_thread.start()

    def set_on_change(self, callback) -> None:
        self.on_change = callback

    def _emit_state_change(self) -> None:
        callback = self.on_change
        if callback:
            try:
                callback()
            except Exception:
                pass

    @staticmethod
    def _known_hosts_target() -> str:
        return "NUL" if os.name == "nt" else "/dev/null"

    @staticmethod
    def _compact_error_message(raw: str, max_len: int = 260) -> str:
        text = str(raw or "").replace("\r", "\n")
        lines = [line.strip() for line in text.splitlines() if line.strip()]
        if not lines:
            return ""

        skip_markers = (
            "localhost.run/docs",
            "admin.localhost.run",
            "connection id",
            "authenticated as",
            "http://localhost:3000/docs/faq",
        )
        cleaned = [line for line in lines if not any(marker in line.lower() for marker in skip_markers)]
        base = cleaned[0] if cleaned else lines[0]
        if len(base) > max_len:
            return base[: max_len - 1] + "…"
        return base

    def _is_running(self) -> bool:
        return self.process is not None and self.process.poll() is None

    def _reset_runtime(self, clear_logs: bool = False) -> None:
        self.process = None
        self.reader_thread = None
        self.public_url = None
        self.error_message = None
        self.closing = False
        self.next_refresh_at = None
        if clear_logs:
            self.log_lines.clear()

    def snapshot(self):
        with self.lock:
            return {
                "active": self._is_running(),
                "public_url": self.public_url,
                "error_message": self.error_message,
                "reconnecting": self.reconnecting,
                "next_refresh_epoch": int(self.next_refresh_at) if self.next_refresh_at else None,
                "refresh_interval_seconds": self.refresh_interval_seconds,
                "local_host": self.local_host,
                "local_port": int(self.local_port),
            }

    def build_public_url_for_path(self, path: str):
        safe_path = str(path or "").strip()
        if not safe_path:
            safe_path = "/"
        if not safe_path.startswith("/"):
            safe_path = "/" + safe_path
        with self.lock:
            base = str(self.public_url or "").strip()
        if not base:
            return ""
        return f"{base.rstrip('/')}{safe_path}"

    def _start_reconnect_unlocked(self) -> None:
        if not self.allow_reconnect or self.reconnecting:
            return
        self.reconnecting = True
        self._emit_state_change()
        threading.Thread(target=self._reconnect_worker, daemon=True).start()

    def _reader_loop(self, proc) -> None:
        try:
            assert proc.stdout is not None
            for raw_line in iter(proc.stdout.readline, ""):
                if raw_line == "" and proc.poll() is not None:
                    break
                line = ANSI_ESCAPE_RE.sub("", raw_line).replace("\r", "").strip()
                if not line:
                    continue
                with self.lock:
                    self.log_lines.append(line)
                    if not self.public_url:
                        parsed = extract_public_url(line)
                        if parsed:
                            self.public_url = parsed
                            self.next_refresh_at = time.time() + self.refresh_interval_seconds
                            self.error_message = None
                            self._emit_state_change()
            proc.wait()
        except Exception as exc:
            with self.lock:
                self.error_message = f"Ошибка чтения вывода ssh: {exc}"
        finally:
            with self.lock:
                if self.process is proc and not self.closing:
                    self.next_refresh_at = None
                    if not self.error_message:
                        rc = proc.returncode
                        if rc == 0:
                            self.error_message = "SSH-сессия завершилась. Запускаю переподключение."
                        else:
                            self.error_message = (
                                f"SSH завершился с кодом {rc}. "
                                "Проверьте ssh/интернет и доступность localhost.run."
                            )
                    self._start_reconnect_unlocked()
                    self._emit_state_change()

    def open(self, local_port=None, local_host=None, wait_seconds: float = 12.0):
        if shutil.which("ssh") is None:
            return False, "ssh command not found. Install OpenSSH Client."

        with self.lock:
            if local_port:
                try:
                    self.local_port = int(local_port)
                except Exception:
                    pass
            if local_host:
                self.local_host = str(local_host).strip() or self.local_host

            self.allow_reconnect = True
            if self._is_running():
                if self.public_url:
                    return True, f"Public URL active: {self.public_url}"
                return False, "Tunnel is starting. Please wait a few seconds."

            self._reset_runtime(clear_logs=True)
            self.error_message = None

            forward = f"80:{self.local_host}:{int(self.local_port)}"
            cmd = [
                "ssh",
                "-T",
                "-R",
                forward,
                "nokey@localhost.run",
                "-o",
                "StrictHostKeyChecking=no",
                "-o",
                f"UserKnownHostsFile={self._known_hosts_target()}",
                "-o",
                "ExitOnForwardFailure=yes",
                "-o",
                "ServerAliveInterval=30",
                "-o",
                "ServerAliveCountMax=3",
            ]

            creationflags = getattr(subprocess, "CREATE_NO_WINDOW", 0) if os.name == "nt" else 0
            try:
                proc = subprocess.Popen(
                    cmd,
                    stdout=subprocess.PIPE,
                    stderr=subprocess.STDOUT,
                    text=True,
                    bufsize=1,
                    creationflags=creationflags,
                )
            except OSError as exc:
                return False, f"Failed to start ssh: {exc}"

            self.process = proc
            self.reader_thread = threading.Thread(target=self._reader_loop, args=(proc,), daemon=True)
            self.reader_thread.start()
            self._emit_state_change()

        deadline = time.monotonic() + float(wait_seconds or 0)
        while time.monotonic() < deadline:
            with self.lock:
                if self.public_url:
                    return True, f"Public URL active: {self.public_url}"
                if not self._is_running():
                    details = self.error_message or "ssh exited before public URL was obtained."
                    logs = "\n".join(list(self.log_lines)[-10:])
                    return False, f"{details}\n{logs}".strip()
            time.sleep(0.1)

        with self.lock:
            if self._is_running():
                self.error_message = "Public URL is still initializing. Please wait a few seconds."
                self._emit_state_change()
                return False, "Tunnel is starting. Please wait a few seconds."

            logs = "\n".join(list(self.log_lines)[-10:])
        return (
            False,
            "Could not obtain a valid public URL in time.\n"
            f"{logs}",
        )

    def close(self, manual: bool = True):
        with self.lock:
            if manual:
                self.allow_reconnect = False
            proc = self.process
            if proc is None or proc.poll() is not None:
                self._reset_runtime(clear_logs=False)
                return True, "Туннель уже закрыт."
            self.closing = True

        try:
            proc.terminate()
            proc.wait(timeout=3)
        except Exception:
            try:
                proc.kill()
                proc.wait(timeout=2)
            except Exception as exc:
                with self.lock:
                    self.closing = False
                return False, f"Не удалось завершить ssh-процесс: {exc}"

        with self.lock:
            self._reset_runtime(clear_logs=False)
            self.reconnecting = False
            self._emit_state_change()
        return True, "Туннель закрыт."

    def _refresh_loop(self) -> None:
        while True:
            time.sleep(1)
            with self.lock:
                if (
                    not self.allow_reconnect
                    or self.closing
                    or self.reconnecting
                    or not self._is_running()
                    or not self.next_refresh_at
                    or time.time() < self.next_refresh_at
                ):
                    continue
                self.reconnecting = True
                self.error_message = "Scheduled public link rotation in progress..."
                self.log_lines.append("[AUTO] Scheduled tunnel rotation started.")
                self._emit_state_change()

            closed, close_message = self.close(manual=False)
            if not closed:
                with self.lock:
                    self.reconnecting = False
                    compact_close = self._compact_error_message(close_message)
                    if compact_close:
                        self.error_message = f"Failed to close tunnel for rotation. {compact_close}"
                    else:
                        self.error_message = "Failed to close tunnel for rotation."
                    self._emit_state_change()
                continue

            with self.lock:
                if not self.allow_reconnect:
                    self.reconnecting = False
                    self._emit_state_change()
                    continue

            ok, message = self.open(wait_seconds=18.0)
            with self.lock:
                compact_message = self._compact_error_message(message)
                self.reconnecting = False
                if ok:
                    self.error_message = None
                    self.log_lines.append("[AUTO] Public URL rotated automatically.")
                elif self._is_running():
                    self.error_message = "Public URL is still updating. Please wait a few seconds."
                    self.log_lines.append("[AUTO] Waiting for a new public URL after rotation.")
                else:
                    if compact_message:
                        self.error_message = f"Automatic public URL refresh failed. {compact_message}"
                    else:
                        self.error_message = "Automatic public URL refresh failed."
                    self.log_lines.append("[AUTO] Rotation failed, starting reconnect attempts.")
                    self._start_reconnect_unlocked()
                self._emit_state_change()

    def _reconnect_worker(self) -> None:
        last_error = ""
        for attempt in range(1, self.max_reconnect_attempts + 1):
            with self.lock:
                if not self.allow_reconnect:
                    self.reconnecting = False
                    return
                self.log_lines.append(f"[AUTO] Reconnect attempt {attempt}/{self.max_reconnect_attempts}")
            ok, message = self.open(wait_seconds=12.0)
            if ok:
                with self.lock:
                    self.reconnecting = False
                    self.error_message = None
                    self.log_lines.append("[AUTO] Tunnel reconnected automatically.")
                    self._emit_state_change()
                return
            last_error = message
            time.sleep(min(8, attempt * 2))

        with self.lock:
            self.reconnecting = False
            safe_last_error = self._compact_error_message(last_error)
            self.error_message = (
                "Tunnel disconnected and could not reconnect automatically. "
                f"{safe_last_error or 'Check internet connectivity and open QR again.'}"
            )
            self._emit_state_change()
