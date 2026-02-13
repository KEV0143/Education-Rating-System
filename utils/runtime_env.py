import os
import socket
import sqlite3
import sys
from pathlib import Path
from typing import List, Optional


def is_frozen() -> bool:
    return bool(getattr(sys, "frozen", False))


def resource_dir() -> Path:
    if is_frozen():
        meipass = getattr(sys, "_MEIPASS", None)
        if meipass:
            return Path(meipass)
    return Path(__file__).resolve().parent.parent


def _is_writable_dir(path: Path) -> bool:
    try:
        path.mkdir(parents=True, exist_ok=True)
        probe = path / ".write_test.tmp"
        probe.write_text("ok", encoding="utf-8")
        probe.unlink(missing_ok=True)
        return True
    except Exception:
        return False


def _runtime_data_candidates(app_dir_name: str) -> List[Path]:
    candidates: List[Path] = []

    custom = (os.environ.get("APP_DATA_DIR") or "").strip()
    if custom:
        candidates.append(Path(custom).expanduser())

    if is_frozen():
        candidates.append(Path(sys.executable).resolve().parent)

        local_appdata = (os.environ.get("LOCALAPPDATA") or "").strip()
        if local_appdata:
            candidates.append(Path(local_appdata) / app_dir_name)

        appdata = (os.environ.get("APPDATA") or "").strip()
        if appdata:
            candidates.append(Path(appdata) / app_dir_name)
    else:
        candidates.append(Path(__file__).resolve().parent.parent)

    unique: List[Path] = []
    seen = set()
    for p in candidates:
        try:
            rp = p.resolve()
        except Exception:
            rp = p
        key = str(rp).lower()
        if key in seen:
            continue
        seen.add(key)
        unique.append(rp)
    return unique


def runtime_data_dir(app_dir_name: str) -> Path:
    for p in _runtime_data_candidates(app_dir_name):
        if _is_writable_dir(p):
            return p
    return Path(__file__).resolve().parent.parent


def ensure_sqlite_file(path: Path) -> None:
    try:
        path.parent.mkdir(parents=True, exist_ok=True)
        if path.exists():
            return
        conn = sqlite3.connect(str(path))
        conn.close()
    except Exception:
        pass


def env_flag(name: str, default: bool) -> bool:
    raw = os.environ.get(name)
    if raw is None:
        return default
    return str(raw).strip().lower() not in {"0", "false", "no", "off"}


def parse_int(value: Optional[str], default: int = 0) -> int:
    try:
        return int(str(value).strip())
    except Exception:
        return default


def pick_available_port(host: str, preferred_port: int, max_tries: int = 24) -> int:
    host = (host or "127.0.0.1").strip() or "127.0.0.1"
    preferred_port = max(1, min(65535, int(preferred_port)))
    max_tries = max(0, int(max_tries))

    for offset in range(max_tries + 1):
        candidate = preferred_port + offset
        if candidate > 65535:
            break
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        try:
            sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            sock.bind((host, candidate))
            return candidate
        except OSError:
            continue
        finally:
            sock.close()
    return preferred_port
