import threading
import time


class RealtimeEventBus:
    def __init__(self) -> None:
        self._cond = threading.Condition()
        self._versions: dict[str, int] = {}

    def get_version(self, key: str) -> int:
        safe_key = str(key or "")
        with self._cond:
            return int(self._versions.get(safe_key, 0))

    def bump(self, key: str) -> None:
        safe_key = str(key or "")
        with self._cond:
            self._versions[safe_key] = int(self._versions.get(safe_key, 0)) + 1
            self._cond.notify_all()

    def wait_for_change(self, key: str, last_version: int, timeout: float = 30.0) -> int:
        safe_key = str(key or "")
        deadline = time.monotonic() + float(timeout or 0)
        with self._cond:
            while True:
                current = int(self._versions.get(safe_key, 0))
                if current != int(last_version):
                    return current
                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    return current
                self._cond.wait(remaining)
