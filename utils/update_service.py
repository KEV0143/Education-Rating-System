import json
import os
import re
import threading
import urllib.parse
import urllib.request
from datetime import datetime
from typing import Dict, Optional, Tuple


def _normalize_version(value: str) -> Optional[Tuple[int, int, int]]:
    if not value:
        return None
    nums = [int(x) for x in re.findall(r"\d+", value)]
    if not nums:
        return None
    while len(nums) < 3:
        nums.append(0)
    return tuple(nums[:3])


def _is_newer_version(remote: str, local: str) -> bool:
    r = _normalize_version(remote)
    local_v = _normalize_version(local)
    if not r or not local_v:
        return False
    return r > local_v


class UpdateService:
    def __init__(self, app_version: str, repo: str, timeout: float, user_agent: str) -> None:
        self.app_version = app_version
        self.repo = repo
        self.timeout = timeout
        self.user_agent = user_agent
        self._lock = threading.Lock()
        self._remind_later_clicked = False
        self._info = self._empty_info()

    @staticmethod
    def _empty_info() -> Dict[str, object]:
        return {
            "available": False,
            "url": None,
            "source_url": None,
            "exe_url": None,
            "release_url": None,
            "checked_at": None,
            "remote_version": None,
            "notes": None,
        }

    def _fetch_json(self, url: str) -> Optional[dict]:
        headers = {
            "Accept": "application/vnd.github+json",
            "User-Agent": self.user_agent,
        }
        token = os.environ.get("GITHUB_TOKEN")
        if token:
            headers["Authorization"] = f"Bearer {token}"

        req = urllib.request.Request(url, headers=headers)
        with urllib.request.urlopen(req, timeout=self.timeout) as resp:
            return json.load(resp)

    def _fetch_latest_release_via_html(self) -> Optional[Dict[str, str]]:
        headers = {
            "Accept": "text/html,application/xhtml+xml",
            "User-Agent": self.user_agent,
        }
        req = urllib.request.Request(
            f"https://github.com/{self.repo}/releases/latest",
            headers=headers,
        )
        with urllib.request.urlopen(req, timeout=self.timeout) as resp:
            final_url = resp.geturl()

        match = re.search(r"/releases/tag/([^/?#]+)", final_url)
        if not match:
            return None

        tag = urllib.parse.unquote(match.group(1)).strip()
        if not tag:
            return None

        tag_for_url = urllib.parse.quote(tag, safe="")
        archive_url = f"https://github.com/{self.repo}/archive/refs/tags/{tag_for_url}.zip"

        return {
            "tag": tag,
            "release_url": final_url,
            "source_url": archive_url,
            "download_url": archive_url,
        }

    @staticmethod
    def _select_release_download_url(release: dict) -> Optional[str]:
        assets = release.get("assets") or []
        preferred_exts = (".zip", ".exe", ".msi", ".dmg", ".deb", ".rpm")

        for ext in preferred_exts:
            for asset in assets:
                name = str(asset.get("name") or "").lower()
                if name.endswith(ext):
                    url = asset.get("browser_download_url")
                    if url:
                        return url

        for asset in assets:
            url = asset.get("browser_download_url")
            if url:
                return url

        return release.get("zipball_url") or release.get("html_url")

    @staticmethod
    def _select_asset_download_url(release: dict, allowed_exts: Tuple[str, ...]) -> Optional[str]:
        assets = release.get("assets") or []
        for asset in assets:
            name = str(asset.get("name") or "").lower()
            if any(name.endswith(ext) for ext in allowed_exts):
                url = asset.get("browser_download_url")
                if url:
                    return url
        return None

    def _build_source_archive_url(self, tag: str) -> Optional[str]:
        if not tag:
            return None
        tag_for_url = urllib.parse.quote(tag, safe="")
        return f"https://github.com/{self.repo}/archive/refs/tags/{tag_for_url}.zip"

    def check_for_updates(self) -> None:
        info = self._empty_info()
        info["checked_at"] = datetime.utcnow()
        tag: Optional[str] = None
        download_url: Optional[str] = None
        source_url: Optional[str] = None
        exe_url: Optional[str] = None
        release_url: Optional[str] = None
        notes: Optional[str] = None

        try:
            release = self._fetch_json(
                f"https://api.github.com/repos/{self.repo}/releases/latest",
            )
            if isinstance(release, dict):
                tag = (release.get("tag_name") or "").strip() or None
                download_url = self._select_release_download_url(release)
                source_url = self._build_source_archive_url(tag or "")
                exe_url = self._select_asset_download_url(release, (".exe",))
                release_url = (release.get("html_url") or "").strip() or None
                notes = (release.get("body") or "").strip() or None
        except Exception:
            pass

        if not tag or not (download_url or release_url):
            try:
                fallback = self._fetch_latest_release_via_html()
            except Exception:
                fallback = None

            if fallback:
                tag = tag or fallback.get("tag")
                release_url = release_url or fallback.get("release_url")
                download_url = download_url or fallback.get("download_url")
                source_url = source_url or fallback.get("source_url")

        source_url = source_url or self._build_source_archive_url(tag or "")

        info["remote_version"] = tag
        info["source_url"] = source_url
        info["exe_url"] = exe_url
        info["release_url"] = release_url
        info["notes"] = notes
        info["checked_at"] = datetime.utcnow()

        if tag and _is_newer_version(tag, self.app_version):
            info["available"] = True
            info["url"] = source_url or exe_url or download_url or release_url

        with self._lock:
            self._info = info

    def mark_remind_later(self) -> None:
        with self._lock:
            self._remind_later_clicked = True

    def context(self) -> Dict[str, object]:
        with self._lock:
            data = dict(self._info)
            if self._remind_later_clicked:
                data["available"] = False
            return data
