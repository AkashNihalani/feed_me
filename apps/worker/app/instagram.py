from __future__ import annotations

import re

_SHORTCODE_ALPHABET = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789-_"


def shortcode_from_url(url: str) -> str:
    match = re.search(r"/(?:p|reel|tv)/([A-Za-z0-9_-]+)", (url or "").strip(), flags=re.IGNORECASE)
    return match.group(1).strip() if match else ""


def shortcode_from_media_id(value: str | int | None) -> str:
    raw = str(value or "").strip()
    if not raw:
        return ""

    # Bright Data may return the Instagram media id as "<media_id>_<user_id>".
    media_id = raw.split("_", 1)[0].strip()
    if not media_id.isdigit():
        return ""

    num = int(media_id)
    if num <= 0:
        return ""

    chars: list[str] = []
    while num > 0:
        num, rem = divmod(num, 64)
        chars.append(_SHORTCODE_ALPHABET[rem])
    return "".join(reversed(chars))


def canonical_post_url(shortcode: str, fallback_url: str = "") -> str:
    code = (shortcode or "").strip()
    if code:
        kind_match = re.search(r"/(p|reel|tv)/", (fallback_url or "").strip(), flags=re.IGNORECASE)
        kind = (kind_match.group(1).lower() if kind_match else "p")
        return f"https://www.instagram.com/{kind}/{code}"

    fallback = (fallback_url or "").strip()
    return fallback.split("?", 1)[0].split("#", 1)[0] if fallback else ""
