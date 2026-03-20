from __future__ import annotations

import json
from typing import Any

from pywebpush import WebPushException, webpush

from .config import WEB_PUSH_SUBJECT, WEB_PUSH_VAPID_PRIVATE_KEY


def is_enabled() -> bool:
    return bool(WEB_PUSH_VAPID_PRIVATE_KEY and WEB_PUSH_SUBJECT)


def send(subscription: dict[str, Any], payload: dict[str, Any]) -> tuple[bool, str | None, int | None]:
    if not is_enabled():
        return False, "Web Push is not configured", None

    try:
        webpush(
            subscription_info=subscription,
            data=json.dumps(payload),
            vapid_private_key=WEB_PUSH_VAPID_PRIVATE_KEY,
            vapid_claims={"sub": WEB_PUSH_SUBJECT},
        )
        return True, None, None
    except WebPushException as exc:
        response = getattr(exc, "response", None)
        status_code = getattr(response, "status_code", None)
        text = ""
        if response is not None:
            try:
                text = (response.text or "")[:500]
            except Exception:
                text = ""
        message = text or str(exc)[:500] or "web push failed"
        return False, message, status_code
    except Exception as exc:
        return False, str(exc)[:500] or "web push failed", None
