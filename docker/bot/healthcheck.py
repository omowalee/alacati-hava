"""Container healthcheck for the Telegram bot: can we reach the Bot API with our token?

Uses getMe (free, no side effects, no message sent). Standard library only.
Exit 0 = healthy, 1 = unhealthy. The token is never printed.
"""

from __future__ import annotations

import json
import os
import sys
import urllib.error
import urllib.request

TIMEOUT_S = 10


def main() -> int:
    token = os.environ.get("TELEGRAM_BOT_TOKEN", "")
    if not token:
        print("TELEGRAM_BOT_TOKEN is not set", file=sys.stderr)
        return 1
    url = f"https://api.telegram.org/bot{token}/getMe"
    try:
        with urllib.request.urlopen(url, timeout=TIMEOUT_S) as resp:  # noqa: S310 - fixed https URL
            ok = bool(json.load(resp).get("ok"))
    except (urllib.error.URLError, TimeoutError, ValueError) as exc:
        # str(exc) can contain the URL for some errors; report only the type.
        print(f"Telegram API unreachable: {type(exc).__name__}", file=sys.stderr)
        return 1
    return 0 if ok else 1


if __name__ == "__main__":
    sys.exit(main())
