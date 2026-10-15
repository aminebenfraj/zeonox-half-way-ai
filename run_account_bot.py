#!/usr/bin/env python3
"""Run one additional dashboard-created bot account."""

from __future__ import annotations

import asyncio
import sys
from dataclasses import replace
from importlib import import_module
from pathlib import Path

from playwright.async_api import async_playwright

from core.bot_accounts import BASE_DIR, get_account
from core.launcher import is_cdp_ready, start_chrome, wait_for_cdp
from core.login import login_chameleon, login_mod_site
from core.platforms import PLATFORM_BY_SLUG


def _runtime_config(account: dict):
    module = import_module(f"configs.{account['platform_type']}")
    changes = {
        "platform": account["label"],
        "cdp_url": f"http://127.0.0.1:{account['cdp_port']}",
        "username": account["username"],
        "password": account["password"],
    }
    if hasattr(module.config, "profile"):
        changes["profile"] = f"accounts/{account['id']}"
    return module, replace(module.config, **changes)


async def _prepare_react_browser(config, account_id: str) -> None:
    port = int(config.cdp_url.rsplit(":", 1)[-1])
    profile = BASE_DIR / "profiles" / "accounts" / account_id
    if not is_cdp_ready(port):
        profile.mkdir(parents=True, exist_ok=True)
        start_chrome(str(profile), port)
        if not wait_for_cdp(port, timeout=30):
            raise RuntimeError(f"Chrome did not start on port {port}")

    async with async_playwright() as playwright:
        browser = await playwright.chromium.connect_over_cdp(config.cdp_url)
        context = browser.contexts[0]

        tab1 = next((page for page in context.pages if config.tab1_pattern in page.url), None)
        if tab1 is None:
            tab1 = await context.new_page()
        await login_mod_site(
            tab1, config.tab1_url, config.username, config.password, config.platform
        )

        tab2 = next((page for page in context.pages if config.tab2_pattern in page.url), None)
        if tab2 is None:
            tab2 = await context.new_page()
        await login_chameleon(
            tab2,
            config.chameleon_email,
            config.chameleon_password,
            config.platform,
            config.chameleon_chat,
        )


def main() -> None:
    if len(sys.argv) != 2:
        raise SystemExit("Usage: python run_account_bot.py <account-id>")
    account = get_account(sys.argv[1])
    if account is None:
        raise SystemExit(f"Unknown bot account: {sys.argv[1]}")

    module, config = _runtime_config(account)
    platform = PLATFORM_BY_SLUG[account["platform_type"]]
    bot_class = getattr(module, "bot_class", None)
    if bot_class is None:
        from core.bot import ChatBot

        bot_class = ChatBot
    if not platform.self_managed:
        asyncio.run(_prepare_react_browser(config, account["id"]))
    asyncio.run(bot_class(config).run())


if __name__ == "__main__":
    main()
