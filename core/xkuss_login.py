#!/usr/bin/env python3
"""
xkuss mod-site login + page-classification helpers.

The xkuss mod site is an old table-based PHP UI (xkuss.com / old_c.php), totally
different from the React chat platforms handled by core/login.py. These helpers
cover the xkuss-specific half of the flow:

  * log in (the nick / pass form)
  * click 'Home' in the navbar (old_c.php)
  * classify which page tab1 is currently showing so the bot can react

The chameleon (tab2) half is identical to every other platform, so it keeps
using login_chameleon() from core/login.py.
"""

import asyncio

# ── Page states returned by classify_page() ─────────────────────────────────────
PAGE_LOGIN   = "login"     # the nick / pass login form is showing
PAGE_CHAT    = "chat"      # a dialog is open — the reply box is present
PAGE_WAITING = "waiting"   # home / waiting room (the Dialog Scanner is running)
PAGE_UNKNOWN = "unknown"   # none of the above — recover by clicking Home


def _is_home_url(url: str, cfg) -> bool:
    """True when `url` looks like the xkuss home / waiting room (old_c.php)."""
    url = (url or "").split("?", 1)[0].lower()
    if cfg.home_url and cfg.home_url.split("?", 1)[0].lower() in url:
        return True
    return "old_c.php" in url


async def classify_page(page, cfg) -> str:
    """Decide which xkuss page tab1 is currently on.

    Order matters: login is checked first (it can appear over anything when the
    session dies), then the chat reply box, then the waiting room. The waiting
    room is detected by the Dialog Scanner element *or*, as a fallback, by the
    home URL — some home pages render without the scanner element but are still
    the room the bot should sit in (rather than treating them as UNKNOWN and
    hammering the Home button forever).
    """
    try:
        if await page.query_selector(cfg.sel_login_user) is not None:
            return PAGE_LOGIN
        if await page.query_selector(cfg.sel_textarea) is not None:
            return PAGE_CHAT
        if await page.query_selector(cfg.sel_scanner) is not None:
            return PAGE_WAITING
        if _is_home_url(page.url, cfg):
            return PAGE_WAITING
    except Exception:
        pass
    return PAGE_UNKNOWN


async def describe_page(page, cfg) -> tuple:
    """Return (state, url, title) for logging — the phase plus where we are.

    Lets the bot report *why* it thinks it is in a given phase so a stuck bot
    can be diagnosed from the logs alone.
    """
    state = await classify_page(page, cfg)
    url   = ""
    title = ""
    try:
        url = page.url
        title = await page.title()
    except Exception:
        pass
    return state, url, title


def _derive_home_url(current_url: str) -> str:
    """Build the old_c.php URL relative to wherever tab1 currently is."""
    base = current_url.split("?", 1)[0].rsplit("/", 1)[0]
    return f"{base}/old_c.php"


async def click_home(page, cfg, platform: str = ""):
    """Click 'Home' in the navbar; fall back to navigating there directly.

    This is the universal recovery action: whenever the bot ends up on an
    unexpected page, pressing Home returns it to the waiting room.
    """
    tag = f"[{platform}] " if platform else ""
    link = page.locator(cfg.sel_home_link)
    if await link.count() > 0:
        print(f"{tag}xkuss: clicking 'Home' in navbar...", flush=True)
        await link.first.click()
    else:
        target = cfg.home_url or _derive_home_url(page.url)
        print(f"{tag}xkuss: navbar not found — navigating to {target}", flush=True)
        await page.goto(target, wait_until="domcontentloaded", timeout=30_000)
    await asyncio.sleep(1.5)


async def login_xkuss(page, cfg, platform: str = ""):
    """Open the xkuss login page, log in if needed, then land on Home.

    The required flow is: login -> press Home in the navbar. After this returns,
    tab1 is on the waiting room (or has already been auto-redirected to a chat).
    """
    tag = f"[{platform}] " if platform else ""

    await page.goto(cfg.tab1_url, wait_until="domcontentloaded", timeout=30_000)
    await asyncio.sleep(1.5)

    if await page.query_selector(cfg.sel_login_user) is None:
        print(f"{tag}xkuss: already logged in.", flush=True)
    else:
        print(f"{tag}xkuss: logging in as '{cfg.username}'...", flush=True)
        await page.fill(cfg.sel_login_user, cfg.username)
        await page.fill(cfg.sel_login_pass, cfg.password)
        await page.click(cfg.sel_login_btn)
        try:
            await page.wait_for_selector(cfg.sel_login_user, state="detached", timeout=15_000)
        except Exception:
            pass
        await asyncio.sleep(1.5)
        print(f"{tag}xkuss: login submitted. URL: {page.url}", flush=True)

    # Always go Home after logging in, exactly as the manual flow does.
    await click_home(page, cfg, platform)
