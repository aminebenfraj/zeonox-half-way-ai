#!/usr/bin/env python3
"""
ExtJS mod-site login + navigation + page-classification helpers.

Shared by justlo AND linduu: both drive the SAME old ExtJS moderation console
(Play/Pause, the 'Unterhaltung' grid, the message box) but reach it through
DIFFERENT login flows. Everything here is config-driven so one module serves both.

Three surfaces, in order:

  1. login      — the logged-out entry.
       justlo: mod.justlo.de redirects to the justlo.de landing page, whose
               header 'Login' link opens a `#login` MODAL (cfg.sel_login_link).
       linduu: a dedicated login PAGE at cfg.login_url; no modal to open.
  2. community  — after 'Einloggen', the logged-in community home. A 'Mod' link
       leads to the console. justlo shows it in the navbar; linduu tucks it in a
       'Mehr' dropdown, so linduu reaches the console by URL (cfg.console_via_goto).
  3. mod console — the ExtJS moderation UI. Pressing 'Play' (cfg.sel_play_btn)
       starts the dialog scanner that feeds conversations into the workspace.

The chameleon (tab2) half is identical to every other platform, so it keeps
using login_chameleon() from core/login.py.
"""

import asyncio

# ── Page states returned by classify_page() ─────────────────────────────────────
PAGE_LOGIN     = "login"      # logged out — the landing page / login modal is showing
PAGE_COMMUNITY = "community"  # logged in but on justlo.de community, not the console yet
PAGE_CHAT      = "chat"       # a dialog is loaded in the console workspace
PAGE_WAITING   = "waiting"    # in the console, scanner running, no dialog yet
PAGE_UNKNOWN   = "unknown"    # none of the above — recover by re-running login_justlo()


# ── Low-level surface detectors ─────────────────────────────────────────────────
# Each returns True when tab1 is currently showing that surface. They are cheap
# query_selector checks so classify_page() can decide the phase without guessing
# from the URL alone (the console and the community home share the justlo host).

async def _in_console(page, cfg) -> bool:
    """True when tab1 is on the ExtJS moderation console (Play/Pause present)."""
    try:
        if await page.query_selector(cfg.sel_play_btn) is not None:
            return True
        if await page.query_selector(cfg.sel_pause_btn) is not None:
            return True
    except Exception:
        pass
    return False


async def _on_community(page, cfg) -> bool:
    """True when tab1 is on the logged-in community home (the 'Mod' link shows)."""
    try:
        return await page.query_selector(cfg.sel_mod_link) is not None
    except Exception:
        return False


async def _on_login(page, cfg) -> bool:
    """True when tab1 is logged out (the landing page / login form is reachable)."""
    try:
        # The `#login` form lives in the DOM of the landing page even while the
        # modal is hidden, so its presence (with no console around it) means we
        # are logged out and need to authenticate.
        return await page.query_selector(cfg.sel_login_user) is not None
    except Exception:
        return False


async def _chat_active(page, cfg) -> bool:
    """True when a real dialog is loaded (the client panel holds a member).

    The console always renders the client/fake panels, the reply box and an (often
    empty) message grid, so none of those is a signal on its own — a FASA
    first-contact has a client but ZERO message rows. When NO dialog is loaded the
    client panel is blank: its username link is empty and the age reads '(NaN)'. So
    a non-empty client username is what tells us a dialog is actually live.
    """
    try:
        name = await page.evaluate(
            """(sel) => {
                const a = document.querySelector(sel);
                return a ? (a.textContent || '').trim() : '';
            }""",
            cfg.sel_client_username,
        )
        return bool(name)
    except Exception:
        return False


async def classify_page(page, cfg) -> str:
    """Decide which justlo surface tab1 is currently on.

    Order matters: the console is checked first (Play/Pause are unique to it), so
    a loaded workspace is never mistaken for the community home. Only when we are
    NOT in the console do we look for the community 'Mod' link or the logged-out
    login form.
    """
    try:
        if await _in_console(page, cfg):
            return PAGE_CHAT if await _chat_active(page, cfg) else PAGE_WAITING
        if await _on_community(page, cfg):
            return PAGE_COMMUNITY
        if await _on_login(page, cfg):
            return PAGE_LOGIN
    except Exception:
        pass
    return PAGE_UNKNOWN


async def describe_page(page, cfg) -> tuple:
    """Return (state, url, title) for logging — the phase plus where we are."""
    state = await classify_page(page, cfg)
    url = ""
    title = ""
    try:
        url = page.url
        title = await page.title()
    except Exception:
        pass
    return state, url, title


# ── Individual navigation steps ─────────────────────────────────────────────────

async def _dismiss_popups(page):
    """Best-effort close of any promo / streak / profile pop-up (linduu).

    These overlays sit over the community home and can intercept clicks. Escape
    dismisses most of them; it's harmless when nothing is open. Never raises.
    """
    try:
        await page.keyboard.press("Escape")
        await asyncio.sleep(0.2)
        await page.keyboard.press("Escape")
    except Exception:
        pass


async def _do_login(page, cfg, tag: str):
    """Sign in with the nick / password form, whichever variant this site uses.

    linduu (cfg.login_url set) navigates to a dedicated login page. justlo
    (cfg.sel_login_link set) reveals a `#login` modal by clicking the header
    'Login' link first. Both then fill the user/pass fields and submit.
    """
    print(f"{tag}opening login + signing in as '{cfg.username}'...", flush=True)

    # linduu: go straight to the dedicated login page (the form is already there).
    login_url = getattr(cfg, "login_url", "") or ""
    if login_url:
        try:
            await page.goto(login_url, wait_until="domcontentloaded", timeout=30_000)
            await asyncio.sleep(1.0)
        except Exception:
            pass

    # justlo: reveal the modal via the 'Login' link (header/footer share it, so
    # the first match is enough). Skipped when sel_login_link is empty (linduu).
    if getattr(cfg, "sel_login_link", ""):
        link = page.locator(cfg.sel_login_link)
        try:
            if await link.count() > 0:
                await link.first.click()
        except Exception:
            pass

    await _dismiss_popups(page)

    # Wait for the username input to be actionable, then fill + submit.
    user = page.locator(cfg.sel_login_user)
    await user.first.wait_for(state="visible", timeout=15_000)
    await user.first.fill(cfg.username)
    await page.locator(cfg.sel_login_pass).first.fill(cfg.password)
    await page.locator(cfg.sel_login_btn).first.click()

    # After submit the site navigates to the community home; wait for that.
    try:
        await page.wait_for_load_state("domcontentloaded", timeout=20_000)
    except Exception:
        pass
    await asyncio.sleep(1.5)
    print(f"{tag}login submitted. URL: {page.url}", flush=True)


async def _open_console(page, cfg, tag: str):
    """Reach the ExtJS moderation console from the community home.

    linduu (cfg.console_via_goto) hides 'Mod' inside a 'Mehr' dropdown and stacks
    pop-ups over the page, so it navigates straight to the console URL. justlo
    clicks the navbar 'Mod' link, falling back to the URL if it's not found.
    """
    await _dismiss_popups(page)
    if getattr(cfg, "console_via_goto", False):
        print(f"{tag}opening the moderation console at {cfg.mod_url}...", flush=True)
        await page.goto(cfg.mod_url, wait_until="domcontentloaded", timeout=30_000)
    else:
        print(f"{tag}clicking 'Mod' to open the moderation console...", flush=True)
        link = page.locator(cfg.sel_mod_link)
        if await link.count() > 0:
            await link.first.click()
        else:
            print(f"{tag}'Mod' link not found — navigating to {cfg.mod_url}", flush=True)
            await page.goto(cfg.mod_url, wait_until="domcontentloaded", timeout=30_000)
    try:
        await page.wait_for_load_state("domcontentloaded", timeout=20_000)
    except Exception:
        pass
    await asyncio.sleep(1.5)


async def press_play(page, cfg, tag: str = ""):
    """Press 'Play' so the console's dialog scanner starts feeding conversations.

    Clicking an already-running (disabled) Play button is harmless — ExtJS
    ignores the click — so this is safe to call defensively after reaching the
    console. It's best-effort: a missing button is logged, not raised.
    """
    try:
        play = page.locator(cfg.sel_play_btn)
        if await play.count() > 0:
            print(f"{tag}pressing 'Play' to start the dialog scanner...", flush=True)
            await play.first.click()
            await asyncio.sleep(1.0)
        else:
            print(f"{tag}'Play' button not found (already running?).", flush=True)
    except Exception as e:
        print(f"{tag}could not press Play: {e}", flush=True)


# ── Public entry points ─────────────────────────────────────────────────────────

async def login_justlo(page, cfg, platform: str = ""):
    """Bring tab1 to the moderation console with the scanner running.

    Idempotent: it inspects the current surface and runs only the steps still
    needed, so it doubles as the universal recovery action — call it from any
    state (logged out, community home, stray page) and it climbs back up to the
    console and presses Play.
    """
    tag = f"[{platform}] " if platform else ""

    # If we are nowhere useful, start from the top by loading the entry URL
    # (the dedicated login page when set, else the mod landing URL).
    state = await classify_page(page, cfg)
    if state in (PAGE_UNKNOWN,):
        entry = getattr(cfg, "login_url", "") or cfg.tab1_url
        print(f"{tag}on a stray page — navigating to {entry}", flush=True)
        await page.goto(entry, wait_until="domcontentloaded", timeout=30_000)
        await asyncio.sleep(1.5)
        state = await classify_page(page, cfg)

    # 1. Logged out -> authenticate, which lands us on the community home.
    if state == PAGE_LOGIN:
        await _do_login(page, cfg, tag)
        state = await classify_page(page, cfg)

    # 2. Community home -> open the console via the 'Mod' link.
    if state == PAGE_COMMUNITY:
        await _open_console(page, cfg, tag)
        state = await classify_page(page, cfg)

    # 3. In the console (WAITING/CHAT) -> make sure the scanner is running.
    if await _in_console(page, cfg):
        await press_play(page, cfg, tag)
        print(f"{tag}console ready. URL: {page.url}", flush=True)
    else:
        # Last resort: navigate straight to the console URL and press Play.
        print(f"{tag}console not reached via the UI — navigating to {cfg.mod_url}", flush=True)
        await page.goto(cfg.mod_url, wait_until="domcontentloaded", timeout=30_000)
        await asyncio.sleep(1.5)
        await press_play(page, cfg, tag)


async def go_console(page, cfg, platform: str = ""):
    """Recovery helper used by the loop: get back to a running console.

    Thin wrapper around login_justlo() kept as a separate name so the bot's
    intent reads clearly ('return to the console') at the call sites where the
    session has NOT dropped — login_justlo() still handles a dropped session
    transparently if it turns out we were logged out.
    """
    await login_justlo(page, cfg, platform)
