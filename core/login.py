#!/usr/bin/env python3
import asyncio

CHAMELEON_WORKSPACE_URL = "https://chamaleon-ai-0c02461b.base44.app/AgentWorkspace"

# The chat combobox shows a placeholder until a chat is picked. The workspace UI
# is localised, so the placeholder can be English ('Select chat...') or German
# ('Chat wählen...' / 'Chat auswählen...' — the ChatPhantom rebrand uses the
# latter). Treat any of these — or an empty button — as "no chat yet".
_CHAT_PLACEHOLDERS = ("select chat", "chat wählen", "wähle einen chat", "chat auswählen")


def chat_not_selected(combo_text: str) -> bool:
    """True when the combobox still shows a placeholder (no chat chosen yet)."""
    text = (combo_text or "").strip().lower()
    if not text:
        return True
    return any(placeholder in text for placeholder in _CHAT_PLACEHOLDERS)


async def login_mod_site(page, login_url: str, username: str, password: str, platform: str = ""):
    """Navigate to the mod site and log in if the session is not already active."""
    tag = f"[{platform}] " if platform else ""

    await page.goto(login_url, wait_until="domcontentloaded", timeout=30_000)
    await asyncio.sleep(1.5)

    username_el = await page.query_selector("input[name='username']")
    if username_el is None:
        print(f"{tag}Mod site: already logged in.", flush=True)
        return

    print(f"{tag}Mod site: logging in as '{username}'...", flush=True)
    await page.fill("input[name='username']", username)
    await page.fill("input[name='password']", password)
    await page.click("#login-btn")

    # Wait for the login form to disappear (indicates successful redirect)
    try:
        await page.wait_for_selector("input[name='username']", state="detached", timeout=10_000)
    except Exception:
        pass

    await asyncio.sleep(1)
    print(f"{tag}Mod site: done. URL: {page.url}", flush=True)


async def login_chameleon(page, email: str, password: str, platform: str = "", chat_name: str = "Gold XL WM"):
    """Full setup flow for Chameleon AI workspace.

    Steps:
      1. AgentWorkspace → shows 'Nicht eingeloggt' splash if not authenticated
      2. Click 'Zum Login' → navigates to /login page
      3. Fill email + password → click 'Sign in' → redirected to AgentWorkspace
      4. Select chat from the dropdown combobox (if not already selected)
      5. Click the 'Chat Extractor' tab so the bot is ready to work
    """
    tag = f"[{platform}] " if platform else ""

    await page.goto(CHAMELEON_WORKSPACE_URL, wait_until="domcontentloaded", timeout=30_000)
    await asyncio.sleep(2)

    # Step 1: 'Nicht eingeloggt' splash — click the redirect button.
    # Chameleon is a React SPA, so the route change does NOT fire a new
    # domcontentloaded event. Wait for the email input to appear instead.
    zum_login = page.locator("button", has_text="Zum Login")
    if await zum_login.count() > 0:
        print(f"{tag}Chameleon: clicking 'Zum Login'...", flush=True)
        await zum_login.first.click()
        try:
            await page.wait_for_selector("input#email", timeout=15_000)
        except Exception:
            pass
        await asyncio.sleep(1)

    # Step 2: email / password form
    email_el = await page.query_selector("input#email")
    if email_el is not None:
        print(f"{tag}Chameleon: logging in as '{email}'...", flush=True)
        await page.fill("input#email", email)
        await page.fill("input#password", password)
        await page.locator("button[type='submit']").click()

        try:
            await page.wait_for_selector("input#email", state="detached", timeout=15_000)
        except Exception:
            pass

        await asyncio.sleep(2)

        # Make sure we end up on the workspace page the bot needs
        if "AgentWorkspace" not in page.url:
            await page.goto(CHAMELEON_WORKSPACE_URL, wait_until="domcontentloaded", timeout=20_000)
            await asyncio.sleep(1)
    else:
        print(f"{tag}Chameleon: already logged in.", flush=True)

    # Step 3: Select the chat from the dropdown combobox if not already chosen.
    # The combobox shows 'Select chat...' until a chat is picked.
    combobox = page.locator("button[role='combobox']")
    if await combobox.count() > 0:
        combo_text = await combobox.inner_text()
        if chat_not_selected(combo_text):
            print(f"{tag}Chameleon: selecting chat '{chat_name}'...", flush=True)
            await combobox.click()
            await asyncio.sleep(0.5)
            # Radix UI renders options in a portal with role="option"
            option = page.locator(f"[role='option']:has-text('{chat_name}')")
            try:
                await option.wait_for(state="visible", timeout=10_000)
                await option.click()
            except Exception:
                # Fallback: pick the first available option
                fallback = page.locator("[role='option']")
                if await fallback.count() > 0:
                    await fallback.first.click()
            await asyncio.sleep(1.5)
        else:
            print(f"{tag}Chameleon: chat already selected ({combo_text.strip()}).", flush=True)

    # Step 4: Activate the 'Extractor' tab so the bot can immediately start.
    extractor_tab = page.locator("button[role='tab']:has-text('Extractor')")
    if await extractor_tab.count() > 0:
        if await extractor_tab.get_attribute("data-state") != "active":
            print(f"{tag}Chameleon: activating 'Chat Extractor' tab...", flush=True)
            await extractor_tab.click()
            await asyncio.sleep(0.5)

    print(f"{tag}Chameleon: ready. URL: {page.url}", flush=True)


async def force_extractor_tab(page, platform: str = "") -> bool:
    """Force the Chat Extractor tab active regardless of its current state.

    Returns True if the tab is active afterwards, False if not found.
    """
    tag = f"[{platform}] " if platform else ""
    extractor_tab = page.locator("button[role='tab']:has-text('Extractor')")
    if await extractor_tab.count() == 0:
        print(f"{tag}Chameleon: Chat Extractor tab not found on page.", flush=True)
        return False
    state = await extractor_tab.get_attribute("data-state")
    if state == "active":
        print(f"{tag}Chameleon: Chat Extractor tab already active.", flush=True)
        return True
    print(f"{tag}Chameleon: forcing Chat Extractor tab active...", flush=True)
    await extractor_tab.click()
    await asyncio.sleep(0.5)
    state = await extractor_tab.get_attribute("data-state")
    active = state == "active"
    print(f"{tag}Chameleon: Chat Extractor tab {'ACTIVE' if active else 'STILL INACTIVE — try again'}.", flush=True)
    return active


# Maps the stats API's own JSON field names (camelCase, as seen in DevTools'
# Network tab) to the display labels the rest of the codebase keys off of
# (e.g. checkinall.py does stats.get("Ins"), stats.get("ASA Outs")).
_STATS_KEY_MAP = {
    "ins": "Ins",
    "outs": "Outs",
    "openins": "Open Ins",
    "openouts": "Open Outs",
    "asains": "ASA Ins",
    "asaouts": "ASA Outs",
    "timeout": "Timeout",
}


def _find_stats_payload(obj):
    """Recursively search a parsed JSON response body for the stats object,
    identified purely by its shape: it must carry ins/outs/asaIns/asaOuts/
    openIns/openOuts keys (case-insensitive), matching what the Network tab
    shows for the stats call. Works regardless of nesting (e.g. {"data": {...}})."""
    if isinstance(obj, dict):
        lower_keys = {k.lower() for k in obj.keys()}
        if {"ins", "outs", "asains", "asaouts", "openins", "openouts"} <= lower_keys:
            return obj
        for v in obj.values():
            found = _find_stats_payload(v)
            if found is not None:
                return found
    elif isinstance(obj, list):
        for item in obj:
            found = _find_stats_payload(item)
            if found is not None:
                return found
    return None


def _normalize_stats(payload: dict) -> dict:
    stats = {}
    for k, v in payload.items():
        label = _STATS_KEY_MAP.get(k.lower())
        if label is None:
            continue
        try:
            stats[label] = int(v)
        except (TypeError, ValueError):
            stats[label] = v
    return stats


async def check_stats(page, platform: str = "") -> dict | None:
    """Read the 'Meine Statistiken' numbers straight from the site's own stats
    API response instead of scraping the dialog's DOM (the DOM no longer
    reliably exposes the values). Clicks the site's own stats button to
    trigger the request, watches network traffic for the JSON response
    matching the stats shape (ins/outs/asaIns/asaOuts/openIns/openOuts), and
    reads the numbers from there the moment it arrives.

    Returns a dict such as {'Ins': 1407, 'ASA Outs': 441, ...}, or None if the
    button couldn't be found or no matching response showed up in time.
    """
    tag = f"[{platform}] " if platform else ""

    stats_btn = page.locator("#mod-stats-btn")
    if await stats_btn.count() == 0:
        # Fall back to matching the button by its label text.
        stats_btn = page.locator("button", has_text="Meine Statistiken")
    if await stats_btn.count() == 0:
        print(f"{tag}Stats: 'Meine Statistiken' button not found "
              f"(is the mod site logged in?).", flush=True)
        return None

    # If the dialog was left open from a previous run, close it first so the
    # click below always fires a fresh request rather than a no-op.
    if await page.locator("div[role='dialog']").count() > 0:
        try:
            close_btn = page.locator("#close-mod-stats-btn")
            if await close_btn.count() > 0:
                await close_btn.first.click()
            else:
                await page.keyboard.press("Escape")
            await asyncio.sleep(0.3)
        except Exception:
            pass

    captured: dict | None = None

    async def _on_response(response):
        nonlocal captured
        if captured is not None:
            return
        if response.request.resource_type not in ("fetch", "xhr"):
            return
        if "json" not in (response.headers.get("content-type") or ""):
            return
        try:
            body = await response.json()
        except Exception:
            return
        payload = _find_stats_payload(body)
        if payload is not None:
            captured = payload

    page.on("response", _on_response)
    try:
        await stats_btn.first.click()
        waited = 0.0
        while captured is None and waited < 10.0:
            await asyncio.sleep(0.25)
            waited += 0.25
    finally:
        page.remove_listener("response", _on_response)

    # Close the dialog again so the page is left as we found it.
    try:
        close_btn = page.locator("#close-mod-stats-btn")
        if await close_btn.count() > 0:
            await close_btn.first.click()
        else:
            await page.keyboard.press("Escape")
    except Exception:
        pass

    if captured is None:
        print(f"{tag}Stats: clicked but no matching stats response was seen.", flush=True)
        return None

    return _normalize_stats(captured)


async def check_chameleon(page, platform: str = "") -> bool:
    """Check whether the Chameleon tab is in the correct ready state.

    Prints a per-condition report and returns True if everything is OK,
    False if login_chameleon should be re-run.
    """
    tag = f"[{platform}] " if platform else ""
    ok = True

    # 1. Right URL
    on_workspace = "AgentWorkspace" in page.url
    print(f"{tag}  URL              : {page.url}  {'OK' if on_workspace else 'WRONG — not on AgentWorkspace'}", flush=True)
    if not on_workspace:
        ok = False

    # 2. Logged in (no 'Zum Login' splash)
    zum_login = page.locator("button", has_text="Zum Login")
    logged_in = await zum_login.count() == 0
    print(f"{tag}  Logged in        : {'OK' if logged_in else 'NO — Zum Login button visible'}", flush=True)
    if not logged_in:
        ok = False

    # 3. Chat selected
    combobox = page.locator("button[role='combobox']")
    if await combobox.count() > 0:
        combo_text = (await combobox.inner_text()).strip()
        chat_ok = not chat_not_selected(combo_text)
        print(f"{tag}  Chat selected    : {'OK (' + combo_text + ')' if chat_ok else 'NO — still showing: ' + combo_text}", flush=True)
        if not chat_ok:
            ok = False
    else:
        print(f"{tag}  Chat selected    : UNKNOWN — combobox not found", flush=True)
        ok = False

    # 4. Chat Extractor tab active
    extractor_tab = page.locator("button[role='tab']:has-text('Extractor')")
    if await extractor_tab.count() > 0:
        state = await extractor_tab.get_attribute("data-state")
        tab_ok = state == "active"
        print(f"{tag}  Chat Extractor   : {'ACTIVE' if tab_ok else 'INACTIVE (data-state=' + str(state) + ')'}", flush=True)
        if not tab_ok:
            ok = False
    else:
        print(f"{tag}  Chat Extractor   : UNKNOWN — tab not found", flush=True)
        ok = False

    verdict = "OK — no redo needed" if ok else "NEEDS REDO — run login_chameleon again"
    print(f"{tag}  Result           : {verdict}", flush=True)
    return ok
