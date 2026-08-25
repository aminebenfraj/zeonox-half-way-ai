#!/usr/bin/env python3
"""
Local Chameleon adapter for XkussBot / JustloBot.

Normally a bot's tab2 is the real Chameleon-AI site: it logs in, selects a
chat, pastes the captured HTML, clicks 'Antwort generieren', and reads the
reply back (see core/xkuss_bot.py / core/justlo_bot.py, and login_chameleon in
core/login.py). "Automatic Mode" — toggled on the dashboard's /chameleon page
(approval_server.py's _chameleon_source, GET/POST /api/chameleon/source) — is
an alternative: tab2 instead points at THIS project's own /chameleon page,
which does the same paste -> extract -> generate steps but answers with a
direct Groq call (approval_server.py's /api/chameleon/reply) rather than the
real Chameleon-AI backend.

Deliberately much thinner than the real-chameleon flow: our own page has no
login and no "broken workspace" state to detect/fix, and every cycle re-pastes
+ re-extracts from scratch, so there's nothing worth a dedicated multi-attempt
recovery loop for. A failure here is just raised and left to the bot's normal
per-cycle error handling (retry the whole cycle after a short sleep, restart
after too many in a row) — see _restart_chameleon_job()'s local-mode branch in
xkuss_bot.py / justlo_bot.py, which is a no-op for the same reason.

The mode is read once per bot process, at the start of run() — flipping the
dashboard toggle live takes effect the next time that bot restarts (via the
existing Bot Controls "Restart" button), not mid-run, so a running cycle never
has tab2's URL pattern swapped out from under it.
"""

import asyncio

from playwright.async_api import Error as PlaywrightError

from core.approval import APPROVAL_SERVER_URL, get_chameleon_source

LOCAL_TAB_PATTERN = "/chameleon"  # substring matched against the local tab's URL
LOCAL_CHAMELEON_URL = f"{APPROVAL_SERVER_URL}/chameleon"

# platform key -> the /chameleon page's platform-tab button id (see approval_server.py's _CHAMELEON_PAGE)
_SEL_PLATFORM_TAB = {
    "xkuss": "#tab-xkuss",
    "justlo_lindu": "#tab-justlo",
}

_GET_REPLY_JS = "() => { const el = document.querySelector('#replyDe'); return el ? el.textContent.trim() : ''; }"
_GET_ERROR_JS = """
() => {
    const el = document.querySelector('#genError');
    if (!el || el.style.display === 'none') return '';
    return (el.textContent || '').trim();
}
"""
_GET_TYPE_JS = "() => { const el = document.querySelector('#typeBadge'); return el ? el.textContent.trim() : ''; }"
_GET_LAST_MSG_JS = "() => { const el = document.querySelector('#lastMsgDe'); return el ? el.textContent.trim() : ''; }"
# `extracted` is the /chameleon page's own top-level `let extracted = ...`
# (set by its doExtract()) — script-scope `let`/`const` bindings are visible
# to page.evaluate() the same way they're visible to the DevTools console, so
# this reads the client/fake profile dicts straight from it instead of
# re-parsing the rendered #clientProfile/#fakeProfile DOM.
_GET_PROFILES_JS = """
() => {
    try {
        return {
            client: (extracted && extracted.clientProfile) || {},
            fake: (extracted && extracted.fakeProfile) || {},
        };
    } catch (e) {
        return { client: {}, fake: {} };
    }
}
"""


async def is_local_mode(platform_key: str) -> bool:
    """Best-effort read of this platform's own Automatic Mode toggle."""
    return (await get_chameleon_source(platform_key)) == "local"


async def setup_local_tab(tab2, platform_key: str):
    """Point tab2 at our own /chameleon page and select the right platform
    tab. No login step — the page has none.

    Always reloads, even if tab2's URL already matches — this browser tab is
    typically reused across bot restarts (same CDP-connected Chrome, only the
    Python process restarts), so a URL match alone doesn't mean the DOM is
    current: it could still be whatever /chameleon last rendered before the
    most recent approval_server.py deploy. Reloading here (once, at startup)
    is what makes a server-side change to the page actually take effect the
    next time a bot restarts, instead of silently running stale JS."""
    if LOCAL_TAB_PATTERN not in tab2.url:
        await tab2.goto(LOCAL_CHAMELEON_URL, wait_until="domcontentloaded")
    else:
        await tab2.reload(wait_until="domcontentloaded")
    tab_btn = tab2.locator(_SEL_PLATFORM_TAB[platform_key])
    await tab_btn.wait_for(state="visible", timeout=15_000)
    await tab_btn.click()


async def paste_and_extract(tab2, html: str, platform_key: str, timeout_s: int = 15) -> bool:
    """Paste HTML and click 'Daten extrahieren'. Returns True once extraction
    produced a Generate card, False if it never showed up (empty/unparseable
    HTML) — callers should treat that like any other cycle failure."""
    await tab2.locator(_SEL_PLATFORM_TAB[platform_key]).click()
    await tab2.locator("#htmlInput").fill(html)
    await tab2.locator("button:has-text('Daten extrahieren')").click()
    try:
        await tab2.locator("#generateCard").wait_for(state="visible", timeout=timeout_s * 1_000)
        return True
    except PlaywrightError:
        return False


async def is_first_contact(tab2) -> bool:
    return (await tab2.evaluate(_GET_TYPE_JS)) == "FC"


async def get_conversation_data(tab2) -> dict:
    """The literal last message in the just-extracted conversation (either
    side) plus the client-vs-fake-account profile comparison table, for the
    approval dashboard's "Last Message" / "Client data" / "Fake account data"
    fields. Local-mode-only fast path: tab2 already has the conversation
    extracted (it's mid-way through generating a reply from it), so this just
    reads the result back with no extra navigation. See
    extract_conversation_data() for the real-mode equivalent. Returns
    {"last_message": str, "client_profile": dict, "fake_profile": dict}."""
    last_message = await tab2.evaluate(_GET_LAST_MSG_JS)
    profiles = await tab2.evaluate(_GET_PROFILES_JS)
    return {
        "last_message": last_message,
        "client_profile": profiles.get("client") or {},
        "fake_profile": profiles.get("fake") or {},
    }


async def extract_conversation_data(context, html: str, platform_key: str, timeout_s: int = 10) -> dict:
    """Real-Chameleon-mode equivalent of get_conversation_data(): tab2 there
    is busy with the actual Chameleon-AI site, so this pastes `html` into a
    throwaway visit to our own /chameleon page purely to reuse its
    already-precise, sender-aware conversation parsing (see
    XkussExtractor/JustloExtractor in approval_server.py) for the dashboard's
    "Last Message" / "Client data" / "Fake account data" fields — real mode
    never otherwise touches this page. Costs one extra lightweight page load
    per reply (this project's own page, no login, sub-second), and is
    best-effort: any failure just leaves those dashboard fields blank rather
    than affecting the actual reply. Returns the same shape as
    get_conversation_data()."""
    page = await context.new_page()
    try:
        await page.goto(LOCAL_CHAMELEON_URL, wait_until="domcontentloaded")
        await page.locator(_SEL_PLATFORM_TAB[platform_key]).click()
        await page.locator("#htmlInput").fill(html)
        await page.locator("button:has-text('Daten extrahieren')").click()
        await page.locator("#extractedCard").wait_for(state="visible", timeout=timeout_s * 1_000)
        last_message = await page.evaluate(_GET_LAST_MSG_JS)
        profiles = await page.evaluate(_GET_PROFILES_JS)
        return {
            "last_message": last_message,
            "client_profile": profiles.get("client") or {},
            "fake_profile": profiles.get("fake") or {},
        }
    except PlaywrightError:
        return {"last_message": "", "client_profile": {}, "fake_profile": {}}
    finally:
        await page.close()


async def generate_reply(tab2, additional_instructions: str, timeout_s: int) -> str:
    """One generation attempt: click 'Antwort generieren' and poll for the
    reply text to change from whatever was there before (mirrors the real
    _generate_reply()'s old/new diff so a stale reply is never mistaken for a
    fresh one). Raises on error/timeout instead of retrying internally — see
    this module's docstring for why.

    On success, also clicks the page's own 'Kopieren' button — same as a
    human would — so the reply is sitting on the OS clipboard, ready for
    paste_via_clipboard() to paste into the chat. Best-effort: a failed click
    here doesn't fail generation, since paste_via_clipboard() refreshes the
    clipboard itself right before pasting anyway."""
    old_reply = await tab2.evaluate(_GET_REPLY_JS)
    if additional_instructions:
        try:
            await tab2.locator("#instructionsInput").fill(additional_instructions)
        except PlaywrightError:
            pass
    await tab2.locator("#genBtn").click()

    deadline = asyncio.get_event_loop().time() + timeout_s
    while asyncio.get_event_loop().time() < deadline:
        reply = await tab2.evaluate(_GET_REPLY_JS)
        if reply and reply != old_reply:
            try:
                await tab2.locator("button:has-text('Kopieren')").click()
            except PlaywrightError:
                pass
            return reply
        err = await tab2.evaluate(_GET_ERROR_JS)
        if err:
            raise RuntimeError(f"Local Chameleon page returned an error: {err}")
        await asyncio.sleep(1)
    raise RuntimeError(f"Timed out ({timeout_s}s) waiting for a local AI reply.")


async def paste_via_clipboard(tab1, textarea, text: str) -> bool:
    """Best-effort real paste (Ctrl+V): writes `text` to the OS clipboard from
    tab1's origin, then pastes it into `textarea` — mirrors how a human
    actually moves the reply from our page into the chat, instead of setting
    the textarea's value directly. Refreshes the clipboard here (rather than
    relying solely on generate_reply()'s earlier 'Kopieren' click) so a human
    edit made on the approval dashboard after generation is still what gets
    pasted. Returns False (never raises) on any clipboard/permission hiccup so
    the caller can fall back to setting the value directly — a live chat reply
    must never be left stuck over a clipboard quirk."""
    try:
        await tab1.evaluate("(t) => navigator.clipboard.writeText(t)", text)
        await textarea.press("Control+V")
        await asyncio.sleep(0.3)
        value = await textarea.input_value()
        return value.strip() == text.strip()
    except PlaywrightError:
        return False
