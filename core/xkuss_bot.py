#!/usr/bin/env python3
"""
xkuss chat automation core.

xkuss is an old table-based PHP mod site (xkuss.com / old_c.php) whose DOM has
nothing in common with the React platforms driven by core/bot.py, so it gets its
own bot. The chameleon (tab2) half is identical to every other platform, so this
module reuses the shared chameleon login + the HTML serializer / reply-extractor
JavaScript from core/bot.py.

Flow (matches the manual process):
  1. log in to xkuss, then press 'Home' in the navbar
  2. wait in the waiting room until the page auto-jumps to a dialog (chat)
  3. on a chat: capture HTML -> paste into chameleon -> Generate Reply ->
     paste the reply into #inhalt -> send via #sendbutton1
  4. land on an unexpected page at any point -> click 'Home' to recover

Exit codes (used by launcher to decide restart):
  0 = setup failure (Chrome unreachable, tabs cannot be opened) — do NOT restart
  1 = runtime crash (browser dropped, repeated errors)          — DO restart
"""

import asyncio
import random
import sys
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path

from playwright.async_api import (
    async_playwright,
    TimeoutError as PlaywrightTimeout,
    Error as PlaywrightError,
)

# Reuse the shared chameleon-side pieces — they are identical for every platform.
from core.bot import (
    _HTML_SERIALIZER_JS,
    _GET_DE_REPLY_JS,
    _GET_MANUAL_REVIEW_JS,
    _GET_SERVER_ERROR_JS,
    _IS_FIRST_CONTACT_JS,
    _is_fatal,
    _is_context_destroyed,
    ManualReviewLimitExceeded,
    log_sent_message,
    contains_banned_language,
)
from core.login import login_chameleon, chat_not_selected
from core.launcher import is_cdp_ready, start_chrome, wait_for_cdp
from core.approval import request_approval, mark_sent, mark_failed, report_status, ApprovalCancelled
from core.xkuss_login import (
    login_xkuss,
    click_home,
    describe_page,
    PAGE_LOGIN,
    PAGE_CHAT,
    PAGE_WAITING,
    PAGE_UNKNOWN,
)

POLL_INTERVAL    = 3    # seconds between idle checks
UNKNOWN_REHOME_INTERVAL = 30  # seconds between re-clicking Home while stuck on a stray page
WAITING_RELOAD_INTERVAL = 90  # seconds in the waiting room before reloading to restart the scanner
EXTRACT_TIMEOUT  = 30   # seconds to wait for the Generate Reply button
GENERATE_TIMEOUT = 90   # seconds to wait for the AI to finish
MAX_ERRORS       = 8    # consecutive errors before forced restart (exit 1)
TAB_RETRY_DELAY  = 5    # seconds between tab-search retries
TAB_MAX_RETRIES  = 6    # max tab-search retries before giving up

# Chameleon (tab2) selectors — constant across all platforms.
_SEL_EXTRACTOR_TAB = "button[role='tab']:has-text('Extractor')"
_SEL_HTML_TEXTAREA = "textarea[placeholder*='HTML-Quellcode']"
_SEL_EXTRACT_BTN   = "button:has-text('Daten extrahieren')"
_SEL_GEN_BTN       = "button:has-text('Antwort generieren')"
_SEL_INSTRUCTIONS  = "textarea[placeholder*='Etwas flirtender']"
_SEL_COMBOBOX      = "button[role='combobox']"

# xkuss puts the open dialog inside #showpm; capture only that so the serialized
# HTML never includes the inbox / other chat threads on the page.
_XKUSS_CHAT_ROOT = "#showpm"

# Inject the HTML straight into the React-controlled textarea in ONE shot:
# use the native value setter + a single 'input' event so React picks it up
# without Playwright's focus/clear/type/verify sequence. Fewer synthetic events
# than fill() means less chance of the base44 SPA choking on a huge payload.
_PASTE_HTML_JS = """(payload) => {
  const ta = document.querySelector("textarea[placeholder*='HTML-Quellcode']");
  if (!ta) return false;
  const setter = Object.getOwnPropertyDescriptor(
    window.HTMLTextAreaElement.prototype, 'value').set;
  setter.call(ta, payload);
  ta.dispatchEvent(new Event('input', { bubbles: true }));
  return true;
}"""


# ── Config ─────────────────────────────────────────────────────────────────────

@dataclass
class XkussConfig:
    platform: str = "Xkuss"
    cdp_url:  str = "http://127.0.0.1:9227"
    tab1_pattern: str = "xkuss"           # substring matched against the mod-site tab URL
    tab2_pattern: str = "chamaleon-ai"    # substring matched against the chameleon tab URL

    # ── xkuss mod site ──────────────────────────────────────────────────────
    tab1_url: str = ""                    # login page URL
    home_url: str = ""                    # explicit Home URL (blank = use navbar / derive)
    username: str = ""                    # <input name='nick'>
    password: str = ""                    # <input name='pass'>

    # ── chameleon AI (shared) ───────────────────────────────────────────────
    chameleon_email:    str = ""
    chameleon_password: str = ""
    chameleon_chat:     str = "Gold XL WM"
    additional_instructions: str = ""

    # ── xkuss-specific selectors ────────────────────────────────────────────
    sel_login_user: str = "input[name='nick']"
    sel_login_pass: str = "input[name='pass']"
    sel_login_btn:  str = "input[type='submit'][value='OK']"
    sel_home_link:  str = "a[href='old_c.php']"
    sel_scanner:    str = "#diacheck"          # present on the home / waiting room page
    sel_textarea:   str = "textarea#inhalt"    # the reply box on a chat page
    sel_send_btn:   str = "#sendbutton1"       # the send button on a chat page


# ── Bot ────────────────────────────────────────────────────────────────────────

_PHASE_LABELS = {
    PAGE_LOGIN:   "LOGIN (session dropped)",
    PAGE_CHAT:    "IN A CONVERSATION",
    PAGE_WAITING: "WAITING ROOM",
    PAGE_UNKNOWN: "UNKNOWN / stray page",
}


class XkussBot:
    def __init__(self, config: XkussConfig):
        self.cfg = config
        self._last_phase = None   # last logged phase, so we only log on change

    def log(self, msg: str):
        ts = datetime.now().strftime("%H:%M:%S")
        print(f"[{ts}][{self.cfg.platform}] {msg}", flush=True)

    async def _detect_phase(self, tab1, *, force_log: bool = False) -> str:
        """Classify tab1's phase and log it whenever it changes.

        This is the single detector the whole workflow relies on: it decides
        whether we are in a conversation, sitting in the waiting room, logged
        out, or on a stray page — and prints the transition (with URL/title) so
        a stuck bot is obvious from the logs.
        """
        state, url, title = await describe_page(tab1, self.cfg)
        if force_log or state != self._last_phase:
            label = _PHASE_LABELS.get(state, state)
            self.log(f"[PHASE] {label}  |  {title[:60]!r}  {url[:80]}")
            self._last_phase = state
        return state

    # ── Chrome launch ────────────────────────────────────────────────────────

    def _ensure_chrome(self):
        """Make sure a Chrome with the right debugging port is up.

        The bot connects to an existing Chrome over CDP; it does not own one. If
        nothing is listening on the port we launch a visible Chrome ourselves
        (using the xkuss profile) so the user can watch the browser and finish
        any manual login that may be required.
        """
        port = int(self.cfg.cdp_url.rsplit(":", 1)[-1])
        if is_cdp_ready(port):
            self.log(f"Chrome already running on port {port}.")
            return

        base_dir = Path(__file__).resolve().parent.parent
        profile  = str(base_dir / "profiles" / "xkuss")
        self.log(f"No Chrome on port {port} — launching a visible one (profile: {profile})...")
        try:
            start_chrome(profile, port)
        except FileNotFoundError as e:
            self.log(f"[ERROR] {e}")
            sys.exit(0)

        if wait_for_cdp(port, timeout=30):
            self.log(f"Chrome ready on port {port}.")
        else:
            self.log(f"[ERROR] Chrome did not come up on port {port} after 30s.")
            sys.exit(0)

    # ── Page / tab helpers ──────────────────────────────────────────────────

    async def _resolve_tabs(self, context, retries: int = TAB_MAX_RETRIES):
        """Re-query both tabs from the live context, matched by URL substring."""
        for attempt in range(retries):
            pages = context.pages
            self.log(f"Tab search (attempt {attempt + 1}/{retries}) — {len(pages)} page(s) open.")
            tab1 = next((p for p in pages if self.cfg.tab1_pattern in p.url), None)
            tab2 = next((p for p in pages if self.cfg.tab2_pattern in p.url), None)
            if tab1 and tab2:
                self.log(f"  Tab1 OK -> {tab1.url[:80]}")
                self.log(f"  Tab2 OK -> {tab2.url[:80]}")
                return tab1, tab2

            missing = []
            if not tab1:
                missing.append(f"tab1 (pattern: '{self.cfg.tab1_pattern}')")
            if not tab2:
                missing.append(f"tab2 (pattern: '{self.cfg.tab2_pattern}')")
            self.log(f"[WARN] Not found: {', '.join(missing)}")
            for p in pages:
                self.log(f"  open tab -> {p.url[:100]}")

            if attempt < retries - 1:
                self.log(f"[WARN] Retrying in {TAB_RETRY_DELAY}s...")
                await asyncio.sleep(TAB_RETRY_DELAY)

        self.log(f"[ERROR] Could not find required tabs after {retries} attempts.")
        return None, None

    async def _find_or_open_tab(self, context, url_pattern: str):
        for page in context.pages:
            if url_pattern in page.url:
                return page
        return await context.new_page()

    async def _wait_for_page_ready(self, page, state: str = "domcontentloaded", timeout: int = 15_000):
        try:
            await page.wait_for_load_state(state, timeout=timeout)
        except Exception:
            pass

    async def _safe_evaluate(self, page, script: str, arg=None, retries: int = 3) -> str:
        """Evaluate JS, retrying when the execution context is destroyed mid-call."""
        for attempt in range(retries):
            try:
                return await page.evaluate(script, arg)
            except PlaywrightError as e:
                if _is_context_destroyed(e) and attempt < retries - 1:
                    self.log(
                        f"[WARN] Execution context destroyed — waiting for page to "
                        f"stabilize (attempt {attempt + 1}/{retries - 1})"
                    )
                    await self._wait_for_page_ready(page, "domcontentloaded", 10_000)
                    await asyncio.sleep(2)
                else:
                    raise
        return ""

    # ── Chameleon (tab2) helpers ─────────────────────────────────────────────

    async def _is_chameleon_broken(self, tab2) -> bool:
        try:
            if "AgentWorkspace" not in tab2.url:
                return True
            zum_login = tab2.locator("button", has_text="Zum Login")
            return await zum_login.count() > 0
        except PlaywrightError as e:
            if _is_fatal(e):
                raise
            return False

    async def _fix_chameleon_only(self, tab2):
        self.log("[RECOVERY] Chameleon tab not on workspace — running fix...")
        await login_chameleon(
            tab2, self.cfg.chameleon_email, self.cfg.chameleon_password,
            self.cfg.platform, self.cfg.chameleon_chat,
        )
        self.log("[RECOVERY] Chameleon fix done.")

    async def _is_chat_selected(self, tab2) -> bool:
        try:
            combobox = tab2.locator(_SEL_COMBOBOX)
            if await combobox.count() == 0:
                return True
            combo_text = (await combobox.first.inner_text()).strip()
            return not chat_not_selected(combo_text)
        except PlaywrightError as e:
            if _is_fatal(e):
                raise
            return True

    async def _ensure_chat_selected(self, tab2) -> bool:
        if await self._is_chat_selected(tab2):
            return True
        chat = self.cfg.chameleon_chat
        self.log(f"[RECOVERY] Stuck on select screen — picking chat '{chat}'...")
        try:
            combobox = tab2.locator(_SEL_COMBOBOX)
            await combobox.first.click()
            await asyncio.sleep(0.5)
            option = tab2.locator(f"[role='option']:has-text('{chat}')")
            try:
                await option.wait_for(state="visible", timeout=10_000)
                await option.first.click()
            except Exception:
                fallback = tab2.locator("[role='option']")
                if await fallback.count() > 0:
                    self.log(f"[RECOVERY] '{chat}' not found — selecting first available chat.")
                    await fallback.first.click()
            await asyncio.sleep(1.5)
        except PlaywrightError as e:
            if _is_fatal(e):
                raise
            self.log(f"[WARN] Could not select chat: {e}")
            return False
        selected = await self._is_chat_selected(tab2)
        self.log(f"[RECOVERY] Chat selection {'OK' if selected else 'STILL STUCK'}.")
        return selected

    async def _ensure_extractor_tab_active(self, tab2):
        try:
            btn = tab2.locator(_SEL_EXTRACTOR_TAB)
            if await btn.count() > 0:
                if await btn.get_attribute("data-state") != "active":
                    await btn.click()
                    await asyncio.sleep(0.5)
        except PlaywrightError as e:
            if _is_fatal(e):
                raise
            self.log(f"[WARN] Could not activate extractor tab: {e}")

    async def _paste_and_extract(self, tab2, html: str):
        self.log(f"Switching to extractor tab and pasting {len(html):,} chars of HTML...")
        await self._wait_for_page_ready(tab2, "domcontentloaded")
        await self._ensure_extractor_tab_active(tab2)

        textarea = tab2.locator(_SEL_HTML_TEXTAREA)
        self.log("Waiting for HTML textarea to be visible...")
        await textarea.wait_for(state="visible", timeout=15_000)
        self.log("Textarea ready — injecting HTML...")
        await textarea.click()
        # Single-shot React-native injection instead of fill(): the html payload
        # is passed as an evaluate arg (never typed, never logged) and produces
        # exactly one 'input' event for the SPA to handle.
        ok = await tab2.evaluate(_PASTE_HTML_JS, html)
        if not ok:
            self.log("[WARN] HTML textarea vanished before inject — falling back to fill().")
            await textarea.fill(html)
        await asyncio.sleep(0.3)
        self.log("Clicking 'Daten extrahieren'...")
        await tab2.locator(_SEL_EXTRACT_BTN).click()
        self.log("Extraction triggered — waiting for 'Generate Reply' button...")

    async def _is_first_contact(self, tab2) -> bool:
        try:
            return bool(await self._safe_evaluate(tab2, _IS_FIRST_CONTACT_JS))
        except PlaywrightError as e:
            if _is_fatal(e):
                raise
            self.log(f"[WARN] Could not check for First Contact: {e}")
            return False

    async def _generate_reply(self, tab2) -> str:
        old_reply = await self._safe_evaluate(tab2, _GET_DE_REPLY_JS)
        gen_btn   = tab2.locator(_SEL_GEN_BTN)
        self.log(f"Waiting up to {EXTRACT_TIMEOUT}s for 'Generate Reply' button...")
        await gen_btn.wait_for(state="visible", timeout=EXTRACT_TIMEOUT * 1_000)
        self.log("'Generate Reply' button visible — clicking...")
        if self.cfg.additional_instructions:
            try:
                instr = tab2.locator(_SEL_INSTRUCTIONS)
                if await instr.count() > 0:
                    await instr.fill(self.cfg.additional_instructions)
                    self.log(f"Additional instructions set: {self.cfg.additional_instructions}")
            except PlaywrightError as e:
                if _is_fatal(e):
                    raise
                self.log(f"[WARN] Could not fill additional instructions: {e}")

        # Chameleon can refuse to produce a reply two different ways:
        #  - it flags the request as needing a human ("... muss MANUELL
        #    beantwortet werden", e.g. an outing/doxxing attempt)
        #  - its backend just fails outright ("Fehler: SERVER_ERROR" /
        #    "Interner Fehler. Bitte erneut versuchen.")
        # Click 'Antwort generieren' again — up to 3 attempts total — before
        # giving up; the caller then refreshes the chat.
        max_attempts = 3
        for attempt in range(1, max_attempts + 1):
            if attempt > 1:
                await gen_btn.wait_for(state="visible", timeout=EXTRACT_TIMEOUT * 1_000)
            await gen_btn.scroll_into_view_if_needed()
            await gen_btn.click()
            self.log(f"'Generate Reply' clicked (attempt {attempt}/{max_attempts}) — "
                     f"polling for AI response (timeout: {GENERATE_TIMEOUT}s)...")

            t_start       = asyncio.get_event_loop().time()
            last_progress = 0.0
            deadline      = t_start + GENERATE_TIMEOUT
            manual_review = False
            server_error  = False
            server_error_text = ""

            while asyncio.get_event_loop().time() < deadline:
                try:
                    reply = await self._safe_evaluate(tab2, _GET_DE_REPLY_JS)
                    if reply and reply != old_reply:
                        elapsed = asyncio.get_event_loop().time() - t_start
                        self.log(f"AI reply received in {elapsed:.1f}s: {reply[:80]}{'...' if len(reply) > 80 else ''}")
                        return reply

                    if await self._safe_evaluate(tab2, _GET_MANUAL_REVIEW_JS):
                        manual_review = True
                        break

                    server_error_text = await self._safe_evaluate(tab2, _GET_SERVER_ERROR_JS)
                    if server_error_text:
                        server_error = True
                        break
                except PlaywrightError as e:
                    if _is_fatal(e):
                        raise
                    self.log(f"[WARN] Error polling reply: {e}")

                elapsed = asyncio.get_event_loop().time() - t_start
                if elapsed - last_progress >= 15:
                    self.log(f"Still waiting for AI reply... ({elapsed:.0f}s elapsed, {GENERATE_TIMEOUT - elapsed:.0f}s remaining)")
                    last_progress = elapsed
                await asyncio.sleep(1)

            if not manual_review and not server_error:
                raise RuntimeError(f"Timed out ({GENERATE_TIMEOUT}s) waiting for AI reply.")

            if manual_review:
                self.log(f"[WARN] Chameleon flagged this request for manual review "
                         f"(attempt {attempt}/{max_attempts}) — retrying generation.")
            else:
                self.log(f"[WARN] Chameleon returned an error banner ({server_error_text}) "
                         f"(attempt {attempt}/{max_attempts}) — retrying generation.")
            if attempt < max_attempts:
                await asyncio.sleep(2)

        raise ManualReviewLimitExceeded(
            "Chameleon still won't produce a reply after 3 attempts "
            f"({'manual review' if manual_review else 'server error'})."
        )

    async def _get_approved_reply(self, tab1, tab2) -> tuple[str, str]:
        """Generate a reply and block on the approval dashboard before it may be
        sent. A rejection regenerates and resubmits until something is approved.
        ApprovalCancelled propagates to the caller (chat closed, or an operator
        clicked Cancel) so it can restart this chat's Chameleon job instead."""
        while True:
            await report_status(self.cfg.platform, "generating")
            reply = await self._generate_reply(tab2)

            banned = contains_banned_language(reply)
            if banned:
                self.log(f"[GUARD] Reply contained banned language ('{banned}') — discarding, never shown, regenerating.")
                await report_status(self.cfg.platform, "recovering", f"blocked reply containing '{banned}'")
                continue

            self.log(f"Reply generated — awaiting approval: {reply[:80]}{'...' if len(reply) > 80 else ''}")
            await report_status(self.cfg.platform, "awaiting_approval")
            approved, final_text, req_id = await request_approval(
                self.cfg.platform, reply,
                chat_still_active=lambda: self._chat_still_active(tab1),
            )
            if approved:
                self.log(f"[APPROVAL] Approved{' with edits' if final_text != reply else ''}.")
                return final_text, req_id
            self.log("[APPROVAL] Rejected — regenerating a new reply...")

    # ── xkuss (tab1) helpers ─────────────────────────────────────────────────

    async def _get_tab1_html(self, tab1) -> str:
        self.log("Capturing chat HTML...")
        t0   = asyncio.get_event_loop().time()
        # Only serialize the open conversation (#showpm) — never the inbox or
        # other threads on the page. Keeps the payload small so the chameleon
        # tab doesn't choke on it.
        html = await self._safe_evaluate(tab1, _HTML_SERIALIZER_JS, _XKUSS_CHAT_ROOT)
        elapsed = asyncio.get_event_loop().time() - t0
        if html:
            self.log(f"HTML captured: {len(html):,} chars in {elapsed:.1f}s.")
        else:
            self.log(f"[WARN] HTML capture returned empty string after {elapsed:.1f}s!")
        return html

    async def _wait_for_dialog(self, tab1) -> str:
        """Wait in the home/waiting room until the page auto-jumps to a dialog.

        Returns PAGE_CHAT when a dialog opens, or PAGE_LOGIN if the session
        drops. Any page that is neither a chat nor the waiting room is treated
        as a stray page: the bot re-clicks 'Home' to get back to the waiting
        room and keeps waiting. It never gives up on its own — a background bot
        is expected to sit in the waiting room until a conversation arrives.
        """
        self.log("In the waiting room — waiting for a dialog...")
        await report_status(self.cfg.platform, "waiting_for_chat")
        t_start      = asyncio.get_event_loop().time()
        last_report  = t_start
        last_rehome  = t_start
        last_reload  = t_start
        while True:
            state = await self._detect_phase(tab1)
            if state == PAGE_CHAT:
                self.log(f"Dialog received! (waited {asyncio.get_event_loop().time() - t_start:.0f}s)")
                await report_status(self.cfg.platform, "chat_detected")
                return PAGE_CHAT
            if state == PAGE_LOGIN:
                return PAGE_LOGIN

            now = asyncio.get_event_loop().time()
            if state == PAGE_UNKNOWN and now - last_rehome >= UNKNOWN_REHOME_INTERVAL:
                self.log("[RECOVERY] Stray page while waiting — clicking Home to return to the waiting room...")
                await click_home(tab1, self.cfg, self.cfg.platform)
                last_rehome = now
                last_reload = now
            elif state == PAGE_WAITING and now - last_reload >= WAITING_RELOAD_INTERVAL:
                # The site's auto-jump scanner can stall after a conversation ends,
                # so the next dialog never opens. Reloading the waiting room
                # restarts the scanner and lets the next dialog auto-open.
                self.log(
                    f"[RECOVERY] No dialog after {WAITING_RELOAD_INTERVAL}s — reloading "
                    "the waiting room to restart the scanner..."
                )
                try:
                    await tab1.reload(wait_until="domcontentloaded", timeout=30_000)
                except PlaywrightError as e:
                    if _is_fatal(e):
                        raise
                    self.log(f"[WARN] Waiting-room reload failed: {e}")
                last_reload = now
                last_report = now
            elif now - last_report >= 30:
                self.log(f"Still in the waiting room... ({now - t_start:.0f}s)")
                last_report = now
            await asyncio.sleep(POLL_INTERVAL)

    async def _nudge_char_counter(self, tab1):
        """Make the old PHP page re-count the reply box so #sendbutton1 enables.

        xkuss enables its send button from an onkeyup character counter (the
        button reads 'Still N characters' until enough text is typed). Playwright's
        fill() sets the textarea value WITHOUT firing keyup, so the counter never
        updates and the button stays disabled. We fire a real key event plus the
        synthetic input/keyup/change events the handler may listen for.
        """
        try:
            textarea = tab1.locator(self.cfg.sel_textarea)
            await textarea.click()
            # A real keypress that doesn't change the text (End) triggers onkeyup.
            await textarea.press("End")
            await tab1.evaluate(
                """(sel) => {
                    const el = document.querySelector(sel);
                    if (!el) return;
                    for (const type of ['input', 'change', 'keyup', 'keydown']) {
                        el.dispatchEvent(new Event(type, { bubbles: true }));
                    }
                }""",
                self.cfg.sel_textarea,
            )
        except PlaywrightError as e:
            if _is_fatal(e):
                raise
            self.log(f"[WARN] Could not nudge the character counter: {e}")

    async def _wait_send_enabled(self, tab1, send_btn, timeout: int = 8) -> bool:
        """Detector: wait until the send button is actually enabled (True) or
        stays disabled past `timeout` (False)."""
        deadline = asyncio.get_event_loop().time() + timeout
        while asyncio.get_event_loop().time() < deadline:
            try:
                if await send_btn.is_enabled():
                    return True
            except PlaywrightError as e:
                if _is_fatal(e):
                    raise
            await asyncio.sleep(0.5)
        return False

    async def _send_reply(self, tab1, reply: str, approval_id: str | None = None):
        await tab1.bring_to_front()
        textarea = tab1.locator(self.cfg.sel_textarea)
        await textarea.click()
        await textarea.fill(reply)
        # Trigger the character counter so the send button un-disables.
        await self._nudge_char_counter(tab1)
        wait = random.randint(15, 20)
        self.log(f"Reply pasted ({len(reply)} chars) — sending in {wait}s...")
        await asyncio.sleep(wait)

        try:
            send_btn = tab1.locator(self.cfg.sel_send_btn)
            if not await self._wait_send_enabled(tab1, send_btn):
                self.log("[WARN] Send button still disabled — re-triggering the character counter...")
                await self._nudge_char_counter(tab1)
                if not await self._wait_send_enabled(tab1, send_btn):
                    label = ""
                    try:
                        label = (await send_btn.get_attribute("value")) or ""
                    except PlaywrightError:
                        pass
                    raise RuntimeError(
                        f"Send button never enabled (still reads '{label}') — the reply "
                        "box counter did not register the text."
                    )

            await send_btn.scroll_into_view_if_needed()
            await send_btn.click()
        except Exception as e:
            await mark_failed(approval_id, str(e))
            raise
        self.log(f"Sent: {reply[:80]}{'...' if len(reply) > 80 else ''}")
        log_sent_message(self.cfg.platform, reply)
        await mark_sent(approval_id)
        await asyncio.sleep(2)

    # ── Error recovery ───────────────────────────────────────────────────────

    async def _chat_still_active(self, tab1) -> bool:
        """True when tab1 is still sitting on an open dialog (the reply box).

        Used by the error handlers: if a cycle blows up mid-way but a chat is
        still on screen, we restart the Chameleon job on that same chat instead
        of falling back to the waiting room.
        """
        try:
            state, _url, _title = await describe_page(tab1, self.cfg)
            return state == PAGE_CHAT
        except PlaywrightError as e:
            if _is_fatal(e):
                raise
            return False

    async def _restart_chameleon_job(self, tab1, tab2):
        """Re-establish a clean Chameleon (tab2) state after a mid-cycle failure.

        The chat is still on screen, so the next cycle will re-copy the HTML,
        paste it, extract and generate. Here we just make sure tab2 is on the
        workspace with a chat selected first. Best-effort: non-fatal errors are
        swallowed so the retry loop still proceeds.
        """
        self.log("[RECOVERY] Failure but chat still active — restarting Chameleon job "
                 "(re-copy HTML → paste → generate).")
        try:
            if await self._is_chameleon_broken(tab2):
                await self._fix_chameleon_only(tab2)
            await self._ensure_chat_selected(tab2)
        except PlaywrightError as e:
            if _is_fatal(e):
                raise
            self.log(f"[WARN] Chameleon health-fix during recovery failed: {e}")

    # ── Main entry point ─────────────────────────────────────────────────────

    async def run(self):
        # ── Make sure Chrome is up (launch a visible one if needed) ────────
        self._ensure_chrome()

        async with async_playwright() as p:
            # ── Connect to Chrome ──────────────────────────────────────────
            self.log(f"Connecting to Chrome at {self.cfg.cdp_url}...")
            await report_status(self.cfg.platform, "starting")
            browser = None
            for attempt in range(3):
                try:
                    browser = await p.chromium.connect_over_cdp(self.cfg.cdp_url, timeout=10_000)
                    self.log(f"Connected to Chrome. ({len(browser.contexts)} context(s))")
                    break
                except Exception as e:
                    if attempt < 2:
                        self.log(f"[WARN] CDP connect attempt {attempt + 1}/3 failed: {e} — retrying in 5s")
                        await asyncio.sleep(5)
                    else:
                        port = self.cfg.cdp_url.rsplit(":", 1)[-1]
                        self.log(f"[ERROR] Cannot connect to Chrome at {self.cfg.cdp_url}")
                        self.log(f"        Launch Chrome with: --remote-debugging-port={port}")
                        sys.exit(0)

            context = browser.contexts[0]
            self.log(f"Browser context has {len(context.pages)} open page(s).")

            # ── Set up both tabs: login to xkuss + chameleon ───────────────
            tab1 = await self._find_or_open_tab(context, self.cfg.tab1_pattern)
            self.log("Logging in to xkuss and pressing Home...")
            await login_xkuss(tab1, self.cfg, self.cfg.platform)

            tab2 = await self._find_or_open_tab(context, self.cfg.tab2_pattern)
            self.log("Setting up chameleon tab...")
            await login_chameleon(
                tab2, self.cfg.chameleon_email, self.cfg.chameleon_password,
                self.cfg.platform, self.cfg.chameleon_chat,
            )

            self.log("Both tabs ready. Bot running.\n")

            consecutive_errors = 0
            cycle = 0

            while True:
                try:
                    cycle += 1
                    t_cycle = asyncio.get_event_loop().time()
                    self.log(f"──── Cycle {cycle} ────")

                    tab1, tab2 = await self._resolve_tabs(context, retries=3)
                    if tab1 is None or tab2 is None:
                        self.log("[WARN] Tabs lost — waiting 10s before retry...")
                        await asyncio.sleep(10)
                        cycle -= 1
                        continue

                    # ── Decide what page tab1 is on ────────────────────────
                    state = await self._detect_phase(tab1, force_log=True)

                    if state == PAGE_LOGIN:
                        self.log("[RECOVERY] Logged out — logging back in + pressing Home...")
                        await login_xkuss(tab1, self.cfg, self.cfg.platform)
                        cycle -= 1
                        continue

                    # Only an open chat (the reply box) triggers work. Any other
                    # page — the waiting room or a stray page — funnels here: make
                    # sure we are Home, then wait in the waiting room until a
                    # dialog auto-opens, and only then start the workflow.
                    if state != PAGE_CHAT:
                        if state == PAGE_UNKNOWN:
                            self.log("[RECOVERY] On an unexpected page — clicking Home to reach the waiting room...")
                            await click_home(tab1, self.cfg, self.cfg.platform)
                        await tab1.bring_to_front()
                        state = await self._wait_for_dialog(tab1)
                        if state == PAGE_LOGIN:
                            await login_xkuss(tab1, self.cfg, self.cfg.platform)
                            cycle -= 1
                            continue
                        if state != PAGE_CHAT:
                            cycle -= 1
                            continue

                    # ── We are on a chat page — do the chameleon work ──────
                    if await self._is_chameleon_broken(tab2):
                        await self._fix_chameleon_only(tab2)
                    await self._ensure_chat_selected(tab2)

                    await tab1.bring_to_front()
                    await self._wait_for_page_ready(tab1)
                    html = await self._get_tab1_html(tab1)
                    if not html:
                        self.log("[WARN] Empty HTML — skipping this cycle.")
                        cycle -= 1
                        continue

                    await tab2.bring_to_front()
                    await self._paste_and_extract(tab2, html)

                    if await self._is_first_contact(tab2):
                        self.log("[FC] First Contact detected — reloading tabs and going Home.")
                        await tab2.reload()
                        await self._wait_for_page_ready(tab2, "domcontentloaded")
                        await self._ensure_chat_selected(tab2)
                        await click_home(tab1, self.cfg, self.cfg.platform)
                        cycle -= 1
                        continue

                    try:
                        reply, approval_id = await self._get_approved_reply(tab1, tab2)
                    except ManualReviewLimitExceeded:
                        self.log("[RECOVERY] Chameleon kept flagging this request for manual "
                                 "review after 3 attempts — refreshing the chat (re-extracting "
                                 "into Chameleon; tab1 stays put so the loaded dialog isn't lost).")
                        await self._restart_chameleon_job(tab1, tab2)
                        cycle -= 1
                        continue
                    except ApprovalCancelled:
                        self.log("[APPROVAL] Cancelled — chat closed or operator cancelled it. "
                                 "Restarting the Chameleon job on this chat...")
                        await self._restart_chameleon_job(tab1, tab2)
                        cycle -= 1
                        continue
                    await report_status(self.cfg.platform, "sending")
                    await self._send_reply(tab1, reply, approval_id)

                    if consecutive_errors > 0:
                        self.log(f"Consecutive error counter reset (was {consecutive_errors}).")
                    consecutive_errors = 0

                    elapsed = asyncio.get_event_loop().time() - t_cycle
                    self.log(f"──── Cycle {cycle} complete in {elapsed:.1f}s ────\n")

                    # After every reply, press Home and let the site's scanner
                    # route us to the next dialog that needs answering. If it
                    # lands on a chat (this same conversation if the user wrote
                    # back, or a new one), the next cycle does the work again; if
                    # nothing is waiting, the next cycle sits in the waiting room
                    # until a dialog auto-opens.
                    self.log("Reply sent — pressing Home to fetch the next dialog.")
                    await click_home(tab1, self.cfg, self.cfg.platform)

                except KeyboardInterrupt:
                    self.log("Stopped by user.")
                    sys.exit(0)

                except PlaywrightTimeout as e:
                    consecutive_errors += 1
                    self.log(
                        f"[WARN] Timeout ({type(e).__name__}): {e} — "
                        f"retrying in 15s (error {consecutive_errors}/{MAX_ERRORS})"
                    )
                    await report_status(self.cfg.platform, "error", str(e)[:200])
                    await asyncio.sleep(15)
                    if consecutive_errors >= MAX_ERRORS:
                        self.log(f"[FATAL] {MAX_ERRORS} consecutive errors — triggering restart")
                        sys.exit(1)
                    if await self._chat_still_active(tab1):
                        await self._restart_chameleon_job(tab1, tab2)
                        cycle -= 1
                        continue

                except Exception as e:
                    if _is_fatal(e):
                        self.log(f"[FATAL] Browser disconnected: {e}")
                        sys.exit(1)
                    consecutive_errors += 1
                    self.log(
                        f"[ERROR] {type(e).__name__}: {e} "
                        f"(error {consecutive_errors}/{MAX_ERRORS}) — retrying in 15s..."
                    )
                    await report_status(self.cfg.platform, "error", str(e)[:200])
                    await asyncio.sleep(15)
                    if consecutive_errors >= MAX_ERRORS:
                        self.log(f"[FATAL] {MAX_ERRORS} consecutive errors — triggering restart")
                        sys.exit(1)
                    if await self._chat_still_active(tab1):
                        await self._restart_chameleon_job(tab1, tab2)
                        cycle -= 1
                        continue
