#!/usr/bin/env python3
"""
justlo chat automation core.

justlo is reached through the justlo.de landing page and driven inside the old
ExtJS moderation console at mod.justlo.de/community-mod/ — a DOM that matches
neither the React platforms (core/bot.py) nor the old xkuss PHP site
(core/xkuss_bot.py), so it gets its own bot. The chameleon (tab2) half is
identical to every other platform, so this module reuses the shared chameleon
login + the HTML serializer / reply-extractor JavaScript from core/bot.py.

Flow (matches the manual process):
  1. land on mod.justlo.de -> Login -> Einloggen -> click 'Mod' -> Play
  2. wait in the running console until a dialog is fed into the workspace
  3. every dialog is captured (conversation HTML) and pasted into chameleon —
     chameleon's own First Contact badge is the SOLE authority on whether to
     hand the dialog over. The local 'Unterhaltung' grid (sel_conv_grid) is no
     longer used to decide this: a slow-to-render grid was causing real,
     non-first-contact conversations to be handed off to another moderator and
     lost, instead of getting a real reply.
  3a. chameleon says NOT First Contact: Generate Reply -> paste the reply into
      the message box -> send (Abschicken), then wait for the next conversation
      to appear on its own ('Überspringen' is never pressed for a normal reply).
  3b. chameleon says First Contact: click 'Übergeben' and hand it to any other
      moderator in the popup (never our own account) -> OK; if no one else is
      listed, cancel the popup and press 'Überspringen' instead
  4. land on an unexpected page / logged out at any point -> re-run login_justlo()

Exit codes (used by launcher to decide restart):
  0 = setup failure (Chrome unreachable, tabs cannot be opened) — do NOT restart
  1 = runtime crash (browser dropped, repeated errors)          — DO restart
"""

import asyncio
import random
import re
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
)
from core.login import login_chameleon, chat_not_selected
from core.launcher import is_cdp_ready, start_chrome, wait_for_cdp
from core.approval import request_approval, mark_sent, mark_failed, report_status, ApprovalCancelled
from core.justlo_login import (
    login_justlo,
    go_console,
    press_play,
    describe_page,
    PAGE_LOGIN,
    PAGE_COMMUNITY,
    PAGE_CHAT,
    PAGE_WAITING,
    PAGE_UNKNOWN,
)

# The queue label ('#queue-message') tags follow-up ASA tasks like
# "[ ASA 3 ] 1 Tag letzte ASA" or "[ ASA 2 ] ...". For ASA 2 / ASA 3 we don't
# write a reply — we just nudge the client ('Anstupsen'). The \b before ASA keeps
# this from matching 'FASA' (a first-contact task, which has no number anyway).
_ASA_NUDGE_RE = re.compile(r"\bASA\s*[23]\b", re.IGNORECASE)

POLL_INTERVAL    = 3    # seconds between idle checks
WAITING_REPLAY_INTERVAL = 90  # seconds with no dialog before re-pressing Play / reloading the console
UNKNOWN_RECOVER_INTERVAL = 30  # seconds between recovery attempts while stuck off the console
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

# Transfer ('Übergeben') popup — opened from tab1's toolbar on a First Contact so
# the dialog gets handed to another moderator instead of stalling the console.
# The popup window itself gets a fresh Ext auto-id every time it's opened
# (thia-userselectwnd-NNNN), so it's located structurally (the .x-window that
# contains the grid) rather than by that id. '#user-select-grid' is a stable
# itemId; its row grid gets a fresh auto-id too, so rows are queried through
# the stable grid id rather than '#gridview-NNNN-body'.
_SEL_TRANSFER_POPUP = ".x-window:has(#user-select-grid)"
_SEL_TRANSFER_ROWS  = "#user-select-grid .x-grid-row"
_GET_TRANSFER_NAMES_JS = """() => {
  const rows = document.querySelectorAll('#user-select-grid .x-grid-row');
  return Array.from(rows).map((row) => {
    const cells = row.querySelectorAll('.x-grid-cell-inner');
    return cells.length ? (cells[cells.length - 1].textContent || '').trim() : '';
  });
}"""

# Inject the HTML straight into the React-controlled chameleon textarea in ONE
# shot: native value setter + a single 'input' event so React picks it up without
# Playwright's focus/clear/type/verify sequence.
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
class JustloConfig:
    platform: str = "Justlo"
    cdp_url:  str = "http://127.0.0.1:9229"
    profile:  str = "justlo"              # Chrome profile dir under profiles/
    tab1_pattern: str = "justlo"          # matches justlo.de AND mod.justlo.de
    tab2_pattern: str = "chamaleon-ai"    # substring matched against the chameleon tab URL

    # ── mod-site login flow ──────────────────────────────────────────────────
    # justlo and linduu land on the SAME ExtJS console but log in differently:
    #   justlo — mod.justlo.de opens a `#login` MODAL via a header 'Login' link.
    #   linduu — a dedicated login PAGE (login_url); no modal, and 'Mod' lives in
    #            a 'Mehr' dropdown so the console is reached by URL (console_via_goto).
    tab1_url: str = "https://mod.justlo.de"                 # entry / landing URL
    login_url: str = ""                                     # dedicated login page (linduu); "" = use tab1_url + modal
    mod_url:  str = "https://mod.justlo.de/community-mod/"    # the ExtJS console URL
    console_via_goto: bool = False                          # reach console by URL instead of clicking 'Mod'
    username: str = ""                                       # #login input[name='username']
    password: str = ""                                       # #login input[name='password']

    # ── chameleon AI (shared) ───────────────────────────────────────────────
    chameleon_email:    str = ""
    chameleon_password: str = ""
    chameleon_chat:     str = "Justlo/Linduu DE"
    additional_instructions: str = ""

    # ── justlo-specific selectors ────────────────────────────────────────────
    # landing page / login modal (`#login` form is unique to the logged-out page)
    sel_login_link: str = "a:has-text('Login')"
    sel_login_user: str = "#login input[name='username']"
    sel_login_pass: str = "#login input[name='password']"
    sel_login_btn:  str = "#login input[type='submit'][value='Einloggen']"
    # community home -> console
    sel_mod_link:   str = "a[href*='community-mod']"
    # ExtJS moderation console
    sel_play_btn:   str = "#buttonTbPlay"
    sel_pause_btn:  str = "#buttonTbStop"
    sel_transfer_btn: str = "#buttonTbForward"          # 'Übergeben' (hand dialog to another moderator)
    sel_skip_btn:   str = "#buttonTbSkip"                # 'Überspringen' — fallback when the transfer popup has no one else to pick
    sel_textarea:   str = "textarea[name='message']"     # #textarea-1056-inputEl
    sel_send_btn:   str = "#button-1059"                 # 'Abschicken'
    sel_anstupsen_btn: str = "#button-1060"              # 'Anstupsen' (ASA 2 / ASA 3 nudge)
    # A loaded dialog fills the client (Kunde) panel with a REAL member; when the
    # console is idle the panel is blank (empty username link, age shows '(NaN)').
    # So a non-empty client username — NOT a message-grid row — marks a live dialog
    # (a FASA first-contact has a client but zero message rows).
    sel_client_username: str = "#user-panel a.username"           # the client (Kunde)
    sel_fake_username:   str = "#moderator-user-panel a.username"  # the fake account
    sel_queue_message:   str = "#queue-message"                   # FASA / queue task line
    sel_conv_grid:  str = "#gridview-1055-body"          # message rows (empty on a FASA)
    sel_conv_root:  str = "#panel-1012-body"             # serialized to chameleon (profiles + convo)


# ── Bot ────────────────────────────────────────────────────────────────────────

_PHASE_LABELS = {
    PAGE_LOGIN:     "LOGIN (logged out)",
    PAGE_COMMUNITY: "COMMUNITY (not in console yet)",
    PAGE_CHAT:      "IN A CONVERSATION",
    PAGE_WAITING:   "CONSOLE — WAITING FOR DIALOG",
    PAGE_UNKNOWN:   "UNKNOWN / stray page",
}


class JustloBot:
    def __init__(self, config: JustloConfig):
        self.cfg = config
        self._last_phase = None   # last logged phase, so we only log on change
        self._last_sig   = None   # signature of the last conversation we handled

    def log(self, msg: str):
        ts = datetime.now().strftime("%H:%M:%S")
        print(f"[{ts}][{self.cfg.platform}] {msg}", flush=True)

    async def _detect_phase(self, tab1, *, force_log: bool = False) -> str:
        """Classify tab1's phase and log it whenever it changes."""
        state, url, title = await describe_page(tab1, self.cfg)
        if force_log or state != self._last_phase:
            label = _PHASE_LABELS.get(state, state)
            self.log(f"[PHASE] {label}  |  {title[:60]!r}  {url[:80]}")
            self._last_phase = state
        return state

    # ── Chrome launch ────────────────────────────────────────────────────────

    def _ensure_chrome(self):
        """Make sure a Chrome with the right debugging port is up.

        Connects to an existing Chrome over CDP; if nothing is listening we launch
        a visible Chrome (per-platform profile) so the user can watch and finish
        any manual step (e.g. a captcha) if one ever appears.
        """
        port = int(self.cfg.cdp_url.rsplit(":", 1)[-1])
        if is_cdp_ready(port):
            self.log(f"Chrome already running on port {port}.")
            return

        base_dir = Path(__file__).resolve().parent.parent
        profile  = str(base_dir / "profiles" / self.cfg.profile)
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
            if chat_not_selected(combo_text):
                return False
            # Not just "some chat" — it must be OUR chat. Otherwise a stale/wrong
            # selection (e.g. a reload-race fallback pick) looks "fine" forever.
            return self.cfg.chameleon_chat.lower() in combo_text.lower()
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

    # ── justlo (tab1) helpers ─────────────────────────────────────────────────

    async def _get_tab1_html(self, tab1) -> str:
        self.log("Capturing conversation HTML...")
        t0 = asyncio.get_event_loop().time()
        # Serialize the moderation workspace body — client profile + fake-account
        # profile + the conversation/message grid — so chameleon has full context
        # (a FASA first-contact has no messages, only the two profiles).
        html = await self._safe_evaluate(tab1, _HTML_SERIALIZER_JS, self.cfg.sel_conv_root)
        elapsed = asyncio.get_event_loop().time() - t0
        if html:
            self.log(f"HTML captured: {len(html):,} chars in {elapsed:.1f}s.")
        else:
            self.log(f"[WARN] HTML capture returned empty string after {elapsed:.1f}s!")
        return html

    async def _conversation_sig(self, tab1) -> str:
        """Fingerprint the loaded dialog so we handle each one exactly once.

        The console keeps rendering the client/fake panels and an (often empty)
        message grid, so 'something is on screen' can't tell a NEW dialog from the
        one we just answered. The client member, the fake account, the queue/FASA
        task line AND the pending-message grid ('Unterhaltung', sel_conv_grid —
        empty on a FASA, one row holding the latest unanswered client message
        otherwise) together change when a new dialog — or a new message on the
        SAME client/fake pairing — is fed in, so we hash all four. Without the
        grid content, two different dialogs that happen to share the same
        client/fake/queue text (e.g. the same client messaging again with no
        queue label) would hash identically and the second one would silently
        never be processed. Returns '' when the panels are blank (the waiting room).
        """
        try:
            return (await tab1.evaluate(
                """(s) => {
                    const t = (sel) => {
                        const el = document.querySelector(sel);
                        return el ? (el.textContent || '').trim() : '';
                    };
                    const client = t(s.client);
                    if (!client) return '';   // waiting room — no client loaded
                    const grid = document.querySelector(s.convGrid);
                    const rows = grid ? Array.from(grid.querySelectorAll('.x-grid-row')) : [];
                    const gridText = rows.map(r => (r.textContent || '').trim()).join('~');
                    return [client, t(s.fake), t(s.queue), gridText].join('|');
                }""",
                {"client":   self.cfg.sel_client_username,
                 "fake":     self.cfg.sel_fake_username,
                 "queue":    self.cfg.sel_queue_message,
                 "convGrid": self.cfg.sel_conv_grid},
            ) or "").strip()
        except PlaywrightError as e:
            if _is_fatal(e):
                raise
            return ""

    async def _wait_for_dialog(self, tab1) -> str:
        """Wait in the running console until a NEW conversation appears.

        The UI always shows a conversation (fake account + client), so we don't
        just wait for 'a dialog' — we wait until the visible conversation differs
        from the last one we handled (self._last_sig). Returns PAGE_CHAT when a
        fresh conversation is on screen. If tab1 drifts off the console (logged
        out / community / stray page) it recovers by re-running login_justlo() and
        keeps waiting. Per the workflow it never presses 'Überspringen' — it only
        waits for the next conversation to arrive on its own.
        """
        self.log("Console running — waiting for a new conversation...")
        await report_status(self.cfg.platform, "waiting_for_chat")
        t_start     = asyncio.get_event_loop().time()
        last_report = t_start
        last_recover = t_start
        last_replay = t_start
        while True:
            state = await self._detect_phase(tab1)
            if state == PAGE_CHAT:
                sig = await self._conversation_sig(tab1)
                if sig and sig != self._last_sig:
                    self.log(f"New conversation! (waited {asyncio.get_event_loop().time() - t_start:.0f}s)")
                    await report_status(self.cfg.platform, "chat_detected")
                    return PAGE_CHAT
                # Same conversation we already handled (or nothing loaded yet) —
                # keep waiting for the next one; do NOT skip.
            if state == PAGE_LOGIN:
                return PAGE_LOGIN

            now = asyncio.get_event_loop().time()
            if state in (PAGE_UNKNOWN, PAGE_COMMUNITY) and now - last_recover >= UNKNOWN_RECOVER_INTERVAL:
                self.log("[RECOVERY] Off the console while waiting — re-opening the console...")
                await go_console(tab1, self.cfg, self.cfg.platform)
                last_recover = now
                last_replay = now
            elif state == PAGE_WAITING and now - last_replay >= WAITING_REPLAY_INTERVAL:
                # The scanner can stall after a dialog ends; re-press Play, and if
                # that doesn't help reload the console to restart it.
                self.log(f"[RECOVERY] No dialog after {WAITING_REPLAY_INTERVAL}s — re-pressing Play...")
                await press_play(tab1, self.cfg, f"[{self.cfg.platform}] ")
                try:
                    await tab1.reload(wait_until="domcontentloaded", timeout=30_000)
                    await asyncio.sleep(1.5)
                    await press_play(tab1, self.cfg, f"[{self.cfg.platform}] ")
                except PlaywrightError as e:
                    if _is_fatal(e):
                        raise
                    self.log(f"[WARN] Console reload failed: {e}")
                last_replay = now
                last_report = now
            elif now - last_report >= 30:
                self.log(f"Still waiting for a dialog... ({now - t_start:.0f}s)")
                last_report = now
            await asyncio.sleep(POLL_INTERVAL)

    async def _nudge_message_box(self, tab1):
        """Make the ExtJS message field re-evaluate so the send button enables.

        ExtJS validates the reply box from its own input handlers; fill() sets the
        value but may not fire every event the component listens for, so we click
        the field, press End (a real key event that doesn't change the text), then
        dispatch the synthetic input/change/keyup/keydown events for good measure.
        """
        try:
            textarea = tab1.locator(self.cfg.sel_textarea)
            await textarea.click()
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
            self.log(f"[WARN] Could not nudge the message box: {e}")

    async def _message_box_empty(self, tab1) -> bool:
        """True when the message field is empty — used to confirm a send landed
        (ExtJS clears the box after 'Abschicken')."""
        try:
            val = await tab1.eval_on_selector(
                self.cfg.sel_textarea, "el => (el.value || '').trim()"
            )
            return val == ""
        except PlaywrightError as e:
            if _is_fatal(e):
                raise
            return False

    async def _is_asa_nudge(self, tab1) -> bool:
        """True when the queue task is an ASA 2 / ASA 3 follow-up.

        Those are answered with a nudge ('Anstupsen'), not a written reply, so the
        caller skips the whole chameleon step and just waits for the next dialog.
        """
        try:
            txt = await tab1.evaluate(
                "(s) => { const el = document.querySelector(s);"
                " return el ? (el.textContent || '').trim() : ''; }",
                self.cfg.sel_queue_message,
            )
        except PlaywrightError as e:
            if _is_fatal(e):
                raise
            return False
        return bool(_ASA_NUDGE_RE.search(txt or ""))

    async def _click_anstupsen(self, tab1):
        """Press 'Anstupsen' to nudge the client (used for ASA 2 / ASA 3)."""
        self.log("[ASA] ASA 2/3 reminder — clicking 'Anstupsen' (no reply generated).")
        btn = tab1.locator(self.cfg.sel_anstupsen_btn)
        await btn.scroll_into_view_if_needed()
        await btn.click()
        await asyncio.sleep(2)

    async def _handover_first_contact(self, tab1) -> bool:
        """Click 'Übergeben' and hand a First Contact dialog to another moderator.

        Opens the transfer popup, picks any name in the list EXCEPT our own
        account (self.cfg.username), and confirms with 'OK'. This keeps the
        console moving on a First Contact instead of stalling on it, so the bot
        never has to sit and wait on a dialog it isn't going to write into.
        Returns True on a confirmed handover, False if anything went wrong
        (popup never opened, or only our own name was in the list) — the caller
        treats either outcome as "done with this dialog" either way.
        """
        self.log("[FC] Clicking 'Übergeben' to hand the dialog to another moderator...")
        try:
            await tab1.bring_to_front()
            await tab1.locator(self.cfg.sel_transfer_btn).click()

            popup = tab1.locator(_SEL_TRANSFER_POPUP)
            rows = popup.locator(_SEL_TRANSFER_ROWS)
            await rows.first.wait_for(state="visible", timeout=10_000)

            names = await tab1.evaluate(_GET_TRANSFER_NAMES_JS)
            own = self.cfg.username
            candidates = [i for i, name in enumerate(names) if name and name != own]

            if not candidates:
                self.log(f"[WARN] No other moderator in the transfer popup besides '{own}' — "
                         f"closing popup and pressing 'Überspringen' instead.")
                cancel_btn = popup.locator("a[role='button']:has-text('Abbrechen')")
                if await cancel_btn.count() > 0:
                    await cancel_btn.first.click()
                    await asyncio.sleep(0.5)
                await tab1.locator(self.cfg.sel_skip_btn).click()
                await asyncio.sleep(1.5)
                return False

            pick = random.choice(candidates)
            self.log(f"[FC] Handing dialog to '{names[pick]}' (excluding own account '{own}').")
            await rows.nth(pick).click()
            await asyncio.sleep(0.3)

            ok_btn = popup.locator("a[role='button']:has-text('OK')")
            await ok_btn.first.click()
            await asyncio.sleep(1.5)
            return True
        except PlaywrightError as e:
            if _is_fatal(e):
                raise
            self.log(f"[WARN] Handover failed: {e}")
            return False

    async def _send_reply(self, tab1, reply: str, approval_id: str | None = None):
        await tab1.bring_to_front()
        textarea = tab1.locator(self.cfg.sel_textarea)
        await textarea.click()
        await textarea.fill(reply)
        await self._nudge_message_box(tab1)
        wait = random.randint(15, 20)
        self.log(f"Reply pasted ({len(reply)} chars) — sending in {wait}s...")
        await asyncio.sleep(wait)

        try:
            send_btn = tab1.locator(self.cfg.sel_send_btn)
            await send_btn.scroll_into_view_if_needed()
            await send_btn.click()
            await asyncio.sleep(2)

            # Confirm the send registered; if the box still holds the text, re-nudge
            # and click once more before giving up.
            if not await self._message_box_empty(tab1):
                self.log("[WARN] Message box not cleared after send — re-nudging and retrying once...")
                await self._nudge_message_box(tab1)
                await asyncio.sleep(1)
                await send_btn.click()
                await asyncio.sleep(2)
                if not await self._message_box_empty(tab1):
                    raise RuntimeError(
                        "Send did not register (message box still holds the reply) — "
                        "the 'Abschicken' button may be disabled by a character counter."
                    )
        except Exception as e:
            await mark_failed(approval_id, str(e))
            raise
        self.log(f"Sent: {reply[:80]}{'...' if len(reply) > 80 else ''}")
        log_sent_message(self.cfg.platform, reply)
        await mark_sent(approval_id)

    # ── Error recovery ───────────────────────────────────────────────────────

    async def _chat_still_active(self, tab1) -> bool:
        """True when tab1 is still sitting on a loaded dialog."""
        try:
            state, _url, _title = await describe_page(tab1, self.cfg)
            return state == PAGE_CHAT
        except PlaywrightError as e:
            if _is_fatal(e):
                raise
            return False

    async def _restart_chameleon_job(self, tab1, tab2):
        """Re-establish a clean chameleon state after a mid-cycle failure."""
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

            # ── Set up both tabs: login to justlo + chameleon ──────────────
            tab1 = await self._find_or_open_tab(context, self.cfg.tab1_pattern)
            self.log("Logging in to justlo and opening the console (Play)...")
            await login_justlo(tab1, self.cfg, self.cfg.platform)

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

                    # ── Decide what surface tab1 is on ─────────────────────
                    state = await self._detect_phase(tab1, force_log=True)

                    # Logged out, on the community home, or a stray page — climb
                    # back to a running console and re-evaluate next cycle.
                    if state in (PAGE_LOGIN, PAGE_COMMUNITY, PAGE_UNKNOWN):
                        self.log("[RECOVERY] Not on a live console — running login/nav to reach it...")
                        await login_justlo(tab1, self.cfg, self.cfg.platform)
                        cycle -= 1
                        continue

                    # In the console — wait until a NEW conversation appears (the
                    # UI always shows one, so this gates on the content changing;
                    # 'Überspringen' is never pressed).
                    await tab1.bring_to_front()
                    state = await self._wait_for_dialog(tab1)
                    if state == PAGE_LOGIN:
                        await login_justlo(tab1, self.cfg, self.cfg.platform)
                        cycle -= 1
                        continue
                    if state != PAGE_CHAT:
                        cycle -= 1
                        continue

                    # Fingerprint this conversation so we don't handle it twice.
                    sig = await self._conversation_sig(tab1)

                    # ── ASA 2 / ASA 3 follow-up → nudge, don't reply ───────
                    # These queue tasks are answered by pressing 'Anstupsen'; no
                    # HTML capture, no chameleon — mark handled and wait for the
                    # next conversation.
                    if await self._is_asa_nudge(tab1):
                        await tab1.bring_to_front()
                        await self._click_anstupsen(tab1)
                        self._last_sig = sig
                        self.log("[ASA] Nudged — waiting for the next conversation.")
                        cycle -= 1
                        continue

                    # ── Always run the chameleon work — chameleon's own First
                    # Contact badge is the sole authority on handover. The local
                    # 'Unterhaltung' grid is NOT trusted to decide this on its own:
                    # a slow-to-render grid was causing real, non-first-contact
                    # conversations to be handed over to another moderator and lost.
                    # So every dialog is captured and pasted into chameleon, and only
                    # chameleon's verdict (checked below, after extraction) decides
                    # between handover and a normal reply.
                    await tab1.bring_to_front()
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
                        # Chameleon is the sole authority on First Contact — hand over
                        # only when it says so, never based on the local DOM grid.
                        self.log("[FC] Chameleon flagged First Contact — "
                                 "handing over via 'Übergeben'.")
                        await self._handover_first_contact(tab1)
                        await tab2.reload()
                        await self._wait_for_page_ready(tab2, "domcontentloaded")
                        # Reload is a React SPA remount — domcontentloaded fires long
                        # before the combobox exists, so give it a moment to hydrate
                        # before checking/selecting the chat, or we race the fallback
                        # into picking whatever chat happens to be first in the list.
                        try:
                            await tab2.locator(_SEL_COMBOBOX).first.wait_for(
                                state="visible", timeout=15_000
                            )
                        except Exception:
                            pass
                        await self._ensure_chat_selected(tab2)
                        await self._ensure_extractor_tab_active(tab2)
                        # Mark this one handled so the wait loop holds for a new one.
                        self._last_sig = sig
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
                    # Mark this conversation handled so we don't answer it again;
                    # the next cycle waits until a different conversation appears.
                    self._last_sig = sig

                    if consecutive_errors > 0:
                        self.log(f"Consecutive error counter reset (was {consecutive_errors}).")
                    consecutive_errors = 0

                    elapsed = asyncio.get_event_loop().time() - t_cycle
                    self.log(f"──── Cycle {cycle} complete in {elapsed:.1f}s ────\n")

                    # Answered — do NOT skip; wait for the next conversation to
                    # arrive on its own on the next cycle.
                    self.log("Reply sent — waiting for the next conversation.")

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
                        f"[WARN] {type(e).__name__}: {e} — "
                        f"retrying in 10s (error {consecutive_errors}/{MAX_ERRORS})"
                    )
                    await report_status(self.cfg.platform, "error", str(e)[:200])
                    await asyncio.sleep(10)
                    if consecutive_errors >= MAX_ERRORS:
                        self.log(f"[FATAL] {MAX_ERRORS} consecutive errors — triggering restart")
                        sys.exit(1)
