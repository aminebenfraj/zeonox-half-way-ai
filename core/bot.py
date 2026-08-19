#!/usr/bin/env python3
"""
Hardened chat automation core for all platforms.
Instantiate ChatBot with a BotConfig and call .run().

Exit codes (used by launcher to decide restart):
  0 = setup failure (tabs not found, Chrome unreachable) — do NOT restart
  1 = runtime crash (browser dropped, repeated errors)   — DO restart
"""

import asyncio
import random
import re
import sys
import time
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from playwright.async_api import async_playwright, TimeoutError as PlaywrightTimeout, Error as PlaywrightError
from core.login import login_mod_site, login_chameleon, chat_not_selected
from core.approval import request_approval, mark_sent, mark_failed, report_status, ApprovalCancelled

# ── Pause / resume ────────────────────────────────────────────────────────────
# Cross-process signal: the launcher (start_all.py/launch_all.py) creates/deletes
# a per-platform flag file when the operator types 'pause'/'resume'; each bot
# process just polls for its own file's existence (see ChatBot._wait_while_paused).
_STATE_DIR = Path(__file__).resolve().parent.parent

def pause_flag_path(platform: str) -> Path:
    return _STATE_DIR / f".pause_{platform.lower()}.flag"


# ── Sent-message log ──────────────────────────────────────────────────────────
# One shared file across every platform/process so the full send history reads
# as a single chronological timeline: "[time] [platform] message".
_NOTE_LOG_PATH = _STATE_DIR / "note_logs.log"

def log_sent_message(platform: str, message: str):
    ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    line = f"[{ts}] [{platform}] {message}\n"
    with open(_NOTE_LOG_PATH, "a", encoding="utf-8") as f:
        f.write(line)

POLL_INTERVAL    = 3    # seconds between idle checks
EXTRACT_TIMEOUT  = 30   # seconds to wait for Generate Reply button
GENERATE_TIMEOUT = 90   # seconds to wait for AI to finish
ERROR_RELOGIN_AFTER = 300  # seconds of unbroken errors (reply won't send) before logout+login
STAGNANT_CYCLES_BEFORE_RELOGIN = 3  # generate->paste->send cycles on the same unanswered customer message before forcing logout+login
TAB_RETRY_DELAY  = 5    # seconds between tab-search retries
TAB_MAX_RETRIES  = 6    # max tab-search retries before giving up


# ── Helpers ────────────────────────────────────────────────────────────────────

_FATAL_SUBSTRINGS = (
    "target closed",
    "browser has been closed",
    "connection refused",
    "websocket is closed",
    "browser has been disconnected",
    "connection reset",
)

def _is_fatal(e: Exception) -> bool:
    msg = str(e).lower()
    return any(s in msg for s in _FATAL_SUBSTRINGS)

def _is_context_destroyed(e: Exception) -> bool:
    return "execution context was destroyed" in str(e).lower()


# Reply-side profanity guard: Chameleon occasionally generates a reply
# containing a genuine insult rather than the flirty-but-blunt tone that's
# otherwise fine on these platforms — that must never reach a customer. Every
# _get_approved_reply() (in this file, xkuss_bot.py and justlo_bot.py) checks
# each freshly generated reply against this list before it's even shown for
# approval; a hit is treated exactly like a human rejection — discarded, and
# regenerated — so it can never slip through, including in Auto mode where
# there's no human in the loop to catch it.
_BANNED_WORDS = [
    "arschloch",       # asshole
    "depp",            # idiot
    "trottel",         # idiot/moron
    "arschgeige",      # jerk/dickhead
    "schlampe",        # bitch/slut
    "fick dich",       # fuck you
    "verpiss dich",    # piss off / get lost
    "halt die fresse", # shut up (very rude)
    "halt das maul",   # shut your trap (very rude)
    "hurensohn",       # severe insult referring to one's parentage
    "fuck",
]
_BANNED_WORDS_RE = re.compile("|".join(re.escape(w) for w in _BANNED_WORDS), re.IGNORECASE)


def contains_banned_language(text: str) -> str | None:
    """Returns the matched word/phrase if `text` contains banned language, else None."""
    m = _BANNED_WORDS_RE.search(text or "")
    return m.group(0) if m else None


class LoggedOutError(Exception):
    """Raised when the mod site session has expired."""


class SendFailedError(Exception):
    """Raised when the reply was pasted and 'sent' 3 times but never actually went out."""


class ManualReviewLimitExceeded(Exception):
    """Chameleon flagged the request as needing manual handling (e.g. an outing
    attempt) on every one of the retry attempts in _generate_reply(). The caller
    refreshes the chat and starts the cycle over instead of giving up."""


# ── JavaScript ─────────────────────────────────────────────────────────────────

_HTML_SERIALIZER_JS = """(rootSel) => {
  const SKIP_TAGS = new Set([
    'script','style','noscript','meta','link','head','template','slot',
    'svg','path','circle','rect','line','polyline','polygon','ellipse',
    'g','defs','use','symbol','clippath','lineargradient','radialgradient',
    'stop','pattern','marker','text','tspan','textpath','filter',
    'fegaussianblur','fecolormatrix','feblend','fecomposite',
  ]);
  const VOID_TAGS = new Set([
    'area','base','br','col','embed','hr','img','input',
    'param','source','track','wbr',
  ]);
  const STYLE_PROPS = [
    'display','overflow','overflow-x','overflow-y',
    'flex-direction','flex-wrap','flex','flex-grow','flex-shrink',
    'justify-content','align-items','align-self','gap',
    'position','top','right','bottom','left','z-index',
    'width','height','min-width','max-width','min-height','max-height',
    'padding','padding-top','padding-right','padding-bottom','padding-left',
    'margin','margin-top','margin-right','margin-bottom','margin-left',
    'border','border-top','border-right','border-bottom','border-left',
    'border-radius','border-collapse','border-color','border-width',
    'background-color','background-image','background-size','background-position',
    'backdrop-filter','color','font-family','font-size','font-weight','font-style',
    'line-height','letter-spacing','text-align','text-decoration','text-transform',
    'white-space','word-break','overflow-wrap','vertical-align',
    'cursor','opacity','resize','box-sizing','box-shadow',
    'user-select','list-style','transform',
    'transition-property','transition-timing-function','transition-duration',
    'aspect-ratio',
  ];
  const ALWAYS_SKIP = new Set([
    '','initial','unset','inherit','revert','rgba(0, 0, 0, 0)',
    'ease','all','0s','normal','none','repeat','scroll','padding-box',
    'outside none disc','outside none none',
  ]);
  const PROP_DEFAULTS = {
    'display':'inline','position':'static',
    'overflow':'visible','overflow-x':'visible','overflow-y':'visible',
    'flex-direction':'row','flex-wrap':'nowrap','flex-grow':'0','flex-shrink':'1',
    'opacity':'1','border-collapse':'separate','vertical-align':'baseline',
    'text-align':'start','text-transform':'none','white-space':'normal',
    'word-break':'normal','overflow-wrap':'normal','cursor':'auto','resize':'none',
    'box-shadow':'none','backdrop-filter':'none','transform':'none',
    'letter-spacing':'normal','aspect-ratio':'auto','list-style':'outside none disc',
    'background-image':'none','background-size':'auto','background-position':'0% 0%',
    'user-select':'auto',
  };

  function buildStyleAttr(el) {
    const computed = window.getComputedStyle(el);
    const original = el.getAttribute('style') || '';
    const map = new Map();
    if (original) {
      original.split(';').forEach(decl => {
        const colon = decl.indexOf(':');
        if (colon === -1) return;
        const key = decl.slice(0, colon).trim();
        const val = decl.slice(colon + 1).trim();
        if (key && val) map.set(key, val);
      });
    }
    STYLE_PROPS.forEach(prop => {
      if (map.has(prop)) return;
      const val = computed.getPropertyValue(prop).trim();
      if (!val) return;
      if (ALWAYS_SKIP.has(val)) return;
      if (PROP_DEFAULTS[prop] === val) return;
      if (val === '0px' && (prop.startsWith('padding') || prop.startsWith('margin') || prop === 'border-width')) return;
      if (prop === 'background-color' && (val === 'rgba(0, 0, 0, 0)' || val === 'transparent')) return;
      map.set(prop, val);
    });
    if (!map.size) return '';
    return Array.from(map.entries()).map(([k,v]) => `${k}:${v}`).join(';');
  }

  function escText(str) { return str.replace(/&/g,'&amp;').replace(/</g,'&lt;').replace(/>/g,'&gt;'); }

  const HTML_ATTRS = [
    'src','srcset','alt','href','placeholder','rows','cols','type','value','name',
    'width','height','loading','decoding','data-nimg','id','class','role',
    'aria-label','aria-hidden','target','rel','for',
  ];

  function serialize(node, depth) {
    const indent = '    '.repeat(depth);
    if (node.nodeType === 3) {
      const t = node.textContent.replace(/\\s+/g, ' ').trim();
      return t ? indent + escText(t) + '\\n' : '';
    }
    if (node.nodeType !== 1) return '';
    const tag = node.tagName.toLowerCase();
    if (SKIP_TAGS.has(tag)) return '';

    const attrParts = [];
    const styleStr = buildStyleAttr(node);
    if (styleStr) attrParts.push(`style="${styleStr.replace(/"/g, "'")}"`);
    HTML_ATTRS.forEach(a => {
      const v = node.getAttribute(a);
      if (v !== null && v.trim() !== '') attrParts.push(`${a}="${v.trim().replace(/"/g,'&quot;')}"`);
    });
    const attrsStr = attrParts.length ? ' ' + attrParts.join(' ') : '';

    if (VOID_TAGS.has(tag)) return `${indent}<${tag}${attrsStr} />\\n`;

    const childNodes = Array.from(node.childNodes);
    const visibleChildren = childNodes.filter(c => !(c.nodeType===1 && SKIP_TAGS.has(c.tagName.toLowerCase())));
    const textOnly = visibleChildren.every(c => c.nodeType === 3);
    const textContent = node.textContent.replace(/\\s+/g,' ').trim();

    if (textOnly && textContent) return `${indent}<${tag}${attrsStr}>${escText(textContent)}</${tag}>\\n`;

    let out = `${indent}<${tag}${attrsStr}>\\n`;
    for (const child of childNodes) out += serialize(child, depth + 1);
    out += `${indent}</${tag}>\\n`;
    return out;
  }

  // Serialize only the requested subtree when a root selector is given
  // (xkuss passes '#showpm' so only the OPEN chat is captured, not the inbox /
  // other threads). No selector -> whole body, i.e. unchanged for every other
  // platform. Fall back to body if the selector matches nothing.
  const root = (rootSel && document.querySelector(rootSel)) || document.body;
  return '<html>\\n\\n<head></head>\\n\\n' + serialize(root, 0) + '\\n</html>';
}"""

_GET_DE_REPLY_JS = """
() => {
    const spans = [...document.querySelectorAll('span')];
    const label  = spans.find(s => s.textContent.trim().includes('Antwort (Deutsch)'));
    if (!label) return '';
    const section = label.closest('div[class*="rounded-2xl"]');
    if (!section) return '';
    const p = section.querySelector('p');
    return p ? p.textContent.trim() : '';
}
"""

# Shown instead of a reply when Chameleon's quality check rejects all of its own
# (internal) generation attempts — the bot has to click 'Antwort generieren' again
# to get it to try a fresh round, see _generate_reply().
_GET_QUALITY_FAILURE_JS = """
() => {
    return (document.body.textContent || '').includes('Keine sichere Antwort erstellt');
}
"""

# Chameleon can also refuse to call the model at all and instead show a red
# "muss MANUELL beantwortet werden" banner (e.g. it thinks the customer is
# attempting an outing/doxxing). Handled the same way as the quality-check
# rejection above: retry generation a few times, see _generate_reply().
_GET_MANUAL_REVIEW_JS = """
() => {
    return (document.body.textContent || '').includes('muss MANUELL beantwortet werden');
}
"""

# Chameleon's backend can also just fail outright, shown as a red "Fehler: <CODE>"
# banner under the generate button (SERVER_ERROR, COST_LIMIT, BAD_MODEL_OUTPUT,
# ... — new codes appear over time, so match the banner generically rather than
# listing known codes). Treated the same as the manual-review banner: retry
# generation, refresh the chat if it won't clear. Returns the matched text (for
# logging) or '' if no error banner is present.
_GET_SERVER_ERROR_JS = """
() => {
    const t = document.body.textContent || '';
    const m = t.match(/Fehler:\\s*\\S+/);
    if (m) return m[0];
    return t.includes('Interner Fehler') ? 'Interner Fehler' : '';
}
"""

_IS_FIRST_CONTACT_JS = """
() => {
    const spans = [...document.querySelectorAll('span')];
    return spans.some(s => s.textContent.trim().includes('FC') && s.textContent.trim().includes('First Contact'));
}
"""

_GET_CHAT_DURATION_JS = """
() => {
    const spans = [...document.querySelectorAll('span')];
    const label = spans.find(s => s.textContent.trim().startsWith('Chatdauer'));
    if (!label) return '';
    const sib = label.nextElementSibling;
    if (sib && sib.textContent.trim()) return sib.textContent.trim();
    if (label.parentElement) {
        const div = label.parentElement.querySelector('div');
        if (div) return div.textContent.trim();
    }
    return '';
}
"""

# The chat thread renders newest-first inside #scrollable-chat-container, and the
# customer's bubbles carry a 'from-male' class (the operator's own replies carry
# 'from-female') — see tab1_html_with_conversation.txt. So the first '[class*="from-male"]'
# match in that container is the customer's most recent message. Used only for the
# stagnant-reply check below (did OUR sent reply actually change the customer's
# last bubble) — the dashboard's displayed "Last Message" comes from Chameleon
# instead, see _GET_LAST_CUSTOMER_MSG_CHAMELEON_JS, since this tab1 heuristic has
# grabbed the wrong bubble on some platforms.
_GET_LAST_CUSTOMER_MSG_JS = """
() => {
    const container = document.querySelector('#scrollable-chat-container');
    if (!container) return '';
    const bubble = container.querySelector('[class*="from-male"]');
    if (!bubble) return '';
    const clone = bubble.cloneNode(true);
    const small = clone.querySelector('small');
    if (small) small.remove();
    return clone.textContent.replace(/\\s+/g, ' ').trim();
}
"""

# Chameleon parses the pasted conversation itself and renders the customer's
# actual last message in a "Letzte Kundennachricht" card (mirrors the
# "Antwort (Deutsch)" card _GET_DE_REPLY_JS reads from) — reading it from here
# instead of guessing at tab1's raw chat-widget DOM is what the dashboard's
# "Last Message" field is meant to show, and doesn't depend on any
# platform-specific bubble class.
_GET_LAST_CUSTOMER_MSG_CHAMELEON_JS = """
() => {
    const spans = [...document.querySelectorAll('span')];
    const label = spans.find(s => s.textContent.trim().toUpperCase().includes('KUNDENNACHRICHT'));
    if (!label) return '';
    const section = label.closest('div[class*="rounded-2xl"]') || label.parentElement;
    if (!section) return '';
    const p = section.querySelector('p');
    return p ? p.textContent.trim() : '';
}
"""


# ── Config ─────────────────────────────────────────────────────────────────────

@dataclass
class BotConfig:
    platform: str       # display name used in all log lines
    cdp_url: str        # e.g. "http://127.0.0.1:9222"
    tab1_pattern: str   # substring matched against open tab URLs
    tab2_pattern: str   # substring matched against AI tab URL

    tab1_url:           str = ""
    username:           str = ""
    password:           str = ""
    chameleon_email:    str = ""
    chameleon_password: str = ""
    chameleon_chat:     str = "Gold XL WM"

    sel_waiting:       str = "h2:has-text('Derzeit sind keine Chatrooms')"
    sel_textarea:      str = "textarea[placeholder*='Schreibe deine Nachricht']"
    sel_send_btn:      str = "#send-message-btn"
    sel_extractor_tab: str = "button[role='tab']:has-text('Extractor')"
    sel_html_textarea: str = "textarea[placeholder*='HTML-Quellcode']"
    sel_extract_btn:   str = "button:has-text('Daten extrahieren')"
    sel_gen_btn:       str = "button:has-text('Antwort generieren')"
    sel_instructions:  str = "textarea[placeholder*='Etwas flirtender']"
    additional_instructions: str = ""
    # Session keep-alive popup ("Bist Du noch online?") that logs the mod out
    # if the confirm button isn't clicked within ~16s of it appearing — see
    # ChatBot._popup_watcher_loop().
    sel_online_confirm_btn: str = "#confirm-online-btn"

    # When True, a chat whose 'Chatdauer' timer reads 00:00 is treated as stuck
    # and the chat page (tab1) is reloaded to recover.
    reload_on_zero_duration: bool = False


# ── Bot ────────────────────────────────────────────────────────────────────────

class ChatBot:
    def __init__(self, config: BotConfig):
        self.cfg = config
        self._stagnant_msg   = None   # customer's last message text, captured at the last cycle start
        self._stagnant_count = 0      # consecutive cycles where that message hasn't changed
        self._current_tab1   = None   # kept up to date whenever tab1 is (re)resolved; read by _popup_watcher_loop

    async def _wait_while_paused(self):
        """Blocks here while the launcher's per-platform pause flag file exists.
        Called at safe checkpoints throughout the cycle so pausing takes effect
        promptly and resuming just continues the same cycle where it left off."""
        flag = pause_flag_path(self.cfg.platform)
        if not flag.exists():
            return
        self.log("[PAUSE] Paused — waiting for 'resume'...")
        while flag.exists():
            await asyncio.sleep(1)
        self.log("[PAUSE] Resumed — continuing.")

    def log(self, msg: str):
        ts = datetime.now().strftime("%H:%M:%S")
        print(f"[{ts}][{self.cfg.platform}] {msg}", flush=True)

    async def _popup_watcher_loop(self):
        """Runs for the whole life of the bot, independent of whatever the main
        cycle is doing. The mod site's 'Bist Du noch online?' keep-alive popup
        can appear at any moment and force-logs the session out ~16s after it
        shows up — far faster than the main loop would notice it if it's busy
        mid-generate, mid-approval-wait, or anywhere else that isn't already
        polling tab1. So this checks independently every few seconds and clicks
        it the instant it's seen, regardless of what the rest of the bot is doing."""
        while True:
            try:
                await asyncio.sleep(3)
                tab1 = self._current_tab1
                if tab1 is None or tab1.is_closed():
                    continue
                btn = tab1.locator(self.cfg.sel_online_confirm_btn)
                if await btn.count() > 0 and await btn.first.is_visible():
                    await btn.first.click()
                    self.log("[POPUP] 'Bist Du noch online?' — clicked confirm.")
            except PlaywrightError as e:
                if _is_fatal(e):
                    return
                # Page mid-navigation or element gone — just retry next tick.
            except Exception:
                pass

    # ── Page / tab helpers ─────────────────────────────────────────────────────

    async def _resolve_tabs(self, context, retries: int = TAB_MAX_RETRIES):
        """
        Re-query tabs from the live context on every call. Identified by URL
        substring so order and count of open pages doesn't matter.
        Returns (tab1, tab2) or (None, None) after all retries are exhausted.
        """
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

    async def _wait_for_page_ready(self, page, state: str = "domcontentloaded", timeout: int = 15_000):
        """Wait for page load state, silently ignoring timeout (page may already be loaded)."""
        try:
            await page.wait_for_load_state(state, timeout=timeout)
        except Exception:
            pass

    async def _is_logged_out(self, tab1) -> bool:
        """True when the mod-site login form is visible (session expired)."""
        try:
            return await tab1.query_selector("input[name='username']") is not None
        except PlaywrightError as e:
            if _is_fatal(e):
                raise
            return False

    async def _is_chameleon_broken(self, tab2) -> bool:
        """True when the chameleon tab is not on AgentWorkspace or is logged out."""
        try:
            if "AgentWorkspace" not in tab2.url:
                return True
            zum_login = tab2.locator("button", has_text="Zum Login")
            return await zum_login.count() > 0
        except PlaywrightError as e:
            if _is_fatal(e):
                raise
            return False

    async def _is_chat_selected(self, tab2) -> bool:
        """True when a chat is chosen in the combobox (not the 'Select chat...' splash).

        This is the 'stuck on select screen' state: the chameleon tab is on the
        workspace and logged in, but no chat is selected so extraction can't run.
        """
        try:
            combobox = tab2.locator("button[role='combobox']")
            if await combobox.count() == 0:
                # No combobox means we're not on the select screen — nothing to fix.
                return True
            combo_text = (await combobox.first.inner_text()).strip()
            return not chat_not_selected(combo_text)
        except PlaywrightError as e:
            if _is_fatal(e):
                raise
            return True

    async def _ensure_chat_selected(self, tab2) -> bool:
        """Recover from the 'Select chat...' stuck screen by picking the configured chat.

        Opens the dropdown and selects self.cfg.chameleon_chat (e.g. 'Gold XL WM'),
        falling back to the first available option. Returns True once a chat is set.
        """
        if await self._is_chat_selected(tab2):
            return True

        chat = self.cfg.chameleon_chat
        self.log(f"[RECOVERY] Stuck on select screen — picking chat '{chat}'...")
        try:
            combobox = tab2.locator("button[role='combobox']")
            await combobox.first.click()
            await asyncio.sleep(0.5)
            # Radix UI renders options in a portal with role="option".
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

    async def _handle_relogin_and_fix_chameleon(self, tab1, tab2):
        """Re-login to the mod site then run a full Chameleon fix."""
        self.log("[RECOVERY] Re-logging in to mod site...")
        await login_mod_site(tab1, self.cfg.tab1_url, self.cfg.username, self.cfg.password, self.cfg.platform)
        self.log("[RECOVERY] Re-login done — running Chameleon fix...")
        await login_chameleon(tab2, self.cfg.chameleon_email, self.cfg.chameleon_password, self.cfg.platform, self.cfg.chameleon_chat)
        self.log("[RECOVERY] Recovery complete — resuming.")

    async def _fix_chameleon_only(self, tab2):
        """Run a full Chameleon re-setup without touching the mod site."""
        self.log("[RECOVERY] Chameleon tab not on workspace — running fix...")
        await login_chameleon(tab2, self.cfg.chameleon_email, self.cfg.chameleon_password, self.cfg.platform, self.cfg.chameleon_chat)
        self.log("[RECOVERY] Chameleon fix done.")

    async def _safe_evaluate(self, page, script: str, retries: int = 3) -> str:
        """
        Evaluate JS with retry on 'Execution context was destroyed'.
        Waits for the page to stabilize between attempts, fixing Diamond's
        main instability cause.
        """
        for attempt in range(retries):
            try:
                return await page.evaluate(script)
            except PlaywrightError as e:
                if _is_context_destroyed(e) and attempt < retries - 1:
                    self.log(
                        f"[WARN] Execution context destroyed — "
                        f"waiting for page to stabilize (attempt {attempt + 1}/{retries - 1})"
                    )
                    await self._wait_for_page_ready(page, "domcontentloaded", 10_000)
                    await asyncio.sleep(2)
                else:
                    raise
        return ""  # unreachable

    # ── Conversation logic ─────────────────────────────────────────────────────

    async def _wait_for_conversation(self, tab1):
        self.log("Waiting for a new conversation...")
        await report_status(self.cfg.platform, "waiting_for_chat")
        t_start     = asyncio.get_event_loop().time()
        last_report = t_start
        while True:
            try:
                # Detect logout before anything else — raises LoggedOutError (not caught below)
                if await self._is_logged_out(tab1):
                    raise LoggedOutError("Mod site session expired while waiting for conversation")

                # Re-query selectors every iteration — never hold element handles
                waiting  = await tab1.query_selector(self.cfg.sel_waiting)
                textarea = await tab1.query_selector(self.cfg.sel_textarea)
                if waiting is None and textarea is not None:
                    waited = asyncio.get_event_loop().time() - t_start
                    self.log(f"Conversation detected! (waited {waited:.0f}s)")
                    await report_status(self.cfg.platform, "chat_detected")
                    return
            except PlaywrightError as e:
                if _is_fatal(e):
                    raise
                if _is_context_destroyed(e):
                    self.log("[WARN] Context reset while polling — waiting for page to stabilise")
                    await self._wait_for_page_ready(tab1)

            now = asyncio.get_event_loop().time()
            if now - last_report >= 30:
                self.log(f"Still idle... ({now - t_start:.0f}s waiting for a conversation)")
                last_report = now

            await asyncio.sleep(POLL_INTERVAL)
            await self._wait_while_paused()

    async def _get_tab1_html(self, tab1) -> str:
        self.log("Capturing page HTML...")
        t0   = asyncio.get_event_loop().time()
        html = await self._safe_evaluate(tab1, _HTML_SERIALIZER_JS)
        elapsed = asyncio.get_event_loop().time() - t0
        if html:
            self.log(f"HTML captured: {len(html):,} chars in {elapsed:.1f}s.")
        else:
            self.log(f"[WARN] HTML capture returned empty string after {elapsed:.1f}s!")
        return html

    async def _ensure_extractor_tab_active(self, tab2):
        try:
            btn = tab2.locator(self.cfg.sel_extractor_tab)
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

        textarea = tab2.locator(self.cfg.sel_html_textarea)
        self.log("Waiting for HTML textarea to be visible...")
        await textarea.wait_for(state="visible", timeout=15_000)
        self.log("Textarea ready — filling HTML...")
        await textarea.click()
        await textarea.fill(html)
        await asyncio.sleep(0.3)
        self.log("Clicking 'Daten extrahieren'...")
        await tab2.locator(self.cfg.sel_extract_btn).click()
        self.log("Extraction triggered — waiting for 'Generate Reply' button...")

    async def _chat_duration_is_zero(self, tab1) -> bool:
        """True when the chat's 'Chatdauer' timer reads 00:00 (stuck chat)."""
        try:
            value = (await self._safe_evaluate(tab1, _GET_CHAT_DURATION_JS)).strip()
            return value == "00:00"
        except PlaywrightError as e:
            if _is_fatal(e):
                raise
            self.log(f"[WARN] Could not read Chatdauer: {e}")
            return False

    async def _is_first_contact(self, tab2) -> bool:
        try:
            result = await self._safe_evaluate(tab2, _IS_FIRST_CONTACT_JS)
            return bool(result)
        except PlaywrightError as e:
            if _is_fatal(e):
                raise
            self.log(f"[WARN] Could not check for First Contact: {e}")
            return False

    async def _wait_for_new_message(self, tab1, sent_html: str) -> bool:
        """
        Blocks after sending until either a new user message appears (True)
        or the conversation closes/goes idle (False).
        """
        self.log("Waiting for next user message or conversation end...")
        await report_status(self.cfg.platform, "idle", "waiting for the customer's next message")
        while True:
            try:
                waiting = await tab1.query_selector(self.cfg.sel_waiting)
                if waiting is not None:
                    self.log("Conversation closed — back to idle.")
                    await report_status(self.cfg.platform, "waiting_for_chat")
                    return False
                current_html = await self._safe_evaluate(tab1, _HTML_SERIALIZER_JS)
                if current_html != sent_html:
                    self.log("New message detected.")
                    return True
            except PlaywrightError as e:
                if _is_fatal(e):
                    raise
                if _is_context_destroyed(e):
                    self.log("[WARN] Context reset while waiting for message — rewaiting")
                    await self._wait_for_page_ready(tab1)
                else:
                    self.log(f"[WARN] Error polling for new message: {e}")
            await asyncio.sleep(POLL_INTERVAL)
            await self._wait_while_paused()

    async def _chat_is_open(self, tab1) -> bool:
        """True while tab1 is still sitting on an open conversation (not back
        on the 'no chatrooms' idle screen). Passed into request_approval() as
        the chat_still_active check so a pending reply gets auto-cancelled if
        the conversation closes while a human hasn't decided yet."""
        try:
            waiting = await tab1.query_selector(self.cfg.sel_waiting)
            return waiting is None
        except PlaywrightError as e:
            if _is_fatal(e):
                raise
            return True  # unknown/transient — don't cancel on a fluke

    async def _generate_reply(self, tab2) -> str:
        old_reply = await self._safe_evaluate(tab2, _GET_DE_REPLY_JS)
        gen_btn   = tab2.locator(self.cfg.sel_gen_btn)
        self.log(f"Waiting up to {EXTRACT_TIMEOUT}s for 'Generate Reply' button...")
        await gen_btn.wait_for(state="visible", timeout=EXTRACT_TIMEOUT * 1_000)
        self.log("'Generate Reply' button visible — clicking...")
        if self.cfg.additional_instructions:
            try:
                instr = tab2.locator(self.cfg.sel_instructions)
                if await instr.count() > 0:
                    await instr.fill(self.cfg.additional_instructions)
                    self.log(f"Additional instructions set: {self.cfg.additional_instructions}")
            except PlaywrightError as e:
                if _is_fatal(e):
                    raise
                self.log(f"[WARN] Could not fill additional instructions: {e}")

        # Chameleon can reject a generation a few different ways:
        #  - its own quality check rejects the generation and shows
        #    "Keine sichere Antwort erstellt" instead of a reply
        #  - it refuses to call the model at all and shows a "... muss MANUELL
        #    beantwortet werden" banner (e.g. an outing/doxxing attempt)
        #  - its backend just fails outright ("Fehler: SERVER_ERROR" /
        #    "Interner Fehler. Bitte erneut versuchen.")
        # Either way we click 'Antwort generieren' again — up to 3 attempts
        # total — before giving up. A persistent quality-check rejection falls
        # through to the normal error handling in run() (restart/reload as
        # usual); a persistent manual-review banner or server error instead
        # raises ManualReviewLimitExceeded so run() can refresh the chat
        # immediately.
        max_attempts = 3
        for attempt in range(1, max_attempts + 1):
            if attempt > 1:
                await gen_btn.wait_for(state="visible", timeout=EXTRACT_TIMEOUT * 1_000)
            await gen_btn.scroll_into_view_if_needed()
            await gen_btn.click()
            self.log(f"'Generate Reply' clicked (attempt {attempt}/{max_attempts}) — "
                     f"polling for AI response (timeout: {GENERATE_TIMEOUT}s)...")

            t_start        = asyncio.get_event_loop().time()
            last_progress  = 0.0
            deadline       = t_start + GENERATE_TIMEOUT
            quality_failed = False
            manual_review  = False
            server_error   = False
            server_error_text = ""

            while asyncio.get_event_loop().time() < deadline:
                try:
                    reply = await self._safe_evaluate(tab2, _GET_DE_REPLY_JS)
                    if reply and reply != old_reply:
                        elapsed = asyncio.get_event_loop().time() - t_start
                        self.log(f"AI reply received in {elapsed:.1f}s: {reply[:80]}{'...' if len(reply) > 80 else ''}")
                        return reply

                    if await self._safe_evaluate(tab2, _GET_QUALITY_FAILURE_JS):
                        quality_failed = True
                        break

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
                    remaining = GENERATE_TIMEOUT - elapsed
                    self.log(f"Still waiting for AI reply... ({elapsed:.0f}s elapsed, {remaining:.0f}s remaining)")
                    last_progress = elapsed

                await asyncio.sleep(1)

            if not quality_failed and not manual_review and not server_error:
                raise RuntimeError(f"Timed out ({GENERATE_TIMEOUT}s) waiting for AI reply.")

            if manual_review:
                self.log(f"[WARN] Chameleon flagged this request for manual review "
                         f"(attempt {attempt}/{max_attempts}) — retrying generation.")
            elif server_error:
                self.log(f"[WARN] Chameleon returned an error banner ({server_error_text}) "
                         f"(attempt {attempt}/{max_attempts}) — retrying generation.")
            else:
                self.log(f"[WARN] Quality check rejected the reply (attempt {attempt}/{max_attempts}): "
                         "'Keine sichere Antwort erstellt'.")
            if attempt < max_attempts:
                await asyncio.sleep(2)

        if manual_review or server_error:
            raise ManualReviewLimitExceeded(
                "Chameleon still won't produce a reply after 3 attempts "
                f"({'manual review' if manual_review else 'server error'})."
            )
        raise RuntimeError(
            "Quality check rejected the AI reply on all 3 attempts "
            "('Keine sichere Antwort erstellt') — giving up."
        )

    async def _get_approved_reply(self, tab1, tab2, customer_message: str) -> tuple[str, str]:
        """Generate a reply, then block on the approval dashboard before it's
        allowed to be sent. A rejection clicks 'Antwort generieren' again for a
        fresh reply and resubmits it — repeats until something is approved.

        ManualReviewLimitExceeded from _generate_reply() is intentionally left
        to propagate — that's a Chameleon-side failure, not a human decision,
        and the caller already knows how to recover from it (refresh the chat).
        ApprovalCancelled also propagates — either a human clicked Cancel on
        the dashboard, or the chat closed while this reply was still pending;
        the caller restarts/redetects rather than looping here.
        """
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
                self.cfg.platform, reply, customer_message=customer_message,
                chat_still_active=lambda: self._chat_is_open(tab1),
            )
            if approved:
                if final_text != reply:
                    self.log("[APPROVAL] Approved with edits.")
                else:
                    self.log("[APPROVAL] Approved.")
                return final_text, req_id
            self.log("[APPROVAL] Rejected — regenerating a new reply...")

    # ── Main entry point ───────────────────────────────────────────────────────

    async def run(self):
        async with async_playwright() as p:
            # ── Connect to Chrome ──────────────────────────────────────────────
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

            # ── Locate both tabs ───────────────────────────────────────────────
            tab1, tab2 = await self._resolve_tabs(context)
            if tab1 is None or tab2 is None:
                self.log("[ERROR] Required tabs not found. Open them in Chrome and restart.")
                sys.exit(0)
            self._current_tab1 = tab1

            # ── Wait for pages to be ready before starting ─────────────────────
            self.log("Waiting for both pages to finish loading...")
            await self._wait_for_page_ready(tab1, "domcontentloaded")
            await self._wait_for_page_ready(tab2, "domcontentloaded")
            self.log("Both pages ready.")
            self.log(f"  Tab1 -> {tab1.url}")
            self.log(f"  Tab2 -> {tab2.url}")
            self.log("Bot running — type 'help' in the launcher for commands.\n")

            # Independent of the main cycle below so it keeps responding to the
            # 'Bist Du noch online?' popup even while the cycle is stuck mid-generate
            # or waiting on a human approval decision — see _popup_watcher_loop().
            asyncio.create_task(self._popup_watcher_loop())

            consecutive_errors = 0
            first_error_time = None
            cycle = 0

            while True:
                try:
                    await self._wait_while_paused()

                    cycle += 1
                    t_cycle = asyncio.get_event_loop().time()
                    self.log(f"──── Cycle {cycle} ────")

                    # Re-acquire tabs each iteration: handles refreshes and URL changes
                    tab1, tab2 = await self._resolve_tabs(context, retries=3)
                    if tab1 is None or tab2 is None:
                        self.log("[WARN] Tabs lost — waiting 10s before retry...")
                        await asyncio.sleep(10)
                        cycle -= 1
                        continue
                    self._current_tab1 = tab1

                    # ── Pre-cycle health checks ────────────────────────────────
                    if await self._is_logged_out(tab1):
                        self.log("[RECOVERY] Logged out from chat — re-logging in + fixing Chameleon...")
                        await self._handle_relogin_and_fix_chameleon(tab1, tab2)
                        cycle -= 1
                        continue

                    if await self._is_chameleon_broken(tab2):
                        self.log("[RECOVERY] Chameleon not on workspace — running fix...")
                        await self._fix_chameleon_only(tab2)

                    # Stuck on the 'Select chat...' screen — pick the chat so extraction works.
                    await self._ensure_chat_selected(tab2)

                    await tab1.bring_to_front()
                    await self._wait_for_page_ready(tab1)
                    await self._wait_for_conversation(tab1)
                    await asyncio.sleep(1.5)

                    # Gold2: a stuck chat shows 'Chatdauer: 00:00' — reload tab1 to recover.
                    if self.cfg.reload_on_zero_duration:
                        reloads = 0
                        while await self._chat_duration_is_zero(tab1) and reloads < 3:
                            reloads += 1
                            self.log(f"[RECOVERY] Chatdauer 00:00 — reloading chat page (attempt {reloads}/3)...")
                            await tab1.reload()
                            await self._wait_for_page_ready(tab1, "domcontentloaded")
                            await self._wait_for_conversation(tab1)
                            await asyncio.sleep(1.5)

                    html = await self._get_tab1_html(tab1)
                    if not html:
                        self.log("[WARN] Empty HTML — skipping this cycle.")
                        cycle -= 1
                        continue

                    # Track whether the customer's last message is still the same one
                    # we replied to last cycle — if so for several cycles in a row, the
                    # reply isn't actually reaching them (checked again right after send).
                    last_customer_msg = await self._safe_evaluate(tab1, _GET_LAST_CUSTOMER_MSG_JS)
                    if last_customer_msg and last_customer_msg == self._stagnant_msg:
                        self._stagnant_count += 1
                        self.log(
                            f"[WARN] Customer's last message unchanged for "
                            f"{self._stagnant_count} cycle(s) in a row — reply may not be sending."
                        )
                    else:
                        self._stagnant_msg = last_customer_msg
                        self._stagnant_count = 1

                    await tab2.bring_to_front()
                    await self._paste_and_extract(tab2, html)

                    # What the dashboard shows as "Last Message" — read from Chameleon's
                    # own "Letzte Kundennachricht" card rather than the tab1 heuristic
                    # above, which has picked the wrong bubble on some platforms. Falls
                    # back to the tab1 value if Chameleon's card isn't found so the
                    # dashboard field is never silently left blank.
                    dashboard_customer_msg = await self._safe_evaluate(tab2, _GET_LAST_CUSTOMER_MSG_CHAMELEON_JS)
                    dashboard_customer_msg = dashboard_customer_msg or last_customer_msg

                    if await self._is_first_contact(tab2):
                        self.log("[FC] First Contact detected — reloading chat and chameleon tabs.")
                        await tab1.reload()
                        await tab2.reload()
                        await self._wait_for_page_ready(tab1, "domcontentloaded")
                        await self._wait_for_page_ready(tab2, "domcontentloaded")
                        # Reloading tab2 drops the chat selection — re-pick it.
                        await self._ensure_chat_selected(tab2)
                        self.log("[FC] Both tabs reloaded — back to idle.")
                        cycle -= 1
                        continue

                    await self._wait_while_paused()
                    try:
                        reply, approval_id = await self._get_approved_reply(tab1, tab2, dashboard_customer_msg)
                    except ManualReviewLimitExceeded:
                        self.log("[RECOVERY] Chameleon kept flagging this request for manual "
                                 "review after 3 attempts — refreshing the chat page.")
                        await tab1.reload()
                        await self._wait_for_page_ready(tab1, "domcontentloaded")
                        await self._wait_for_conversation(tab1)
                        cycle -= 1
                        continue
                    except ApprovalCancelled:
                        self.log("[APPROVAL] Cancelled — chat closed or operator cancelled it. "
                                 "Reloading the chat page and redetecting...")
                        await tab1.reload()
                        await self._wait_for_page_ready(tab1, "domcontentloaded")
                        await self._wait_for_conversation(tab1)
                        cycle -= 1
                        continue

                    await report_status(self.cfg.platform, "sending")
                    await tab1.bring_to_front()
                    textarea = tab1.locator(self.cfg.sel_textarea)
                    await textarea.click()
                    await textarea.fill(reply)
                    wait = random.randint(15, 20)
                    self.log(f"Reply pasted ({len(reply)} chars) — sending in {wait}s...")
                    await asyncio.sleep(wait)
                    # Never send a message while paused, even if pause was toggled
                    # mid-delay — the reply stays pasted and goes out once resumed.
                    await self._wait_while_paused()
                    try:
                        await tab1.locator(self.cfg.sel_send_btn).click()
                    except Exception as e:
                        await mark_failed(approval_id, str(e))
                        raise
                    self.log(f"Sent: {reply[:80]}{'...' if len(reply) > 80 else ''}")
                    log_sent_message(self.cfg.platform, reply)
                    await mark_sent(approval_id)
                    await asyncio.sleep(2)

                    if self._stagnant_count >= STAGNANT_CYCLES_BEFORE_RELOGIN:
                        self.log(
                            f"[RECOVERY] Pasted and sent {self._stagnant_count} times without "
                            "the conversation moving forward — logging out and back in before resuming..."
                        )
                        self._stagnant_count = 0
                        self._stagnant_msg = None
                        await self._handle_relogin_and_fix_chameleon(tab1, tab2)
                        if consecutive_errors > 0:
                            self.log(f"Consecutive error counter reset (was {consecutive_errors}).")
                        consecutive_errors = 0
                        first_error_time = None
                        cycle -= 1
                        continue

                    snapshot = await self._safe_evaluate(tab1, _HTML_SERIALIZER_JS)
                    conversation_continued = await self._wait_for_new_message(tab1, snapshot)

                    if consecutive_errors > 0:
                        self.log(f"Consecutive error counter reset (was {consecutive_errors}).")
                    consecutive_errors = 0
                    first_error_time = None

                    elapsed = asyncio.get_event_loop().time() - t_cycle
                    self.log(f"──── Cycle {cycle} complete in {elapsed:.1f}s ────\n")

                    if not conversation_continued:
                        self.log("Conversation ended — back to idle.")

                except KeyboardInterrupt:
                    self.log("Stopped by user.")
                    sys.exit(0)

                except LoggedOutError:
                    self.log("[RECOVERY] Logged out during idle — re-logging in + fixing Chameleon...")
                    await report_status(self.cfg.platform, "recovering", "logged out — re-logging in")
                    await self._handle_relogin_and_fix_chameleon(tab1, tab2)
                    if consecutive_errors > 0:
                        self.log(f"Consecutive error counter reset (was {consecutive_errors}).")
                    consecutive_errors = 0
                    first_error_time = None
                    cycle -= 1
                    continue

                except PlaywrightTimeout as e:
                    consecutive_errors += 1
                    if first_error_time is None:
                        first_error_time = asyncio.get_event_loop().time()
                    error_elapsed = asyncio.get_event_loop().time() - first_error_time
                    self.log(
                        f"[WARN] Timeout ({type(e).__name__}): {e} — "
                        f"retrying in 15s (error {consecutive_errors}, {error_elapsed:.0f}s since first error)"
                    )
                    await report_status(self.cfg.platform, "error", str(e)[:200])
                    await asyncio.sleep(15)
                    if error_elapsed >= ERROR_RELOGIN_AFTER:
                        self.log(f"[RECOVERY] Reply still won't send after {error_elapsed:.0f}s — "
                                 f"logging out and back in...")
                        await self._handle_relogin_and_fix_chameleon(tab1, tab2)
                        consecutive_errors = 0
                        first_error_time = None
                        cycle -= 1
                        continue

                except Exception as e:
                    if _is_fatal(e):
                        self.log(f"[FATAL] Browser disconnected: {e}")
                        sys.exit(1)

                    consecutive_errors += 1
                    if first_error_time is None:
                        first_error_time = asyncio.get_event_loop().time()
                    error_elapsed = asyncio.get_event_loop().time() - first_error_time
                    self.log(
                        f"[ERROR] {type(e).__name__}: {e} "
                        f"(error {consecutive_errors}, {error_elapsed:.0f}s since first error) — retrying in 15s..."
                    )
                    await report_status(self.cfg.platform, "error", str(e)[:200])
                    await asyncio.sleep(15)
                    if error_elapsed >= ERROR_RELOGIN_AFTER:
                        self.log(f"[RECOVERY] Reply still won't send after {error_elapsed:.0f}s — "
                                 f"logging out and back in...")
                        await self._handle_relogin_and_fix_chameleon(tab1, tab2)
                        consecutive_errors = 0
                        first_error_time = None
                        cycle -= 1
                        continue
