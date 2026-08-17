#!/usr/bin/env python3
"""
Diagnose why the xkuss bot 'sees the chat but does nothing'.

Run this while Chrome is up on port 9227 with BOTH tabs open:
  - tab1: an OPEN xkuss dialog (old_postfach.php?action=read...)
  - tab2: the chameleon workspace (chamaleon-ai .../AgentWorkspace)

It reproduces the exact extract step the bot does and reports where it stalls:
  1. captures tab1 HTML with the shared serializer
  2. confirms the conversation text actually made it into that HTML
  3. checks the chameleon tab state (workspace? chat selected?)
  4. pastes + clicks 'Daten extrahieren'
  5. reports whether 'Generate Reply' appears  -> extraction OK
     and whether _IS_FIRST_CONTACT_JS fires    -> false-positive FC loop

    python tools/diagnose_xkuss.py
"""
import asyncio
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from playwright.async_api import async_playwright
from core.bot import _HTML_SERIALIZER_JS, _IS_FIRST_CONTACT_JS, _GET_DE_REPLY_JS

CDP = "http://127.0.0.1:9227"
SEL_EXTRACTOR_TAB = "button[role='tab']:has-text('Extractor')"
SEL_HTML_TEXTAREA = "textarea[placeholder*='HTML-Quellcode']"
SEL_EXTRACT_BTN   = "button:has-text('Daten extrahieren')"
SEL_GEN_BTN       = "button:has-text('Antwort generieren')"
SEL_COMBOBOX      = "button[role='combobox']"


def p(msg): print(msg, flush=True)


async def main():
    async with async_playwright() as pw:
        b = await pw.chromium.connect_over_cdp(CDP)
        ctx = b.contexts[0]
        tab1 = next((pg for pg in ctx.pages if "xkuss" in pg.url), None)
        tab2 = next((pg for pg in ctx.pages if "chamaleon-ai" in pg.url), None)
        if not tab1 or not tab2:
            p(f"[FAIL] tabs missing. xkuss={bool(tab1)} chameleon={bool(tab2)}")
            for pg in ctx.pages:
                p(f"   open -> {pg.url[:90]}")
            return

        p(f"tab1 (xkuss)     -> {tab1.url[:90]}")
        p(f"tab2 (chameleon) -> {tab2.url[:90]}\n")

        # 1+2. capture HTML and confirm the conversation is inside it
        p("── STEP 1: capture xkuss HTML ──")
        html = await tab1.evaluate(_HTML_SERIALIZER_JS)
        p(f"serialized HTML: {len(html):,} chars")
        showpm = await tab1.evaluate(
            "() => { const e=document.querySelector('#showpm');"
            "return e ? e.innerText.replace(/\\s+/g,' ').trim() : ''; }"
        )
        p(f"#showpm text: {len(showpm)} chars -> {showpm[:120]!r}")
        # does the message text survive into the serialized HTML the extractor sees?
        probe = showpm[:40].strip()
        p(f"conversation text present in serialized HTML: "
          f"{'YES' if probe and probe in html else 'NO  <-- extractor gets no messages'}\n")

        # 3. chameleon state
        p("── STEP 2: chameleon tab state ──")
        combo = tab2.locator(SEL_COMBOBOX)
        combo_txt = (await combo.first.inner_text()).strip() if await combo.count() else "(no combobox)"
        p(f"selected chat: {combo_txt!r}")
        fc_before = await tab2.evaluate(_IS_FIRST_CONTACT_JS)
        p(f"_IS_FIRST_CONTACT before extract: {fc_before} "
          f"{'<-- already true, bot would skip to Home' if fc_before else ''}\n")

        # 4. paste + extract
        p("── STEP 3: paste HTML + Daten extrahieren ──")
        ext_tab = tab2.locator(SEL_EXTRACTOR_TAB)
        if await ext_tab.count() and await ext_tab.get_attribute("data-state") != "active":
            await ext_tab.click(); await asyncio.sleep(0.5)
        ta = tab2.locator(SEL_HTML_TEXTAREA)
        await ta.wait_for(state="visible", timeout=15_000)
        await ta.click(); await ta.fill(html); await asyncio.sleep(0.3)
        await tab2.locator(SEL_EXTRACT_BTN).click()
        p("clicked 'Daten extrahieren' — waiting up to 30s for 'Generate Reply'...")

        # 5. did extraction produce a Generate Reply button?
        gen = tab2.locator(SEL_GEN_BTN)
        try:
            await gen.wait_for(state="visible", timeout=30_000)
            p("[OK] 'Generate Reply' appeared -> extraction WORKS.")
        except Exception:
            p("[FAIL] 'Generate Reply' never appeared -> extractor did not parse the "
              "xkuss HTML. This is why the bot 'does nothing'.")
        fc_after = await tab2.evaluate(_IS_FIRST_CONTACT_JS)
        p(f"_IS_FIRST_CONTACT after extract: {fc_after} "
          f"{'<-- bot reloads + goes Home, never replies' if fc_after else ''}")


asyncio.run(main())
