#!/usr/bin/env python3
"""Read-only live audit for Justlo-family conversation detection.

Connects to each configured Chrome debugging port and reports the exact DOM
signals used by ``classify_page`` and ``_conversation_sig``.  It deliberately
does not click Play, navigate, type, or otherwise mutate the live sessions.
"""

import argparse
import asyncio
import json
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from dotenv import load_dotenv
from playwright.async_api import async_playwright

from core.justlo_login import classify_page


PLATFORMS = {
    "linduu": "configs.linduu",
    "linduu_us": "configs.linduu_us",
    "gnoxx": "configs.gnoxx",
    "justlo_fr": "configs.justlo_fr",
}


def load_config(module_name):
    module = __import__(module_name, fromlist=["config"])
    return module.config


async def audit_platform(playwright, slug):
    cfg = load_config(PLATFORMS[slug])
    result = {"slug": slug, "platform": cfg.platform, "cdp_url": cfg.cdp_url}
    try:
        browser = await playwright.chromium.connect_over_cdp(cfg.cdp_url)
    except Exception as error:
        result["error"] = f"CDP connection failed: {error}"
        return result

    try:
        pages = [page for context in browser.contexts for page in context.pages]
        result["pages"] = [
            {"url": page.url, "title": await page.title()} for page in pages
        ]
        candidates = [
            page for page in pages
            if cfg.tab1_pattern.casefold() in page.url.casefold()
        ]
        if not candidates:
            result["error"] = f"No platform page matching {cfg.tab1_pattern!r}"
            return result

        page = next(
            (candidate for candidate in candidates if "community-mod" in candidate.url.casefold()),
            candidates[0],
        )
        result["selected_url"] = page.url
        result["phase"] = await classify_page(page, cfg)
        result["signals"] = await page.evaluate(
            """(s) => {
                const text = (selector) => {
                    const element = document.querySelector(selector);
                    return element ? (element.textContent || '').trim() : '';
                };
                const info = (selector) => {
                    const element = document.querySelector(selector);
                    if (!element) return {exists: false};
                    const style = window.getComputedStyle(element);
                    return {
                        exists: true,
                        text: (element.textContent || '').trim(),
                        className: String(element.className || ''),
                        ariaDisabled: element.getAttribute('aria-disabled'),
                        display: style.display,
                        visibility: style.visibility,
                    };
                };
                const configuredGrid = document.querySelector(s.grid);
                const allGridBodies = [...document.querySelectorAll('[id^="gridview-"][id$="-body"]')];
                const gridBodies = allGridBodies.map((grid) => ({
                    id: grid.id,
                    rows: grid.querySelectorAll('.x-grid-row').length,
                    text: (grid.textContent || '').trim().slice(0, 240),
                    inConversationPanel: !!grid.closest('#panel-1012-body'),
                    ancestors: (() => {
                        const values = [];
                        let node = grid.parentElement;
                        for (let depth = 0; node && depth < 6; depth += 1, node = node.parentElement) {
                            values.push({
                                tag: node.tagName,
                                id: node.id || '',
                                className: String(node.className || ''),
                                title: node.querySelector(':scope > .x-panel-header .x-header-text, :scope > .x-grid-header-ct')?.textContent?.trim().slice(0, 160) || '',
                            });
                        }
                        return values;
                    })(),
                }));
                const queue = document.querySelector(s.queue);
                const queueStyle = queue ? window.getComputedStyle(queue) : null;
                const queueText = queue ? (queue.textContent || '').trim() : '';
                const client = text(s.client);
                const configuredRows = configuredGrid
                    ? configuredGrid.querySelectorAll('.x-grid-row').length
                    : 0;
                return {
                    readyState: document.readyState,
                    client,
                    fake: text(s.fake),
                    queueText,
                    queueVisible: !!queueStyle && queueStyle.display !== 'none' && queueStyle.visibility !== 'hidden',
                    configuredGridExists: !!configuredGrid,
                    configuredRows,
                    configuredGridText: configuredGrid ? (configuredGrid.textContent || '').trim().slice(0, 500) : '',
                    play: info(s.play),
                    pause: info(s.pause),
                    textarea: info(s.textarea),
                    send: info(s.send),
                    gridBodies,
                    visibleRowsAnywhere: [...document.querySelectorAll('.x-grid-row')].filter((row) => {
                        const style = window.getComputedStyle(row);
                        return style.display !== 'none' && style.visibility !== 'hidden';
                    }).length,
                    detectorWouldSeeChat: !!client && (
                        configuredRows > 0 || (
                            !!queueStyle && queueStyle.display !== 'none' &&
                            queueStyle.visibility !== 'hidden' && !!queueText &&
                            queueText !== 'Keine Beschreibung vorhanden'
                        )
                    ),
                };
            }""",
            {
                "client": cfg.sel_client_username,
                "fake": cfg.sel_fake_username,
                "queue": cfg.sel_queue_message,
                "grid": cfg.sel_conv_grid,
                "play": cfg.sel_play_btn,
                "pause": cfg.sel_pause_btn,
                "textarea": cfg.sel_textarea,
                "send": cfg.sel_send_btn,
            },
        )
        return result
    except Exception as error:
        result["error"] = f"Audit failed: {error}"
        return result
    finally:
        await browser.close()


async def main(slugs):
    async with async_playwright() as playwright:
        results = []
        for slug in slugs:
            results.append(await audit_platform(playwright, slug))
    print(json.dumps(results, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    if hasattr(sys.stdout, "reconfigure"):
        sys.stdout.reconfigure(encoding="utf-8")
    load_dotenv()
    parser = argparse.ArgumentParser()
    parser.add_argument("platforms", nargs="*", choices=tuple(PLATFORMS))
    args = parser.parse_args()
    asyncio.run(main(args.platforms or list(PLATFORMS)))
