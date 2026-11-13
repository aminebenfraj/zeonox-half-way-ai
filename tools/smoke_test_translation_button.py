#!/usr/bin/env python3
"""End-to-end smoke test for the dashboard card translation button."""

import asyncio
import sys
from pathlib import Path

import httpx
from playwright.async_api import async_playwright


BASE_URL = "http://127.0.0.1:8799"
PROJECT_ROOT = Path(__file__).resolve().parents[1]


async def main():
    request_id = None
    async with httpx.AsyncClient(base_url=BASE_URL, timeout=45.0) as client:
        try:
            created = await client.post(
                "/api/requests",
                json={
                    "platform": "Translation Button Test",
                    "reply": "Dies ist ein sicherer Testtext.",
                    "customer_message": "Hallo",
                },
            )
            created.raise_for_status()
            request_id = created.json()["id"]

            async with async_playwright() as playwright:
                browser = await playwright.chromium.launch(headless=True)
                try:
                    page = await browser.new_page()
                    await page.goto(BASE_URL, wait_until="domcontentloaded")
                    card = page.locator(f"#approval-{request_id}")
                    await card.wait_for(state="visible", timeout=15_000)

                    english = page.locator(f"#en-ta-{request_id}")
                    target = page.locator(f"#ta-{request_id}")
                    await english.fill("Hello, how are you today?")
                    before = await target.input_value()
                    await card.locator(".btn-translate").click()
                    await page.wait_for_function(
                        """({id, before}) => {
                            const input = document.getElementById(`ta-${id}`);
                            return input && input.value && input.value !== before;
                        }""",
                        arg={"id": request_id, "before": before},
                        timeout=60_000,
                    )
                    after = await target.input_value()
                    print(f"PASS: {before!r} -> {after!r}")
                finally:
                    await browser.close()
        finally:
            if request_id:
                await client.post(f"/api/requests/{request_id}/cancel", json={})


if __name__ == "__main__":
    if hasattr(sys.stdout, "reconfigure"):
        sys.stdout.reconfigure(encoding="utf-8")
    asyncio.run(main())
