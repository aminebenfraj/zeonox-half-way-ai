"""Browser maintenance operations shared by web and terminal controls."""

from importlib import import_module

from playwright.async_api import TimeoutError as PlaywrightTimeout, async_playwright

from core.login import check_chameleon, force_extractor_tab, login_chameleon


async def _dismiss_message_box(page, *, timeout: int, required: bool) -> bool:
    """Press the exact visible ExtJS OK button and verify its dialog closes."""
    dialog = page.locator("div.x-message-box:visible").last
    try:
        await dialog.wait_for(state="visible", timeout=timeout)
    except PlaywrightTimeout as error:
        if not required:
            return False
        raise RuntimeError("Pools was clicked but its message dialog did not appear") from error

    ok_inner = dialog.locator('span.x-btn-inner:text-is("OK")').last
    try:
        await ok_inner.wait_for(state="visible", timeout=5_000)
        ok_button = ok_inner.locator('xpath=ancestor::a[@role="button"][1]')
        await ok_button.click(force=True, timeout=5_000)
        await dialog.wait_for(state="hidden", timeout=5_000)
    except PlaywrightTimeout as error:
        raise RuntimeError(
            "Pools message appeared, but its OK button was not dismissed"
        ) from error
    return True


async def run_browser_operation(command: str, platforms: list[str]) -> str:
    """Run a bounded Chameleon maintenance command and return UI-safe output."""
    if command not in {"fix", "chameleon", "extractor", "pools"}:
        raise ValueError(f"Unsupported browser operation: {command}")

    output: list[str] = []
    async with async_playwright() as playwright:
        for slug in platforms:
            cfg = import_module(f"configs.{slug}").config
            try:
                browser = await playwright.chromium.connect_over_cdp(cfg.cdp_url)
                context = browser.contexts[0]
                if command == "pools":
                    if slug not in {"justlo", "linduu", "gnoxx"}:
                        raise ValueError("Pools is available only for Justlo, Linduu, and Gnoxx")
                    platform_tabs = [
                        page for page in context.pages if cfg.tab1_pattern in page.url
                    ]
                    tab = next(
                        (page for page in platform_tabs if "community-mod" in page.url),
                        platform_tabs[0] if platform_tabs else None,
                    )
                    if tab is None:
                        tab = await context.new_page()
                        await tab.goto(cfg.mod_url, wait_until="domcontentloaded", timeout=30_000)
                    await tab.bring_to_front()
                    pools = tab.locator("#buttonPools-btnInnerEl")
                    if await pools.count() == 0:
                        await tab.goto(cfg.mod_url, wait_until="domcontentloaded", timeout=30_000)
                        pools = tab.locator("#buttonPools-btnInnerEl")
                    await pools.wait_for(state="visible", timeout=15_000)
                    # Recover from a dialog left open by an earlier attempt;
                    # otherwise its modal overlay prevents the next Pools click.
                    await _dismiss_message_box(tab, timeout=500, required=False)
                    await pools.click(timeout=10_000)
                    # Linduu may title the result "Erfolg" while another
                    # platform/response uses "Fehlermeldung". The stable part
                    # is the visible x-message-box and its exact OK control.
                    await _dismiss_message_box(tab, timeout=15_000, required=True)

                    output.append(
                        f"[{cfg.platform}] Pools opened and OK was confirmed; "
                        "bot can resume from the queue."
                    )
                    continue

                tab = next(
                    (page for page in context.pages if cfg.tab2_pattern in page.url),
                    None,
                )

                if command == "fix":
                    if tab is None:
                        tab = await context.new_page()
                        output.append(f"[{cfg.platform}] Opened a new Chameleon tab.")
                    await login_chameleon(
                        tab,
                        cfg.chameleon_email,
                        cfg.chameleon_password,
                        cfg.platform,
                        cfg.chameleon_chat,
                    )
                    output.append(f"[{cfg.platform}] Fix completed: {tab.url}")
                elif tab is None:
                    output.append(f"[{cfg.platform}] Chameleon tab was not found.")
                elif command == "chameleon":
                    await check_chameleon(tab, cfg.platform)
                    output.append(f"[{cfg.platform}] Chameleon check completed.")
                else:
                    await force_extractor_tab(tab, cfg.platform)
                    output.append(f"[{cfg.platform}] Extractor tab activated.")
            except Exception as error:
                if command == "pools":
                    # A Pools recovery must be all-or-nothing. Let the web API
                    # return an error so it never clears Pause or claims resume
                    # before OK was actually pressed and the overlay closed.
                    raise
                output.append(f"[{cfg.platform}] {command.title()} failed: {error}")

    return "\n".join(output) or "No running platforms matched the command."
