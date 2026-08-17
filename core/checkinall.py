"""checkinall — read 'Meine Statistiken' for every running bot, work out how
much money is being made, and write/update a plain-text note on the Desktop.

Money model (per the operator's rates). All monetary values are in Tunisian
dinar (DT); FX (3.4) is the dinar conversion factor, not a dollar rate:
    In      is worth  0.15 * 3.4  =  0.51 DT each
    ASA Out is worth  0.05 * 3.4  =  0.17 DT each

Daily earnings are estimated from the *delta* since the last time 'checkinall'
was run: a timestamped snapshot of the grand total is kept in a state file, and
    daily ≈ (money_now - money_last_run) / days_elapsed_since_last_run
The very first run has no baseline, so it just records one and reports n/a.
"""
from __future__ import annotations

import json
from datetime import datetime
from importlib import import_module
from pathlib import Path

from playwright.async_api import async_playwright

from core.login import check_stats

# ── rates ──────────────────────────────────────────────────────────────────────
IN_RATE      = 0.15
ASA_OUT_RATE = 0.05
FX           = 3.4

IN_VALUE      = IN_RATE * FX        # DT per In      → 0.51 DT
ASA_OUT_VALUE = ASA_OUT_RATE * FX   # DT per ASA Out → 0.17 DT

# ── file locations ─────────────────────────────────────────────────────────────
NOTE_PATH  = Path.home() / "Desktop" / "checkinall.txt"
STATE_PATH = Path(__file__).resolve().parent.parent / ".checkinall_state.json"


def _money(ins: int, asa_outs: int) -> float:
    return ins * IN_VALUE + asa_outs * ASA_OUT_VALUE


def _load_state() -> dict | None:
    try:
        return json.loads(STATE_PATH.read_text(encoding="utf-8"))
    except Exception:
        return None


def _build_note(rows: list[tuple[str, int, int, bool]]) -> str:
    """rows: list of (platform, ins, asa_outs, ok). ``ok`` is True when the
    account was open and its stats were read this run; those values are used and
    saved. When ``ok`` is False the account was not open — instead of counting it
    as 0, we keep its **last known** values from the previous snapshot (so closed
    accounts still contribute their most recent numbers to the totals and are not
    updated). Returns the full note text and, as a side effect, writes it to disk
    and snapshots the merged per-account values."""
    now = datetime.now()

    prev = _load_state()
    prev_accounts: dict = (prev or {}).get("per_account", {})
    # Start the new snapshot from the previous per-account values; only accounts
    # that are open this run overwrite their entry below.
    new_accounts: dict = dict(prev_accounts)

    grand_ins = grand_asa = 0
    grand_money = 0.0
    read_count = 0
    account_lines = []
    for plat, ins, asa, ok in rows:
        if ok:
            read_count += 1
            new_accounts[plat] = {"ins": ins, "asa_outs": asa}
            note = ""
        else:
            known = prev_accounts.get(plat)
            if not known:
                # Not open and never recorded before — nothing to keep.
                account_lines.append(
                    f"  {plat:<10s} (not open — no prior value, nothing to update)"
                )
                continue
            ins = int(known.get("ins", 0))
            asa = int(known.get("asa_outs", 0))
            note = "   (not open — last known value kept)"
        money = _money(ins, asa)
        grand_ins   += ins
        grand_asa   += asa
        grand_money += money
        account_lines.append(
            f"  {plat:<10s} Ins={ins:<6d} ({ins * IN_VALUE:>9.2f} DT)   "
            f"ASA Outs={asa:<6d} ({asa * ASA_OUT_VALUE:>9.2f} DT)   "
            f"= {money:,.2f} DT{note}"
        )

    # ── daily estimate from the previous snapshot ──────────────────────────────
    daily_line = "  Daily estimate : n/a (first run — baseline saved)"
    if prev and "money" in prev and "timestamp" in prev:
        try:
            prev_time    = datetime.fromisoformat(prev["timestamp"])
            elapsed_days = (now - prev_time).total_seconds() / 86400.0
            delta_money  = grand_money - float(prev["money"])
            if elapsed_days > 0:
                per_day = delta_money / elapsed_days
                daily_line = (
                    f"  Daily estimate : ~{per_day:,.2f} DT/day  "
                    f"(+{delta_money:,.2f} DT over {elapsed_days:.2f} day(s) "
                    f"since {prev_time:%Y-%m-%d %H:%M})"
                )
        except Exception:
            pass

    sep = "=" * 64
    text = (
        f"CHECK-IN ALL — {now:%Y-%m-%d %H:%M:%S}\n"
        f"{sep}\n"
        f"Rates: In = 0.15 × 3.4 = {IN_VALUE:.2f} DT each   |   "
        f"ASA Out = 0.05 × 3.4 = {ASA_OUT_VALUE:.2f} DT each\n"
        f"Accounts listed: {len(rows)}   (stats read from {read_count})\n\n"
        f"Per account:\n"
        + ("\n".join(account_lines) if account_lines else "  (no stats could be read)")
        + "\n\n"
        f"Totals:\n"
        f"  Ins      : {grand_ins:<8d} → {grand_ins * IN_VALUE:,.2f} DT\n"
        f"  ASA Outs : {grand_asa:<8d} → {grand_asa * ASA_OUT_VALUE:,.2f} DT\n"
        f"  TOTAL    : {grand_money:,.2f} DT\n\n"
        f"{daily_line}\n\n"
        f"{sep}\n"
        f"(updated every time you type 'checkinall')\n"
    )

    # ── persist note + new snapshot ────────────────────────────────────────────
    try:
        NOTE_PATH.parent.mkdir(parents=True, exist_ok=True)
        NOTE_PATH.write_text(text, encoding="utf-8")
    except Exception as exc:
        text += f"\n[WARN] could not write note to {NOTE_PATH}: {exc}\n"

    try:
        STATE_PATH.write_text(
            json.dumps(
                {
                    "timestamp": now.isoformat(),
                    "money": grand_money,
                    "ins": grand_ins,
                    "asa_outs": grand_asa,
                    "accounts": [r[0] for r in rows],
                    # Per-account last-known values: open accounts refreshed this
                    # run, closed ones carried over so they are never reset to 0.
                    "per_account": new_accounts,
                },
                indent=2,
            ),
            encoding="utf-8",
        )
    except Exception:
        pass

    return text


async def gather_and_write(platform_names: list[str]) -> tuple[str, Path]:
    """Connect to every platform's Chrome, read its stats, then write the note.

    Returns (note_text, note_path). Platforms whose tab/stats can't be read are
    skipped (with a printed warning) rather than aborting the whole report.
    """
    rows: list[tuple[str, int, int, bool]] = []
    async with async_playwright() as p:
        for name in platform_names:
            label = name.upper()
            try:
                cfg = import_module(f"configs.{name}").config
                label = cfg.platform
                browser = await p.chromium.connect_over_cdp(cfg.cdp_url)
                context = browser.contexts[0]
                tab = next(
                    (pg for pg in context.pages if cfg.tab1_pattern in pg.url),
                    None,
                )
                if tab is None:
                    print(f"[{label}] Mod-site tab not found — listed as unavailable.", flush=True)
                    rows.append((label, 0, 0, False))
                    continue
                stats = await check_stats(tab, cfg.platform)
                if not stats:
                    rows.append((label, 0, 0, False))
                    continue
                ins      = stats.get("Ins", 0)
                asa_outs = stats.get("ASA Outs", 0)
                rows.append((label, ins, asa_outs, True))
            except Exception as exc:
                print(f"[{label}] Could not read stats: {exc}", flush=True)
                rows.append((label, 0, 0, False))

    text = _build_note(rows)
    return text, NOTE_PATH
