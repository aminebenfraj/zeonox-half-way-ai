"""Collect every calculator variable from the live platform tabs.

The dashboard's ``Check-in All`` button is the automated version of
``calculator.html``:

    goldins
    + (xkussins * 0.20 * 3.4)
    + ((justloins + linduuins + gnoxxins) * 0.17 * 3.4)
    + 200

``goldins`` is the already-converted subtotal from the React/Gold-family
accounts (their Ins and ASA Outs use the existing 0.15/0.05 rates). Xkuss is
read from the ``oldview_inout.php`` account link. Justlo, Linduu, and Gnoxx are
read from the ``Im Monat`` counter in their shared ExtJS moderation console.

Missing counters are reported as UNAVAILABLE and never silently replaced with
zero or an old value. In that case the report shows a known subtotal and marks
the final result incomplete.
"""
from __future__ import annotations

import json
import re
from datetime import datetime
from importlib import import_module
from pathlib import Path

from playwright.async_api import async_playwright

from core.login import check_stats

# React/Gold-family rates used to produce the calculator's direct ``goldins``
# component. The three self-managed platform rates mirror calculator.html.
IN_RATE = 0.15
ASA_OUT_RATE = 0.05
XKUSS_RATE = 0.20
SHARED_RATE = 0.17
FX = 3.4
FIXED_BONUS = 200.0

IN_VALUE = IN_RATE * FX
ASA_OUT_VALUE = ASA_OUT_RATE * FX
XKUSS_VALUE = XKUSS_RATE * FX
SHARED_VALUE = SHARED_RATE * FX

FORMULA_VERSION = 2
NOTE_PATH = Path.home() / "Desktop" / "checkinall.txt"
STATE_PATH = Path(__file__).resolve().parent.parent / ".checkinall_state.json"

REACT_NAMES = {"gold", "gold2", "diamond", "platin", "s69", "ml"}
SHARED_NAMES = {"justlo", "linduu", "gnoxx"}


def _react_money(ins: int, asa_outs: int) -> float:
    return ins * IN_VALUE + asa_outs * ASA_OUT_VALUE


def _parse_counter(value: object) -> int | None:
    """Parse a displayed integer counter while tolerating thousands separators."""
    if value is None:
        return None
    match = re.search(r"\d[\d\s.,]*", str(value).replace("\xa0", " "))
    if not match:
        return None
    digits = re.sub(r"\D", "", match.group(0))
    return int(digits) if digits else None


def _parse_xkuss_ins(text: str) -> int | None:
    match = re.search(r"\bINs?\b\s*:\s*(\d[\d\s.,]*)", text or "", re.I)
    return _parse_counter(match.group(1)) if match else None


def _load_state() -> dict | None:
    try:
        return json.loads(STATE_PATH.read_text(encoding="utf-8"))
    except Exception:
        return None


def _row_unavailable(label: str, name: str, kind: str, reason: str) -> dict:
    return {
        "platform": label,
        "name": name,
        "kind": kind,
        "available": False,
        "reason": reason,
    }


def _build_note(rows: list[dict]) -> str:
    now = datetime.now()
    missing = [row["platform"] for row in rows if not row.get("available")]

    gold_rows = [row for row in rows if row["kind"] == "react"]
    gold_money = sum(float(row.get("money", 0)) for row in gold_rows if row.get("available"))
    gold_missing = [row["platform"] for row in gold_rows if not row.get("available")]

    xkuss_row = next((row for row in rows if row["kind"] == "xkuss"), None)
    xkussins = int(xkuss_row["ins"]) if xkuss_row and xkuss_row.get("available") else None

    shared_rows = {row["name"]: row for row in rows if row["kind"] == "shared"}
    shared_values = {
        name: int(shared_rows[name]["ins"])
        if name in shared_rows and shared_rows[name].get("available") else None
        for name in ("justlo", "linduu", "gnoxx")
    }

    known_total = gold_money + FIXED_BONUS
    if xkussins is not None:
        known_total += xkussins * XKUSS_VALUE
    known_total += sum(value * SHARED_VALUE for value in shared_values.values() if value is not None)
    complete = not missing

    account_lines: list[str] = []
    for row in rows:
        label = row["platform"]
        if not row.get("available"):
            account_lines.append(f"  {label:<10s} UNAVAILABLE — {row.get('reason') or 'counter not found'}")
        elif row["kind"] == "react":
            account_lines.append(
                f"  {label:<10s} Ins={row['ins']:<6d}  ASA Outs={row['asa_outs']:<6d}  "
                f"→ {row['money']:,.2f} DT"
            )
        elif row["kind"] == "xkuss":
            account_lines.append(
                f"  {label:<10s} INs={row['ins']:<6d}  × 0.20 × 3.4  → {row['money']:,.2f} DT"
            )
        else:
            account_lines.append(
                f"  {label:<10s} Im Monat={row['ins']:<6d}  × 0.17 × 3.4  → {row['money']:,.2f} DT"
            )

    def variable_line(label: str, value: int | None, rate: float) -> str:
        if value is None:
            return f"  {label:<12s}: UNAVAILABLE"
        return f"  {label:<12s}: {value:<8d} × {rate:.2f} × {FX:.1f} = {value * rate * FX:,.2f} DT"

    if gold_rows and len(gold_missing) == len(gold_rows):
        gold_line = f"  {'goldins':<12s}: UNAVAILABLE"
    elif gold_missing:
        gold_line = (
            f"  {'goldins':<12s}: PARTIAL {gold_money:,.2f} DT "
            f"(missing {', '.join(gold_missing)})"
        )
    else:
        gold_line = f"  {'goldins':<12s}: {gold_money:,.2f} DT"

    variable_lines = [
        gold_line,
        variable_line("xkussins", xkussins, XKUSS_RATE),
        variable_line("justloins", shared_values["justlo"], SHARED_RATE),
        variable_line("linduuins", shared_values["linduu"], SHARED_RATE),
        variable_line("gnoxxins", shared_values["gnoxx"], SHARED_RATE),
        f"  {'fixed':<12s}: {FIXED_BONUS:,.2f} DT",
    ]

    if complete:
        total_line = f"  TOTAL        : {known_total:,.2f} DT"
    else:
        total_line = (
            f"  TOTAL        : UNAVAILABLE — known subtotal {known_total:,.2f} DT; "
            f"missing {', '.join(missing)}"
        )

    previous = _load_state()
    daily_line = "  Daily estimate: n/a (new calculator baseline saved after a complete run)"
    if complete and previous and previous.get("formula_version") == FORMULA_VERSION and previous.get("complete"):
        try:
            previous_time = datetime.fromisoformat(previous["timestamp"])
            elapsed_days = (now - previous_time).total_seconds() / 86400.0
            delta = known_total - float(previous["money"])
            if elapsed_days > 0:
                daily_line = (
                    f"  Daily estimate: ~{delta / elapsed_days:,.2f} DT/day "
                    f"({delta:+,.2f} DT since {previous_time:%Y-%m-%d %H:%M})"
                )
        except Exception:
            pass

    sep = "=" * 76
    text = (
        f"CHECK-IN ALL CALCULATOR — {now:%Y-%m-%d %H:%M:%S}\n"
        f"{sep}\n"
        "Formula: goldins + (xkussins × 0.20 × 3.4) + "
        "((justloins + linduuins + gnoxxins) × 0.17 × 3.4) + 200\n\n"
        "Live counters:\n"
        + ("\n".join(account_lines) if account_lines else "  No platform data available")
        + "\n\nCalculator variables:\n"
        + "\n".join(variable_lines)
        + f"\n\n{total_line}\n{daily_line}\n\n{sep}\n"
        "Unavailable means the live tab or exact counter could not be read; it was not counted as zero.\n"
    )

    try:
        NOTE_PATH.parent.mkdir(parents=True, exist_ok=True)
        NOTE_PATH.write_text(text, encoding="utf-8")
    except Exception as exc:
        text += f"\n[WARN] could not write note to {NOTE_PATH}: {exc}\n"

    state = {
        "formula_version": FORMULA_VERSION,
        "timestamp": now.isoformat(),
        "complete": complete,
        "missing": missing,
        "known_subtotal": known_total,
        "variables": {
            "goldins": gold_money if not gold_missing else None,
            "xkussins": xkussins,
            **{f"{name}ins": value for name, value in shared_values.items()},
        },
        "rows": rows,
    }
    if complete:
        state["money"] = known_total
    try:
        STATE_PATH.write_text(json.dumps(state, indent=2), encoding="utf-8")
    except Exception:
        pass

    return text


async def _xkuss_ins(page) -> int | None:
    link = page.locator("a.admein[href*='oldview_inout.php']")
    if await link.count() == 0:
        return None
    text = await link.first.inner_text()
    return _parse_xkuss_ins(text)


async def _monthly_ins(page) -> int | None:
    stats = page.locator(".mod-stats")
    if await stats.count() == 0:
        return None
    value = await stats.first.evaluate(
        """root => {
          const labels = [...root.querySelectorAll('p')];
          const label = labels.find(el => el.textContent.trim().toLowerCase() === 'im monat');
          return label?.nextElementSibling?.textContent?.trim() ?? null;
        }"""
    )
    return _parse_counter(value)


def _kind_for(name: str) -> str:
    if name == "xkuss":
        return "xkuss"
    if name in SHARED_NAMES:
        return "shared"
    return "react"


async def gather_and_write(platform_names: list[str]) -> tuple[str, Path]:
    """Read every requested live browser counter and write the calculator note."""
    rows: list[dict] = []
    async with async_playwright() as playwright:
        for name in platform_names:
            name = name.lower()
            kind = _kind_for(name)
            label = name.title()
            try:
                cfg = import_module(f"configs.{name}").config
                label = cfg.platform
                browser = await playwright.chromium.connect_over_cdp(cfg.cdp_url)
                context = browser.contexts[0]
                candidates = [page for page in context.pages if cfg.tab1_pattern.lower() in page.url.lower()]
                tab = next((page for page in candidates if "community-mod" in page.url.lower()), None)
                tab = tab or (candidates[0] if candidates else None)
                if tab is None:
                    reason = "site tab not found"
                    print(f"[{label}] {reason} — variable is unavailable.", flush=True)
                    rows.append(_row_unavailable(label, name, kind, reason))
                    continue

                if kind == "xkuss":
                    ins = await _xkuss_ins(tab)
                    source = "oldview_inout.php INs"
                elif kind == "shared":
                    ins = await _monthly_ins(tab)
                    source = "Im Monat"
                else:
                    stats = await check_stats(tab, cfg.platform)
                    ins = stats.get("Ins") if stats else None
                    asa_outs = stats.get("ASA Outs") if stats else None
                    if not isinstance(ins, int) or not isinstance(asa_outs, int):
                        reason = "Ins/ASA Outs statistics unavailable"
                        print(f"[{label}] {reason}.", flush=True)
                        rows.append(_row_unavailable(label, name, kind, reason))
                        continue
                    money = _react_money(ins, asa_outs)
                    rows.append({
                        "platform": label,
                        "name": name,
                        "kind": kind,
                        "available": True,
                        "ins": ins,
                        "asa_outs": asa_outs,
                        "money": money,
                        "source": "Meine Statistiken API",
                    })
                    print(f"[{label}] Ins={ins}, ASA Outs={asa_outs} → {money:,.2f} DT", flush=True)
                    continue

                if ins is None:
                    reason = f"{source} counter not found"
                    print(f"[{label}] {reason} — variable is unavailable.", flush=True)
                    rows.append(_row_unavailable(label, name, kind, reason))
                    continue
                rate_value = XKUSS_VALUE if kind == "xkuss" else SHARED_VALUE
                money = ins * rate_value
                rows.append({
                    "platform": label,
                    "name": name,
                    "kind": kind,
                    "available": True,
                    "ins": ins,
                    "money": money,
                    "source": source,
                })
                print(f"[{label}] {source}={ins} → {money:,.2f} DT", flush=True)
            except Exception as exc:
                raw_reason = " ".join(str(exc).split())
                reason = "browser is not running" if "ECONNREFUSED" in raw_reason else raw_reason
                reason = reason or type(exc).__name__
                print(f"[{label}] Could not read counter: {reason}", flush=True)
                rows.append(_row_unavailable(label, name, kind, reason[:180]))

    return _build_note(rows), NOTE_PATH
