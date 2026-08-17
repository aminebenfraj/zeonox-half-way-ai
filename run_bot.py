#!/usr/bin/env python3
"""
Entry point for a single platform bot.

Usage:
    python run_bot.py gold
    python run_bot.py diamond
    python run_bot.py platin
"""
import sys
import asyncio
from importlib import import_module

# Force UTF-8 stdout/stderr so bot output with non-ASCII characters (German chat
# text, the ──── cycle separators, …) prints on the Windows console instead of
# crashing with a cp1252 UnicodeEncodeError.
for _stream in (sys.stdout, sys.stderr):
    try:
        _stream.reconfigure(encoding="utf-8", errors="replace")
    except (AttributeError, ValueError):
        pass


def main():
    if len(sys.argv) < 2:
        print("Usage: python run_bot.py <platform>")
        print("Available platforms: gold, gold2, gold3, diamond, platin, s69, ml, xkuss, justlo, linduu")
        sys.exit(1)

    platform = sys.argv[1].lower()
    try:
        mod = import_module(f"configs.{platform}")
    except ModuleNotFoundError:
        print(f"[ERROR] Unknown platform '{platform}'. Check configs/ for available options.")
        sys.exit(1)

    # A config may declare its own bot class (e.g. xkuss uses XkussBot because its
    # DOM is completely different). Otherwise fall back to the shared core ChatBot.
    bot_class = getattr(mod, "bot_class", None)
    if bot_class is None:
        from core.bot import ChatBot
        bot_class = ChatBot
    asyncio.run(bot_class(mod.config).run())


if __name__ == "__main__":
    main()
