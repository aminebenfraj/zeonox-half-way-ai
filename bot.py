#!/usr/bin/env python3
"""
Backward-compatible entry point for the Gold platform bot.

This now delegates to the shared core. To run any platform directly:
    python run_bot.py gold
    python run_bot.py diamond
    python run_bot.py platin

To run all platforms simultaneously:
    python start_all.py

SETUP (one time per Chrome profile):
  1. Close Chrome completely.
  2. Launch it with remote debugging on the matching port:
       chrome.exe --remote-debugging-port=9222 --user-data-dir="profiles\\gold"
  3. Open both tabs in that Chrome window and log in:
       Tab1: https://mods.gold-chat.net/...
       Tab2: https://chamaleon-ai-0c02461b.base44.app/AgentWorkspace
  4. Keep Chrome open and run: python bot.py
"""

import asyncio
from configs.gold import config
from core.bot import ChatBot

if __name__ == "__main__":
    asyncio.run(ChatBot(config).run())

