import os

from core.xkuss_bot import XkussBot, XkussConfig

config = XkussConfig(
    platform="Xkuss",
    cdp_url="http://127.0.0.1:9227",
    tab1_pattern="xkuss",
    tab2_pattern="chamaleon-ai",

    # ── xkuss mod site (old PHP UI) ──────────────────────────────────────────
    tab1_url="https://xkuss.com/agency_new/login.php",   # login page (nick / pass form)
    home_url="",                      # optional explicit Home URL; blank = use navbar / derive
    username=os.environ.get("XKUSS_USERNAME", ""),  # <input name='nick'>
    password=os.environ.get("XKUSS_PASSWORD", ""),  # <input name='pass'>

    # ── chameleon AI (shared with every other platform) ──────────────────────
    chameleon_email=os.environ.get("CHAMELEON_EMAIL", ""),
    chameleon_password=os.environ.get("CHAMELEON_PASSWORD", ""),
    chameleon_chat="Global",          # xkuss uses the 'Global' chameleon chat
    additional_instructions="",
)

# run_bot.py reads this to use the xkuss-specific bot instead of the core ChatBot.
bot_class = XkussBot
