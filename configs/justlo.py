import os

from core.justlo_bot import JustloBot, JustloConfig

config = JustloConfig(
    platform="Justlo",
    cdp_url="http://127.0.0.1:9229",   # own debug port (xkuss=9227, ml=9228)
    tab1_pattern="justlo",             # matches justlo.de AND mod.justlo.de
    tab2_pattern="chamaleon-ai",

    # ── justlo mod flow ──────────────────────────────────────────────────────
    # Logged out, mod.justlo.de redirects to the justlo.de landing page; from
    # there the flow is: Login -> Einloggen -> click 'Mod' -> Play.
    tab1_url="https://mod.justlo.de",                     # entry / landing URL
    mod_url="https://mod.justlo.de/community-mod/",        # the ExtJS console
    username=os.environ.get("JUSTLO_USERNAME", ""),      # #login input[name='username']
    password=os.environ.get("JUSTLO_PASSWORD", ""),      # #login input[name='password']

    # ── chameleon AI (shared with every other platform) ──────────────────────
    chameleon_email=os.environ.get("CHAMELEON_EMAIL", ""),
    chameleon_password=os.environ.get("CHAMELEON_PASSWORD", ""),
    chameleon_chat="Justlo/Linduu DE",  # justlo + linduu share this chameleon chat
    additional_instructions="",
)

# run_bot.py reads this to use the justlo-specific bot instead of the core ChatBot.
bot_class = JustloBot
