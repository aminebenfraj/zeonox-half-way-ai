import os

from core.bot import BotConfig

config = BotConfig(
    platform="S69",
    cdp_url="http://127.0.0.1:9225",
    tab1_pattern="mods.chatsx.net",
    tab2_pattern="chamaleon-ai",
    tab1_url="https://mods.chatsx.net/login",
    username=os.environ.get("S69_USERNAME", ""),
    password=os.environ.get("S69_PASSWORD", ""),
    chameleon_email=os.environ.get("CHAMELEON_EMAIL", ""),
    chameleon_password=os.environ.get("CHAMELEON_PASSWORD", ""),
    reload_on_zero_duration=True,
)
