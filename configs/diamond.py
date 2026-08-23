import os

from core.bot import BotConfig

config = BotConfig(
    platform="Diamond",
    cdp_url="http://127.0.0.1:9223",
    tab1_pattern="mods.diamondchat.net",
    tab2_pattern="chamaleon-ai",
    tab1_url="https://mods.diamondchat.net/login",
    username=os.environ.get("DIAMOND_USERNAME", ""),
    password=os.environ.get("DIAMOND_PASSWORD", ""),
    chameleon_email=os.environ.get("CHAMELEON_EMAIL", ""),
    chameleon_password=os.environ.get("CHAMELEON_PASSWORD", ""),
    reload_on_zero_duration=True,
)


