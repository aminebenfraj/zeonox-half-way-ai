import os

from core.bot import BotConfig

config = BotConfig(
    platform="Gold",
    cdp_url="http://127.0.0.1:9222",
    tab1_pattern="mods.gold-chat.net",
    tab2_pattern="chamaleon-ai",
    tab1_url="https://mods.gold-chat.net/login",
    username=os.environ.get("GOLD_USERNAME", ""),
    password=os.environ.get("GOLD_PASSWORD", ""),
    chameleon_email=os.environ.get("CHAMELEON_EMAIL", ""),
    chameleon_password=os.environ.get("CHAMELEON_PASSWORD", ""),
    additional_instructions="the reply must be longer then 120 character",
    reload_on_zero_duration=True,
)
