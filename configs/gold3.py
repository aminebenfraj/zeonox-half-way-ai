import os

from core.bot import BotConfig

config = BotConfig(
    platform="Gold3",
    # Dedicated port.  9231 belongs to Gnoxx; sharing it makes both bots attach
    # to the same browser and breaks platform/tab detection when run together.
    cdp_url="http://127.0.0.1:9232",
    tab1_pattern="mods.gold-chat.net",
    tab2_pattern="chamaleon-ai",
    tab1_url="https://mods.gold-chat.net/login",
    username=os.environ.get("GOLD3_USERNAME", ""),
    password=os.environ.get("GOLD3_PASSWORD", ""),
    chameleon_email=os.environ.get("CHAMELEON_EMAIL", ""),
    chameleon_password=os.environ.get("CHAMELEON_PASSWORD", ""),
    additional_instructions="the reply must be longer then 120 character",
    reload_on_zero_duration=True,
)
