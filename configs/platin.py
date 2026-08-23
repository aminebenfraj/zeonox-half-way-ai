import os

from core.bot import BotConfig

config = BotConfig(
    platform="Platin",
    cdp_url="http://127.0.0.1:9224",
    tab1_pattern="mods.platin-chat.com",
    tab2_pattern="chamaleon-ai",
    tab1_url="https://mods.platin-chat.com/login",
    username=os.environ.get("PLATIN_USERNAME", ""),
    password=os.environ.get("PLATIN_PASSWORD", ""),
    chameleon_email=os.environ.get("CHAMELEON_EMAIL", ""),
    chameleon_password=os.environ.get("CHAMELEON_PASSWORD", ""),
    reload_on_zero_duration=True,
)
