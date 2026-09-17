import os

from core.justlo_bot import JustloBot, JustloConfig

# Gnoxx uses the same ExtJS moderation console and workflow as Justlo/Linduu.
# Only the login host, browser profile, local extractor key, and credentials
# differ, so it intentionally reuses the proven JustloBot implementation.
config = JustloConfig(
    platform="Gnoxx",
    cdp_url="http://127.0.0.1:9231",
    profile="gnoxx",
    tab1_pattern="gnoxx",
    tab2_pattern="chamaleon-ai",

    tab1_url="https://www.gnoxx.de/auth/index/login",
    login_url="https://www.gnoxx.de/auth/index/login",
    mod_url="https://mod.gnoxx.de/community-mod/",
    console_via_goto=True,
    username=os.environ.get("GNOXX_USERNAME", ""),
    password=os.environ.get("GNOXX_PASSWORD", ""),

    chameleon_email=os.environ.get("CHAMELEON_EMAIL", ""),
    chameleon_password=os.environ.get("CHAMELEON_PASSWORD", ""),
    # The real service follows the same Justlo/Linduu workflow. Built-in mode
    # uses the dedicated Gnoxx extractor supplied with this project change.
    chameleon_chat="Justlo/Linduu DE",
    chameleon_platform_key="gnoxx",
    additional_instructions="",

    sel_login_link="",
    sel_login_user="#username",
    sel_login_pass="#password",
    sel_login_btn="#login_btn",
    sel_mod_link="a[href*='community-mod']",
    sel_conv_grid="#conversation-grid",
)

bot_class = JustloBot
