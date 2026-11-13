import os

from core.justlo_bot import JustloBot, JustloConfig


# Linduu US serves the same dedicated login page and ExtJS moderation console
# as Linduu. It gets its own browser profile and Chameleon workspace so the two
# accounts can run at the same time without sharing tabs or session state.
config = JustloConfig(
    platform="Linduu US",
    cdp_url="http://127.0.0.1:9234",
    profile="linduu_us",
    tab1_pattern="linduu.us",
    tab2_pattern="chamaleon-ai",

    tab1_url="https://www.linduu.us/auth/index/login",
    login_url="https://www.linduu.us/auth/index/login",
    mod_url="https://www.linduu.us/community-mod/",
    console_via_goto=True,
    username=os.environ.get("LINDUU_US_USERNAME", ""),
    password=os.environ.get("LINDUU_US_PASSWORD", ""),

    chameleon_email=os.environ.get("CHAMELEON_EMAIL", ""),
    chameleon_password=os.environ.get("CHAMELEON_PASSWORD", ""),
    chameleon_chat="Linduu US",
    # The live "Linduu US" chat exposes Solo/Team/Qualität/Justlo tabs. The
    # extractor used by this moderation flow is the tab labelled "Justlo".
    chameleon_workspace_tab="Justlo",
    additional_instructions="",
    # This workspace renders the authoritative sendable card as
    # "US REPLY (ENGLISH)".  The shared Justlo-family default looks only for
    # "Antwort (Deutsch)", which leaves the bot polling forever even though
    # the English reply is already visible.
    reply_language_labels=("Reply (English)",),

    sel_login_link="",
    sel_login_user="#username",
    sel_login_pass="#password",
    sel_login_btn="#login_btn",
    sel_mod_link="a[href*='community-mod']",
)


bot_class = JustloBot
