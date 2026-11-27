import os

from core.justlo_bot import JustloBot, JustloConfig


# English public login and the same ExtJS moderation console as Justlo FR.
config = JustloConfig(
    platform="Justlo US",
    cdp_url="http://127.0.0.1:9235",
    profile="justlo_us",
    tab1_pattern="justlo.us",
    tab2_pattern="chamaleon-ai",
    tab1_url="https://www.justlo.us/login",
    login_url="https://www.justlo.us/login",
    mod_url="https://www.justlo.us/community-mod/",
    console_via_goto=False,
    username=os.environ.get("JUSTLO_US_USERNAME", ""),
    password=os.environ.get("JUSTLO_US_PASSWORD", ""),
    chameleon_email=os.environ.get("CHAMELEON_EMAIL", ""),
    chameleon_password=os.environ.get("CHAMELEON_PASSWORD", ""),
    chameleon_chat=os.environ.get("JUSTLO_US_CHAMELEON_CHAT", "Justlo US"),
    chameleon_workspace_tab=os.environ.get("JUSTLO_US_CHAMELEON_TAB", "Justlo"),
    additional_instructions="Write the reply in natural American English.",
    reply_language_labels=("Reply (English)", "Answer (English)", "Antwort (Englisch)"),
    asa_2_3_nudge_enabled=True,
    sel_login_link="",
    sel_login_user='input[placeholder="Username"]',
    sel_login_pass='input[placeholder="Password"]',
    sel_login_btn="button[type='submit']:has-text('Sign In')",
    sel_mod_menu_btn=(
        'xpath=//header//button[.//span[normalize-space()="{username}"]]'
        '/following-sibling::div[1]//button[1]'
    ),
    sel_mod_link='text="Alter Mod"',
)

config.chameleon_html_adapter = "fr_extjs"
bot_class = JustloBot
