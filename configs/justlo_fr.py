import os

from core.justlo_bot import JustloBot, JustloConfig


# Justlo FR uses the same ExtJS moderation console and conversation workflow as
# Linduu/Justlo.  Its public React site has a different login form and reveals
# the moderation link through the eye control beside the signed-in username.
config = JustloConfig(
    platform="Justlo FR",
    cdp_url="http://127.0.0.1:9233",
    profile="justlo_fr",
    tab1_pattern="justlo.fr",
    tab2_pattern="chamaleon-ai",

    tab1_url="https://www.justlo.fr/login",
    login_url="https://www.justlo.fr/login",
    mod_url="https://www.justlo.fr/community-mod/",
    console_via_goto=False,
    username=os.environ.get("JUSTLO_FR_USERNAME", ""),
    password=os.environ.get("JUSTLO_FR_PASSWORD", ""),

    chameleon_email=os.environ.get("CHAMELEON_EMAIL", ""),
    chameleon_password=os.environ.get("CHAMELEON_PASSWORD", ""),
    chameleon_chat=os.environ.get("JUSTLO_FR_CHAMELEON_CHAT", "Linduu FR"),
    chameleon_workspace_tab="FR",
    additional_instructions="",
    reply_language_labels=(
        "Antwort (Französisch)",
        "Réponse (Français)",
        "Antwort (Originalsprache)",
        "Réponse (langue originale)",
        # Some Chameleon workspaces keep the legacy German card title even
        # when Zusatzanweisungen makes the generated text itself French.
        "Antwort (Deutsch)",
    ),
    asa_2_3_nudge_enabled=False,

    sel_login_link="",
    sel_login_user='input[placeholder="Nom d\'utilisateur"]',
    sel_login_pass='input[placeholder="Mot de passe"]',
    sel_login_btn="button[type='submit']:has-text('Se connecter')",
    sel_mod_menu_btn=(
        'xpath=//header//button[.//span[normalize-space()="{username}"]]'
        '/following-sibling::div[1]//button[1]'
    ),
    sel_mod_link='text="Alter Mod"',
)


bot_class = JustloBot
