from core.justlo_bot import JustloBot, JustloConfig

# Linduu shares the SAME ExtJS moderation console as justlo (same Play/Pause,
# 'Unterhaltung' grid, message box) but logs in differently — so it reuses
# JustloBot with linduu-specific login settings.

config = JustloConfig(
    platform="Linduu",
    cdp_url="http://127.0.0.1:9230",   # own debug port (justlo=9229, ml=9228)
    profile="linduu",                  # profiles/linduu (separate Chrome profile)
    tab1_pattern="linduu",             # matches linduu.com AND its /community-mod console
    tab2_pattern="chamaleon-ai",

    # ── linduu mod flow ──────────────────────────────────────────────────────
    # Dedicated login PAGE (no modal). After 'Einloggen' the community home shows
    # promo pop-ups and hides 'Mod' inside the 'Mehr' dropdown, so the console is
    # reached by URL instead of clicking the navbar link.
    tab1_url="https://www.linduu.com/auth/index/login",   # entry URL
    login_url="https://www.linduu.com/auth/index/login",  # dedicated login page
    mod_url="https://www.linduu.com/community-mod/",       # the ExtJS console
    console_via_goto=True,
    username="CB-Alex",                                   # #username
    password="654321",                                   # #password

    # ── chameleon AI (shared with justlo — same 'Justlo/Linduu DE' chat) ───────
    chameleon_email="amed13515@gmail.com",
    chameleon_password="Amine963@",
    chameleon_chat="Justlo/Linduu DE",
    additional_instructions="",

    # ── linduu login selectors (dedicated /auth/index/login form) ──────────────
    sel_login_link="",                       # no modal to open
    sel_login_user="#username",
    sel_login_pass="#password",
    sel_login_btn="#login_btn",              # <button name='login' id='login_btn'>Einloggen
    sel_mod_link="a[href*='community-mod']",  # present in the 'Mehr' dropdown
)

# run_bot.py reads this to use the justlo/linduu ExtJS bot instead of the core ChatBot.
bot_class = JustloBot
