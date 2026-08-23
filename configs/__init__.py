from dotenv import load_dotenv

# Every configs/<platform>.py reads its credentials from os.environ at import
# time (see .env). Loading here -- once, in the package's __init__ -- means
# any `import configs.xxx` gets a populated environment first, regardless of
# whether the entry point (run_bot.py, launch_all.py, ...) remembered to call
# load_dotenv() itself.
load_dotenv()
