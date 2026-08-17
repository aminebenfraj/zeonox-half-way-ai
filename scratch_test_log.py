from core.bot import log_sent_message, _NOTE_LOG_PATH

log_sent_message("Gold", "Hallo, wie geht es dir?")
log_sent_message("Platin", "Test message with ümlaut äöü")

data = open(_NOTE_LOG_PATH, encoding="utf-8").read()
print(repr(data))
