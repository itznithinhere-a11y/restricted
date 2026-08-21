# Copyright (C) @TheSmartBisnu
# Channel: https://t.me/itsSmartDev

from time import time


# ═══════════════════════════════════════════════════════════════════════════════
# TELEGRAM CONFIG
# ═══════════════════════════════════════════════════════════════════════════════

API_ID = 38261047
API_HASH = "9d50d164fa275483f5efd19793efbd31"
BOT_TOKEN = "8748789520:AAEVfU8zq_CtrLZtWQPCvzvOAQjpifVT970"

# Pyrogram user session string
SESSION_STRING = "BQJH0TcApCP2iMORiNMlNWV5dGPYRUu5a39ICRV47cBP5XYSMvipEvhNzBbbSWMOPfe_AywtET2hrWOzGSLOLqcN1uJGHc0tok9Yh6_y37wiHQkhHsUwtCIcEnQfclQW1OdylsSqsSy77TYOGRx5v0cQi2OEKTM7K02vqyYSCpK0nlUdlzdWW3kXyKHTMLLicCIgpv6hE6Q52JpJIxF9AyHhokUiLQqjJ2osxs_53PZqrFcDajC4I-FjtlIlQH6wedt5L1GftSr2KCfOnlowdJiOBM5I9AMJw0BgcRIV09JU096twsuoorBFOBdZjAs2YzfpFl4IQRENngTMRkQ-Wzi-5dTS8AAAAAIVekU7AA"


# ═══════════════════════════════════════════════════════════════════════════════
# BOT SETTINGS
# ═══════════════════════════════════════════════════════════════════════════════



FORWARD_CHAT_ID = None
BATCH_SIZE = 10                  # ek time par max 10 jobs schedule
MAX_CONCURRENT_DOWNLOADS = 2    # actual simultaneous downloads
FLOOD_WAIT_DELAY = 2
MAX_BDL_RANGE = 2000
BDL_RETRIES = 3
BDL_PROGRESS_EVERY = 10

# ═══════════════════════════════════════════════════════════════════════════════
# PYROGRAM CONFIG
# ═══════════════════════════════════════════════════════════════════════════════

class PyroConf:
    API_ID = API_ID
    API_HASH = API_HASH
    BOT_TOKEN = BOT_TOKEN
    SESSION_STRING = SESSION_STRING

    BOT_START_TIME = time()

    MAX_CONCURRENT_DOWNLOADS = MAX_CONCURRENT_DOWNLOADS
    BATCH_SIZE = BATCH_SIZE
    FLOOD_WAIT_DELAY = FLOOD_WAIT_DELAY

    FORWARD_CHAT_ID = FORWARD_CHAT_ID