# Copyright (C) @TheSmartBisnu
# Channel: https://t.me/itsSmartDev

from time import time


# ═══════════════════════════════════════════════════════════════════════════════
# TELEGRAM CONFIG
# ═══════════════════════════════════════════════════════════════════════════════

API_ID = 37374661
API_HASH = "e1956491ead91a58c8c1f263a3f30326"
BOT_TOKEN = "8803919104:AAEHmgaWjDFi_r2gJqbPPYz3RykhEdu6dYA"

# Pyrogram user session string
SESSION_STRING = "BQI6SsUAeYd0nTvwaJbJp3RtjG4_lhQpgxjVrRq8eapQXv2x1E2Yn9RlFUTeXA4iat5V7l4kMCLQLA89MnNXluLFG48YNGbsPAIhfWvcYC_-CX_I0-tuvC3k77_Pz5tGXE4aojkWBI3E7zKWrD_T5XqKZ4dCoNtpcck9MLdddjC15qOK9QXi3_-pm0HaegWahS8Qsov2R_XsVzrUP2-CTsKt8ZXpB79Bhw2iR97W8vDvQnHGIra3QbeExn8hK8tHq9nSdQ8dW8O01amoilpRRqMpqyYB54WO6wRA7fkO868eabHwJV2yddjVwjLEBhBneF6AmRXxyI-FjFZm-Cg7m7LD5_UMvwAAAAIHoELPAA"


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
