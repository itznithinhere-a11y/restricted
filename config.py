import os
from time import time

# ═══════════════════════════════════════════════════════════════
# YAHAN APNI VALUES BHARO
# ═══════════════════════════════════════════════════════════════

# Bot ka apna API ID / HASH (my.telegram.org se)
API_ID = 39898036
API_HASH = "2c712fe0455db78196d9ce2c1c61a6b1"

# @BotFather se NAYA token (purana revoke kar do)
BOT_TOKEN = "8868005439:AAEGNfhs6wzPQZRTvX2Trp2H-Q9JIlorFIk"

# Tumhara Telegram user id (@userinfobot se mil jayega)
OWNER_ID = 8856853887

# Sirf in users ko allow karna ho to ids daalo, e.g. [111, 222]
# Khali [] = bot sabke liye public
ALLOWED_USERS = []


# ═══════════════════════════════════════════════════════════════
# ENCRYPTION KEY (automatic)
# Pehli baar run par "secret.key" file khud ban jayegi.
# Is file ka BACKUP rakho aur kisi ko share mat karo. Agar delete
# ho gayi to saare users ko dobara /connect karna padega.
# ═══════════════════════════════════════════════════════════════

def _load_key(path="secret.key"):
    if os.path.exists(path):
        with open(path, "r") as f:
            return f.read().strip()
    from cryptography.fernet import Fernet
    key = Fernet.generate_key().decode()
    with open(path, "w") as f:
        f.write(key)
    return key


# ═══════════════════════════════════════════════════════════════
# BOT SETTINGS
# ═══════════════════════════════════════════════════════════════

MAX_CONCURRENT_DOWNLOADS = 4     # sabhi users ka total simultaneous downloads
PER_USER_DOWNLOADS = 2           # ek user ke simultaneous downloads
BATCH_SIZE = 5
FLOOD_WAIT_DELAY = 2
MAX_BDL_RANGE = 5000
BDL_RETRIES = 3
BDL_PROGRESS_EVERY = 10


class PyroConf:
    API_ID = API_ID
    API_HASH = API_HASH
    BOT_TOKEN = BOT_TOKEN
    OWNER_ID = OWNER_ID
    ALLOWED_USERS = ALLOWED_USERS
    ENCRYPTION_KEY = _load_key()

    BOT_START_TIME = time()

    MAX_CONCURRENT_DOWNLOADS = MAX_CONCURRENT_DOWNLOADS
    PER_USER_DOWNLOADS = PER_USER_DOWNLOADS
    BATCH_SIZE = BATCH_SIZE
    FLOOD_WAIT_DELAY = FLOOD_WAIT_DELAY
    MAX_BDL_RANGE = MAX_BDL_RANGE
    BDL_RETRIES = BDL_RETRIES
    BDL_PROGRESS_EVERY = BDL_PROGRESS_EVERY