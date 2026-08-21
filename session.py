from pyrogram import Client

# =========================
# TELEGRAM API CONFIG
# =========================

API_ID = 38261047
API_HASH = "9d50d164fa275483f5efd19793efbd31"


# =========================
# SESSION GENERATOR
# =========================

app = Client(
    "session_generator",
    api_id=API_ID,
    api_hash=API_HASH,
    in_memory=True
)

print("\n================================")
print("   PYROGRAM SESSION GENERATOR")
print("================================\n")

app.start()

session_string = app.export_session_string()

print("\n================================")
print("YOUR SESSION STRING:")
print("================================\n")
print(session_string)
print("\n================================")
print("Copy this into your config.py")
print("================================\n")

app.stop()