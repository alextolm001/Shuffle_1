# loop_runner.py
import time
import subprocess
from datetime import datetime

while True:
    print(f"🕒 Nieuwe run om {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    subprocess.run(["python", "count_votes.py"])
    print("⏳ Wachten 3 minuten...\n")
    time.sleep(180)
