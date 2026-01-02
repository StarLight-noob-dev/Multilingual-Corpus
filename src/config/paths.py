from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]

DATA_DIR = ROOT / "data"
DOWNLOAD_DIR = DATA_DIR / "downloads"
LOGS_DIR = ROOT / "logs"

if __name__ == "__main__":
    print(f"Root directory: {ROOT}")
    print(f"Data directory: {DATA_DIR}")
    print(f"Download directory: {DOWNLOAD_DIR}")
    print(f"Logs directory: {LOGS_DIR}")
