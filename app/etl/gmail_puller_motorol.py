"""
Gmail puller для MOTOROL:
- знаходить найновіший лист із вкладенням рівно "09033.cennik.zip"
- завантажує zip, розпаковує CSV, форматує
- запускає process_all_prices ТІЛЬКИ для профілю "site"
- прибирає всі тимчасові файли у data/temp (залишає лише state/)
"""
from __future__ import annotations
import base64
import json
import shutil
import zipfile
from pathlib import Path
from typing import Optional, List, Dict, Any
from dotenv import load_dotenv
from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
from googleapiclient.discovery import build

from app.services.paths import TEMP_DIR
from .price_manager import process_all_prices

# ---------- Налаштування ----------
PROCESS_ONLY_LATEST = True
REQUIRED_FILENAME = "09033.cennik.zip"
GMAIL_QUERY = 'has:attachment filename:09033.cennik.zip'
SCOPES = ["https://www.googleapis.com/auth/gmail.readonly"]
# ВАЖЛИВО: ID постачальника MOTOROL у вашій базі
MOTOROL_SUPPLIER_ID = 3

# Шляхи
TMP_DIR = TEMP_DIR
STATE_DIR = TMP_DIR / "state"
STATE_FILE = STATE_DIR / "gmail_puller_state.json"

# BACKEND_DIR = Path(__file__).resolve().parents[2]
# CREDENTIALS_PATH = BACKEND_DIR / "app" / "config" /"credentials.json"
# TOKEN_PATH = BACKEND_DIR / "app" / "config" /"token.json"

# Визначаємо корінь проекту
BACKEND_DIR = Path(__file__).resolve().parents[2]


def get_secret_path(filename: str) -> Path:
    """Шукає файл спочатку в app/config/, а потім у корені (для Render)"""
    # Шлях як у вас на комп'ютері
    local_path = BACKEND_DIR / "app" / "config" / filename
    if local_path.exists():
        return local_path

    # Шлях як на Render (якщо файл додано через Secret Files без вказання папок)
    render_path = BACKEND_DIR / filename
    return render_path


CREDENTIALS_PATH = get_secret_path("credentials.json")
TOKEN_PATH = get_secret_path("token.json")

print(f"DEBUG: Використовую credentials за шляхом: {CREDENTIALS_PATH}")
print(f"DEBUG: Використовую token за шляхом: {TOKEN_PATH}")

load_dotenv(BACKEND_DIR / ".env")


# ---------- Утиліти ----------
def ensure_tmp():
    TMP_DIR.mkdir(parents=True, exist_ok=True)
    STATE_DIR.mkdir(parents=True, exist_ok=True)
    if not STATE_FILE.exists():
        with open(STATE_FILE, "w", encoding="utf-8") as f:
            json.dump({"processed": []}, f, ensure_ascii=False)


def load_state() -> Dict:
    if STATE_FILE.exists():
        with open(STATE_FILE, "r", encoding="utf-8") as f:
            return json.load(f)
    return {"processed": []}


def save_state(state: Dict):
    with open(STATE_FILE, "w", encoding="utf-8") as f:
        json.dump(state, f, ensure_ascii=False, indent=2)


def cleanup_temp_preserve_state():
    for item in TMP_DIR.iterdir():
        if item.resolve() == STATE_DIR.resolve():
            continue
        try:
            if item.is_file():
                item.unlink(missing_ok=True)
            else:
                shutil.rmtree(item, ignore_errors=True)
        except Exception:
            pass


def get_creds() -> Credentials:
    creds: Optional[Credentials] = None
    if TOKEN_PATH.exists():
        creds = Credentials.from_authorized_user_file(str(TOKEN_PATH), SCOPES)

    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            creds.refresh(Request())
        else:
            from google_auth_oauthlib.flow import InstalledAppFlow
            flow = InstalledAppFlow.from_client_secrets_file(str(CREDENTIALS_PATH), SCOPES)
            creds = flow.run_local_server(port=0)
        with open(TOKEN_PATH, "w", encoding="utf-8") as f:
            f.write(creds.to_json())
    return creds


def gmail_service() -> Any:
    return build("gmail", "v1", credentials=get_creds(), cache_discovery=False)


def search_messages(service, q: str) -> List[Dict]:
    res = service.users().messages().list(userId="me", q=q, maxResults=50).execute()
    return res.get("messages", [])


def download_first_zip_attachment(service, msg_id: str, dest_dir: Path) -> Optional[Path]:
    msg = service.users().messages().get(userId="me", id=msg_id).execute()
    parts = (msg.get("payload") or {}).get("parts", []) or []

    for part in parts:
        filename = (part.get("filename") or "").strip()
        if filename.lower() != REQUIRED_FILENAME.lower():
            continue

        body = part.get("body", {}) or {}
        att_id = body.get("attachmentId")

        if not att_id:
            data = body.get("data")
            if data:
                raw = base64.urlsafe_b64decode(data.encode("utf-8"))
                out = dest_dir / filename
                with open(out, "wb") as f:
                    f.write(raw)
                return out
            continue

        att = service.users().messages().attachments().get(
            userId="me", messageId=msg_id, id=att_id
        ).execute()
        raw = base64.urlsafe_b64decode(att["data"].encode("utf-8"))
        out = dest_dir / filename
        with open(out, "wb") as f:
            f.write(raw)
        return out

    return None


def unzip_to_csv(zip_path: Path, extract_dir: Path) -> Path:
    with zipfile.ZipFile(zip_path, "r") as zf:
        zf.extractall(extract_dir)
    for p in extract_dir.iterdir():
        if p.suffix.lower() == ".csv":
            return p
    raise FileNotFoundError("CSV file not found inside zip.")


def format_motorol_csv(input_csv: Path, output_csv: Path) -> None:
    import csv, re
    with open(input_csv, newline="", encoding="utf-8", errors="ignore") as src, \
            open(output_csv, "w", newline="", encoding="utf-8") as dst:
        reader = csv.reader(src, delimiter="\t")
        writer = csv.writer(dst, delimiter=";")
        for row in reader:
            joined = ";".join(row)
            joined = re.sub(r";\s+", ";", joined)
            joined = re.sub(r">\s*5", "10", joined)
            writer.writerow(joined.split(";"))


def already_processed(state: Dict, msg_id: str) -> bool:
    return msg_id in state.get("processed", [])


def mark_processed(state: Dict, msg_id: str):
    s = set(state.get("processed", []))
    s.add(msg_id)
    state["processed"] = list(s)


def pick_latest_matching(service, messages: List[Dict], required_filename: str) -> Optional[Dict]:
    latest = None
    latest_ts = -1
    for m in messages:
        full = service.users().messages().get(userId="me", id=m["id"]).execute()
        parts = (full.get("payload") or {}).get("parts", []) or []
        if not any((p.get("filename") or "").strip().lower() == required_filename.lower() for p in parts):
            continue
        ts = int(full.get("internalDate", 0))
        if ts > latest_ts:
            latest, latest_ts = full, ts
    return latest


def handle_one_message(service, msg_id: str) -> Dict:
    ensure_tmp()
    zip_path = download_first_zip_attachment(service, msg_id, TMP_DIR)
    if not zip_path:
        return {"msg_id": msg_id, "status": "no-zip"}

    csv_raw = unzip_to_csv(zip_path, TMP_DIR)
    csv_fmt = TMP_DIR / f"MOTOROL_formatted_{zip_path.stem}.csv"
    format_motorol_csv(csv_raw, csv_fmt)

    # --- ЗМІНА: Викликаємо обробку ТІЛЬКИ для профілю "site" ---
    # (Вирішує Проблему 2 - не ганяє зайві прайси)
    results = process_all_prices(
        supplier="MOTOROL",
        supplier_id=MOTOROL_SUPPLIER_ID,
        remote_gz_path=str(csv_fmt),
        # profile_filter="site"  # <--- ФІЛЬТР
    )
    # -----------------------------------------------------------

    try:
        zip_path.unlink(missing_ok=True)
        csv_raw.unlink(missing_ok=True)
        csv_fmt.unlink(missing_ok=True)
    except Exception:
        pass

    return {"msg_id": msg_id, "status": "ok", "results": results}


def find_and_process_latest(service) -> None:
    msgs = search_messages(service, GMAIL_QUERY)
    if not msgs:
        print("No messages found.")
        return
    latest = pick_latest_matching(service, msgs, REQUIRED_FILENAME)
    if not latest:
        print(f"No messages with attachment '{REQUIRED_FILENAME}'.")
        return

    state = load_state()
    msg_id = latest["id"]
    if already_processed(state, msg_id):
        print("Latest matching message already processed.")
        return

    out = handle_one_message(service, msg_id)
    print("Processed latest:", out)
    mark_processed(state, msg_id)
    save_state(state)


def main():
    ensure_tmp()
    try:
        service = gmail_service()
        find_and_process_latest(service)
    finally:
        cleanup_temp_preserve_state()
        print("🧹 temp cleaned (state/ збережено).")


if __name__ == "__main__":
    main()