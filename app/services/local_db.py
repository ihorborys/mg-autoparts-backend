# import sqlite3
# import os
# from datetime import datetime
# from app.services.storage import StorageClient  # Переконайся, що шлях до StorageClient вірний
#
# # Шляхи до файлів
# DATA_DB_DIR = os.path.join('data', 'db')
# DB_PATH = os.path.join(DATA_DB_DIR, 'description_translations.db')
#
#
# def init_local_db():
#     """Ініціалізує папку data/db та створює таблицю в SQLite."""
#     if not os.path.exists(DATA_DB_DIR):
#         os.makedirs(DATA_DB_DIR, exist_ok=True)
#         print(f"[INFO] Створено директорію для бази: {DATA_DB_DIR}")
#
#     conn = sqlite3.connect(DB_PATH)
#     cursor = conn.cursor()
#
#     # Створюємо таблицю з усіма необхідними колонками
#     cursor.execute('''
#         CREATE TABLE IF NOT EXISTS dict (
#             id INTEGER PRIMARY KEY AUTOINCREMENT,
#             supplier_id INTEGER,
#             code TEXT,
#             unicode TEXT,
#             pl_text TEXT,
#             uk_text TEXT,
#             created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
#             -- Запобігає дублікатам для одного і того ж товару у постачальника
#             UNIQUE(supplier_id, code, unicode, pl_text)
#         )
#     ''')
#     conn.commit()
#     conn.close()
#     print(f"✅ Локальна база готова: {DB_PATH}")
#
#
# def get_cached_translation(supplier_id: int, code: str, unicode_val: str, pl_text: str):
#     """Шукає існуючий переклад у локальному SQLite."""
#     if not pl_text:
#         return None
#
#     pl_text_clean = str(pl_text).strip().upper()
#     try:
#         with sqlite3.connect(DB_PATH) as conn:
#             cursor = conn.cursor()
#             cursor.execute("""
#                 SELECT uk_text FROM dict
#                 WHERE supplier_id = ? AND code = ? AND unicode = ? AND pl_text = ?
#             """, (supplier_id, str(code), str(unicode_val), pl_text_clean))
#             res = cursor.fetchone()
#             return res[0] if res else None
#     except Exception as e:
#         print(f"⚠️ Помилка пошуку в SQLite: {e}")
#         return None
#
#
# def save_to_cache(supplier_id: int, code: str, unicode_val: str, pl_text: str, uk_text: str):
#     """Зберігає або оновлює переклад у локальній базі."""
#     if not pl_text or not uk_text:
#         return
#
#     pl_text_clean = str(pl_text).strip().upper()
#     try:
#         with sqlite3.connect(DB_PATH) as conn:
#             cursor = conn.cursor()
#             cursor.execute("""
#                 INSERT OR REPLACE INTO dict (supplier_id, code, unicode, pl_text, uk_text)
#                 VALUES (?, ?, ?, ?, ?)
#             """, (supplier_id, str(code), str(unicode_val), pl_text_clean, uk_text))
#             conn.commit()
#     except Exception as e:
#         print(f"❌ Помилка запису в SQLite: {e}")
#
#
# def backup_db_to_r2(keep_last: int = 10):
#     """
#     Робить бекап локальної бази в Cloudflare R2 через існуючий StorageClient.
#     Видаляє старі бекапи, залишаючи лише останні N копій.
#     """
#     if not os.path.exists(DB_PATH):
#         print("❌ Файл бази не знайдено. Бекап скасовано.")
#         return
#
#     try:
#         client = StorageClient()
#
#         # Генеруємо ім'я файлу з часом (напр. backups/db/translations_2026_03_04.db)
#         timestamp = datetime.now().strftime("%Y_%m_%d_%H%M")
#         cloud_key = f"backups/db/description_translations_{timestamp}.db"
#
#         print(f"☁️ Завантажую бекап у Cloudflare R2: {cloud_key}...")
#
#         db_url = client.upload_file(
#             local_path=DB_PATH,
#             key=cloud_key,
#             content_type="application/x-sqlite3",
#             cleanup_prefix="backups/db/",  # Авто-очищення старих баз у цій папці
#             keep_last=keep_last
#         )
#         print(f"✅ Бекап успішно завершено. Файл у хмарі: {cloud_key}")
#         return db_url
#
#     except Exception as e:
#         print(f"❌ Помилка завантаження бекапу в R2: {e}")
#         return None