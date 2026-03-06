import os
import sqlite3
import sys

# Додаємо корінь проекту в шлях, щоб імпорти працювали правильно
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from app.services.cloudflare_d1 import CloudflareD1Manager


def migrate():
    # Автоматично знаходимо шлях до бази:
    # Ми в app/ -> йдемо на рівень вище -> заходимо в data/db/
    base_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    db_path = os.path.join(base_dir, 'data', 'db', 'description_translations.db')

    print(f"🔍 Шукаю базу за шляхом: {db_path}")

    if not os.path.exists(db_path):
        print(f"❌ Помилка: Файл не знайдено! Перевір папку data/db/")
        return

    # 1. Підключаємось до локальної бази
    local_conn = sqlite3.connect(db_path)
    cursor = local_conn.cursor()

    try:
        cursor.execute("SELECT supplier_id, code, unicode, pl_text, uk_text FROM dict")
        rows = cursor.fetchall()
    except sqlite3.OperationalError:
        print("❌ Помилка: В базі немає таблиці 'dict'. Перевір файл.")
        return

    print(f"📦 Знайдено {len(rows)} записів для перенесення...")

    d1 = CloudflareD1Manager()
    count = 0

    # 2. Переносимо дані в хмару
    for row in rows:
        supplier_id, code, unicode_val, pl_text, uk_text = row
        # Використовуємо метод save_to_cache з нашого менеджера
        d1.save_to_cache(supplier_id, code, unicode_val, pl_text, uk_text)

        count += 1
        if count % 20 == 0:
            print(f"✅ Перенесено {count} із {len(rows)}...")

    print(f"\n🚀 Міграція завершена! Успішно перенесено {count} записів.")
    local_conn.close()


if __name__ == "__main__":
    migrate()