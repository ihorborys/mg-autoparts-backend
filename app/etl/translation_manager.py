import pandas as pd
from sqlalchemy import text
from app.services.translator import translate_batch


def translate_dataframe_names(names_series: pd.Series, engine) -> pd.Series:
    # 1. Отримуємо список УНІКАЛЬНИХ назв
    unique_names = names_series.unique().tolist()
    unique_names = [str(n).strip() for n in unique_names if n and str(n).strip()]

    if not unique_names:
        return names_series

    # 2. Перевіряємо, що вже є в БД
    translations = {}
    print(f"[TRANS] 🔎 Шукаємо існуючі переклади в БД для {len(unique_names)} унікальних назв...")

    with engine.connect() as conn:
        # Використовуємо ANY(:names) - це дуже швидко в PostgreSQL
        query = text("SELECT pl_text, uk_text FROM translations_dictionary WHERE pl_text = ANY(:names)")
        rows = conn.execute(query, {"names": unique_names}).fetchall()
        for row in rows:
            translations[row[0]] = row[1]

    # 3. Знаходимо те, чого немає
    all_missing = [n for n in unique_names if n not in translations]

    if not all_missing:
        print("[TRANS] ✅ Всі назви вже мають переклад у БД.")
        return names_series.map(lambda x: translations.get(str(x).strip(), x))

    # --- ЛІМІТУВАННЯ ---
    LIMIT = 2000
    missing_to_process = all_missing[:LIMIT]

    print(f"[TRANS] 🔍 Знайдено нових фраз: {len(all_missing)}. Беремо в роботу перші {len(missing_to_process)}.")

    # 4. Перекладаємо через Google (наш translator.py з чанками по 100 і паузами)
    translated_list = translate_batch(missing_to_process)

    # 5. Зберігаємо нові записи в БД
    if translated_list:
        print(f"[TRANS] 💾 Записуємо {len(translated_list)} нових перекладів до БД...")
        with engine.connect() as conn:
            for pl, uk in zip(missing_to_process, translated_list):
                translations[pl] = uk
                # Використовуємо ON CONFLICT, щоб не було помилок дублікатів
                conn.execute(
                    text("""
                        INSERT INTO translations_dictionary (pl_text, uk_text) 
                        VALUES (:pl, :uk) 
                        ON CONFLICT (pl_text) DO NOTHING
                    """),
                    {"pl": pl, "uk": uk}
                )
            conn.commit()
        print("[TRANS] ✅ БД оновлено.")

    # 6. Мапимо результат
    # Ті позиції, які не потрапили в ліміт 20к, залишаться польськими (до наступного імпорту)
    return names_series.map(lambda x: translations.get(str(x).strip(), x))