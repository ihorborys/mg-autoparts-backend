import os
import time
from dotenv import load_dotenv
from fastapi import APIRouter, Query, HTTPException, Response
from typing import List, Dict, Any, Optional
from sqlalchemy import create_engine, text
import re

# 1. Завантажуємо змінні з .env
load_dotenv()

router = APIRouter()

# 2. Отримуємо налаштування (Універсальний спосіб)
# Спершу шукаємо готовий DATABASE_URL (з Supabase/Render)
raw_url = os.getenv("DATABASE_URL")

if raw_url:
    # Якщо є готовий рядок, перевіряємо чи є в ньому драйвер psycopg2
    if raw_url.startswith("postgresql://"):
        DATABASE_URL = raw_url.replace("postgresql://", "postgresql+psycopg2://", 1)
    else:
        DATABASE_URL = raw_url
else:
    # Якщо готового рядка немає, збираємо по-старому (fallback)
    DB_USER = os.getenv("DB_USER", "postgres")
    DB_PASSWORD = os.getenv("DB_PASSWORD", "")
    DB_HOST = os.getenv("DB_HOST", "localhost")
    DB_PORT = os.getenv("DB_PORT", "5432")
    DB_NAME = os.getenv("DB_NAME", "postgres")
    DATABASE_URL = f"postgresql+psycopg2://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"

# ✅ СТВОРЮЄМО ENGINE ТУТ (ОДИН РАЗ ПРИ ЗАПУСКУ)
# SQLAlchemy сама керуватиме чергою запитів через цей об'єкт
engine = create_engine(DATABASE_URL, pool_pre_ping=True)

# Кеш-флаг: чи є в таблиці нормалізовані колонки для швидкого пошуку
HAS_NORM_COLUMNS: Optional[bool] = None


@router.get("/search", response_model=List[Dict[str, Any]])
def search_products(
        response: Response,
        q: str = Query(..., min_length=2, description="Пошуковий запит"),
        limit: int = Query(50, ge=1, le=200)
):
    q_raw = (q or "").strip()
    if not q_raw:
        return []

    # Допоміжна функція для очищення окремих слів
    def clean_val(text: str) -> str:
        return re.sub(r'[^A-Za-z0-9]', '', text).upper()

    # Розбиваємо запит на окремі слова
    words = q_raw.split()
    q_clean_full = clean_val(q_raw)

    # --- ЗАХИСТ ---
    if not q_clean_full or len(q_clean_full) < 2:
        print(f"[INFO] API Search: Blocked invalid query: '{q_raw}'")
        return []

    print(f"[INFO] API Search request: raw='{q_raw}', words_count={len(words)}")
    t0 = time.perf_counter()

    try:
        results: List[Dict[str, Any]] = []

        with engine.connect() as conn:
            # СЦЕНАРІЙ А: Два або більше слів (напр. "SACHS 315187" або "315187 SACHS")
            if len(words) >= 2:
                w1 = clean_val(words[0])
                w2 = clean_val("".join(words[1:]))  # Решту слів зліплюємо в одну частину

                sql_query = text("""
                    SELECT supplier_id, code, unicode, brand, name, stock, price_eur
                    FROM product_catalog
                    WHERE 
                        -- Варіант 1: Перше слово бренд, друге код
                        (brand_norm LIKE :w1_p AND (code_norm LIKE :w2_p OR unicode_norm LIKE :w2_p))
                        OR
                        -- Варіант 2: Перше слово код, друге бренд
                        (brand_norm LIKE :w2_p AND (code_norm LIKE :w1_p OR unicode_norm LIKE :w1_p))
                    ORDER BY 
                        (stock > 0) DESC, 
                        price_eur ASC
                    LIMIT :limit_val
                """)
                params = {
                    "w1_p": f"{w1}%",
                    "w2_p": f"{w2}%",
                    "limit_val": limit
                }

            # СЦЕНАРІЙ Б: Одне слово (напр. "GDB1330" або "BOSCH")
            else:
                sql_query = text("""
                    SELECT supplier_id, code, unicode, brand, name, stock, price_eur
                    FROM product_catalog
                    WHERE
                        unicode_norm LIKE :q_p
                        OR code_norm LIKE :q_p
                        OR brand_norm LIKE :q_p
                    ORDER BY
                        (unicode_norm = :q_c OR code_norm = :q_c) DESC, -- Точний збіг артикула вгору
                        (stock > 0) DESC,                             -- Те, що є в наявності - вище
                        price_eur ASC                                 -- Найдешевші
                    LIMIT :limit_val
                """)
                params = {
                    "q_p": f"{q_clean_full}%",
                    "q_c": q_clean_full,
                    "limit_val": limit
                }

            rows = conn.execute(sql_query, params)
            for row in rows:
                results.append(dict(row._mapping))

        elapsed_ms = (time.perf_counter() - t0) * 1000
        response.headers["X-Search-Ms"] = f"{elapsed_ms:.1f}"

        print(f"[INFO] API Search found {len(results)} items in {elapsed_ms:.1f} ms")
        return results

    except Exception as e:
        print(f"[ERROR] Database search failed: {e}")
        raise HTTPException(status_code=500, detail=str(e))