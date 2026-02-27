import os
from dotenv import load_dotenv
from fastapi import APIRouter, Query, HTTPException
from typing import List, Dict, Any
from sqlalchemy import create_engine, text
import re

# 1. Завантажуємо змінні з .env
load_dotenv()

router = APIRouter()

# # 2. Отримуємо налаштування
# DB_USER = os.getenv("DB_USER")
# DB_PASSWORD = os.getenv("DB_PASSWORD")
# DB_HOST = os.getenv("DB_HOST")
# DB_PORT = os.getenv("DB_PORT")
# DB_NAME = os.getenv("DB_NAME")
#
# DATABASE_URL = f"postgresql+psycopg2://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"

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

@router.get("/search", response_model=List[Dict[str, Any]])
def search_products(
        q: str = Query(..., min_length=2, description="Пошуковий запит"),
        limit: int = Query(50, ge=1, le=200)
):
    """
    Пошук по коду, unicode, бренду та опису.
    Для code/unicode ігноруємо додаткові символи (тире, пробіли, слеші тощо):
    'OF-935' == 'OF935' == 'OF 935'.
    """
    q_raw = (q or "").strip()
    if not q_raw:
        return []

    # Готуємо "чистий" запит у Python (OF-935 -> OF935)
    q_clean = re.sub(r'[^a-zA-Z0-9]', '', q_raw).upper()

    print(f"[INFO] API Search request: raw='{q_raw}', clean='{q_clean}'")

    try:
        # Нормалізований пошук по code/unicode (видаляємо не‑алфанумеричні символи в БД)
        sql_query = text("""
            SELECT supplier_id, code, unicode, brand, name, stock, price_eur
            FROM product_catalog
            WHERE
                regexp_replace(unicode, '[^[:alnum:]]', '', 'g') ILIKE :q_clean_like
                OR regexp_replace(code,   '[^[:alnum:]]', '', 'g') ILIKE :q_clean_like
                OR brand ILIKE :q_raw_like
                OR name  ILIKE :q_raw_like
            ORDER BY
                (regexp_replace(unicode, '[^[:alnum:]]', '', 'g') = :q_clean) DESC,
                price_eur ASC
            LIMIT :limit_val
        """)

        results: List[Dict[str, Any]] = []

        with engine.connect() as conn:
            rows = conn.execute(
                sql_query,
                {
                    "q_clean_like": f"%{q_clean}%",  # Для коду/унікоду (OF935)
                    "q_raw_like": f"%{q_raw}%",      # Для бренду/опису (як ввів користувач)
                    "q_clean": q_clean,              # Для ORDER BY (точні збіги вгору)
                    "limit_val": limit,
                }
            )

            for row in rows:
                results.append(dict(row._mapping))

        print(f"[INFO] API Search found {len(results)} items")
        return results

    except Exception as e:
        print(f"[ERROR] Database search failed: {e}")
        raise HTTPException(status_code=500, detail=str(e))