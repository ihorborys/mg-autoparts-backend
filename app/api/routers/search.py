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


    # 1. Готуємо "чистий" запит (видаляємо все крім A-Z, 0-9)
    q_clean = re.sub(r'[^A-Za-z0-9]', '', q_raw).upper()

    # --- НОВИЙ БЛОК: ЗАХИСТ ---
    # Якщо після очищення запит став порожнім (наприклад, тільки кирилиця)
    # АБО запит занадто короткий (менше 2 символів)
    if not q_clean or len(q_clean) < 2:
        print(f"[INFO] API Search: Blocked invalid/Cyrillic query: '{q_raw}'")
        return []
        # --------------------------

    print(f"[INFO] API Search request: raw='{q_raw}', clean='{q_clean}'")

    t0 = time.perf_counter()

    try:
        results: List[Dict[str, Any]] = []

        with engine.connect() as conn:
            # 2. ПРЯМИЙ ТА ШВИДКИЙ SQL
            # Ми замінили ILIKE '%...%' на LIKE '...%' для швидкості B-tree
            sql_query = text("""
                SELECT supplier_id, code, unicode, brand, name, stock, price_eur
                FROM product_catalog
                WHERE
                    unicode_norm LIKE :q_prefix      -- Пошук за початком (B-tree)
                    OR code_norm LIKE :q_prefix      -- Пошук за початком (B-tree)
                    OR brand_norm = :q_clean         -- Точний пошук бренду (Миттєво)
                ORDER BY
                    (unicode_norm = :q_clean OR code_norm = :q_clean) DESC, -- Точний номер вище за все
                    price_eur ASC                                          -- Найдешевші перші
         
                LIMIT :limit_val
            """)

            rows = conn.execute(
                sql_query,
                {
                    "q_prefix": f"{q_clean}%",  # Працює з B-tree індексом
                    "q_clean": q_clean,  # Для точного збігу та бренду
                    "limit_val": limit,
                },
            )

            for row in rows:
                results.append(dict(row._mapping))

        # Додаємо час виконання в хедери (для дебагу)
        elapsed_ms = (time.perf_counter() - t0) * 1000
        response.headers["X-Search-Ms"] = f"{elapsed_ms:.1f}"

        print(f"[INFO] API Search found {len(results)} items in {elapsed_ms:.1f} ms")
        return results

    except Exception as e:
        print(f"[ERROR] Database search failed: {e}")
        raise HTTPException(status_code=500, detail=str(e))