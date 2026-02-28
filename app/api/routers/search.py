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

# Кеш-флаг: чи є в таблиці нормалізовані колонки для швидкого пошуку
HAS_NORM_COLUMNS: Optional[bool] = None

@router.get("/search", response_model=List[Dict[str, Any]])
def search_products(
        response: Response,
        q: str = Query(..., min_length=2, description="Пошуковий запит"),
        limit: int = Query(50, ge=1, le=200)
):
    """
    Пошук по коду та unicode (головний пріоритет) і, додатково, по опису.
    Для code/unicode ігноруємо додаткові символи (тире, пробіли, слеші тощо):
    'OF-935' == 'OF935' == 'OF 935'.

    Для швидкості пошуку використовуємо попередньо нормалізовані колонки:
    code_norm / unicode_norm (заповнюються в ETL).

    Пошук по бренду ми більше не виконуємо, щоб максимально
    прискорити відповіді саме по номерах.
    """
    q_raw = (q or "").strip()
    if not q_raw:
        return []

    # Готуємо "чистий" запит у Python (OF-935 -> OF935)
    q_clean = re.sub(r'[^a-zA-Z0-9]', '', q_raw).upper()

    print(f"[INFO] API Search request: raw='{q_raw}', clean='{q_clean}'")
    t0 = time.perf_counter()

    try:
        global HAS_NORM_COLUMNS
        results: List[Dict[str, Any]] = []

        with engine.connect() as conn:
            # 1. Разово перевіряємо, чи є в таблиці code_norm / unicode_norm
            if HAS_NORM_COLUMNS is None:
                cols_res = conn.execute(
                    text(
                        """
                        SELECT column_name
                        FROM information_schema.columns
                        WHERE table_name = 'product_catalog'
                          AND column_name IN ('code_norm', 'unicode_norm')
                        """
                    )
                )
                cols = {row[0] for row in cols_res}
                HAS_NORM_COLUMNS = "code_norm" in cols and "unicode_norm" in cols
                print(f"[INFO] DB: normalized columns present = {HAS_NORM_COLUMNS}")

            if HAS_NORM_COLUMNS:
                # ШВИДКИЙ варіант: використовуємо попередньо нормалізовані колонки
                sql_query = text(
                    """
                    SELECT supplier_id, code, unicode, brand, name, stock, price_eur
                    FROM product_catalog
                    WHERE
                        unicode_norm ILIKE :q_clean_like
                        OR code_norm ILIKE :q_clean_like
                        OR name  ILIKE :q_raw_like
                    ORDER BY
                        (unicode_norm = :q_clean) DESC,
                        price_eur ASC
                    LIMIT :limit_val
                    """
                )
            else:
                # РЕЗЕРВНИЙ варіант: працює навіть без нових колонок (але повільніше)
                sql_query = text(
                    """
                    SELECT supplier_id, code, unicode, brand, name, stock, price_eur
                    FROM product_catalog
                    WHERE
                        regexp_replace(unicode, '[^[:alnum:]]', '', 'g') ILIKE :q_clean_like
                        OR regexp_replace(code,   '[^[:alnum:]]', '', 'g') ILIKE :q_clean_like
                        OR name  ILIKE :q_raw_like
                    ORDER BY
                        (regexp_replace(unicode, '[^[:alnum:]]', '', 'g') = :q_clean) DESC,
                        price_eur ASC
                    LIMIT :limit_val
                    """
                )

            rows = conn.execute(
                sql_query,
                {
                    "q_clean_like": f"%{q_clean}%",  # Для коду/унікоду (OF935)
                    "q_raw_like": f"%{q_raw}%",      # Для бренду/опису (як ввів користувач)
                    "q_clean": q_clean,              # Для ORDER BY (точні збіги вгору)
                    "limit_val": limit,
                },
            )

            for row in rows:
                results.append(dict(row._mapping))

        elapsed_ms = (time.perf_counter() - t0) * 1000
        response.headers["X-Search-Ms"] = f"{elapsed_ms:.1f}"
        print(f"[INFO] API Search found {len(results)} items in {elapsed_ms:.1f} ms")
        return results

    except Exception as e:
        print(f"[ERROR] Database search failed: {e}")
        raise HTTPException(status_code=500, detail=str(e))