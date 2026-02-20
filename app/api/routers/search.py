import os
from dotenv import load_dotenv
from fastapi import APIRouter, Query, HTTPException
from typing import List, Dict, Any
from sqlalchemy import create_engine, text

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
    if not q:
        return []

    print(f"[INFO] API Search request: '{q}'")

    try:
        # Тепер ми просто використовуємо вже готовий engine
        sql_query = text("""
                    SELECT supplier_id, code, unicode, brand, name, stock, price_eur
                    FROM product_catalog
                    WHERE
                        code ILIKE :search_term
                        OR name ILIKE :search_term
                        OR brand ILIKE :search_term
                    ORDER BY price_eur ASC
                    LIMIT :limit_val
                """)

        results = []
        # 'with engine.connect()' автоматично бере вільне з'єднання з пулу
        with engine.connect() as conn:
            rows = conn.execute(
                sql_query,
                {"search_term": f"%{q}%", "limit_val": limit}
            )

            for row in rows:
                results.append(dict(row._mapping))

        print(f"[INFO] API Search found {len(results)} items for '{q}'")
        return results

    except Exception as e:
        print(f"[ERROR] Database search failed: {e}")
        raise HTTPException(status_code=500, detail="Database search error")