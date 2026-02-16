from fastapi import APIRouter, Query, HTTPException
from typing import List, Dict, Any
from sqlalchemy import create_engine, text

# Створюємо роутер (маршрутизатор) для пошукових запитів
router = APIRouter()

# --- НАЛАШТУВАННЯ БАЗИ ДАНИХ ---
# (Таке саме, як у price_processor.py)
DB_USER = "postgres"
# ВАЖЛИВО: Впишіть сюди ваш пароль!
DB_PASSWORD = "123456789"
DB_HOST = "localhost"
DB_PORT = "5432"
DB_NAME = "postgres"

DATABASE_URL = f"postgresql+psycopg2://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
# -------------------------------


@router.get("/search", response_model=List[Dict[str, Any]])
def search_products(
    q: str = Query(..., min_length=2, description="Пошуковий запит (мінімум 2 символи)"),
    limit: int = Query(50, ge=1, le=200, description="Максимальна кількість результатів")
):
    """
    Шукає товари в базі даних за артикулом (code), назвою (name) або брендом (brand).
    Використовує нечутливий до регістру пошук (ILIKE).
    """
    if not q:
         return []

    print(f"[INFO] API Search request: '{q}'")

    try:
        # Створюємо з'єднання з базою для цього запиту
        engine = create_engine(DATABASE_URL)

        # SQL-запит для пошуку.
        # Використовуємо ILIKE та %...% для пошуку по входженню рядка без урахування регістру.
        # ВИПРАВЛЕНО: price замінено на price_EUR у SELECT та ORDER BY
        sql_query = text("""
                    SELECT supplier_id, code, unicode, brand, name, stock, price_eur
                    FROM product_catalog
                    WHERE
                        code ILIKE :search_term
                        OR name ILIKE :search_term
                        OR brand ILIKE :search_term
                    ORDER BY price_eur ASC -- Сортуємо за правильною колонкою
                    LIMIT :limit_val
                """)
        results = []
        with engine.connect() as conn:
            # Виконуємо запит, передаючи параметри безпечно (щоб уникнути SQL-ін'єкцій)
            rows = conn.execute(
                sql_query,
                {"search_term": f"%{q}%", "limit_val": limit}
            )

            # Перетворюємо результати з формату бази даних у список словників (JSON)
            # SQLAlchemy row._mapping перетворює рядок на словник {колонки: значення}
            for row in rows:
                results.append(dict(row._mapping))

        print(f"[INFO] API Search found {len(results)} items for '{q}'")
        return results

    except Exception as e:
        print(f"[ERROR] Database search failed: {e}")
        # Повертаємо помилку клієнту, якщо щось пішло не так з базою
        raise HTTPException(status_code=500, detail=f"Database search error: {str(e)}")