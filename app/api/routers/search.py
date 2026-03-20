import time
import re
from fastapi import APIRouter, Query, HTTPException, Response
from typing import List, Dict, Any
from sqlalchemy import text

# ІМПОРТУЄМО ENGINE З НАШОГО НОВОГО ФАЙЛУ
from app.database import engine

router = APIRouter()


@router.get("/search", response_model=List[Dict[str, Any]])
def search_products(
        response: Response,
        q: str = Query(..., min_length=2, description="Пошуковий запит"),
        limit: int = Query(50, ge=1, le=200),
        offset: int = Query(0, ge=0)

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

    # ⏱️ СТАРТ ЗАГАЛЬНОГО ТАЙМЕРА
    t_start_total = time.perf_counter()

    try:
        results: List[Dict[str, Any]] = []

        with engine.connect() as conn:
            # СЦЕНАРІЙ А: Два або більше слів (напр. "SACHS 315187")
            if len(words) >= 2:
                w1 = clean_val(words[0])
                w2 = clean_val("".join(words[1:]))
                full_combined = clean_val("".join(words))

                sql_query = text("""
                    SELECT supplier_id, code, unicode, brand, name, stock, price_eur
                    FROM product_catalog
                    WHERE 
                        (brand_norm LIKE :w1_p AND (code_norm LIKE :w2_p OR unicode_norm LIKE :w2_p))
                        OR
                        (brand_norm LIKE :w2_p AND (code_norm LIKE :w1_p OR unicode_norm LIKE :w1_p))
                        OR
                        (code_norm = :full OR unicode_norm = :full)
                    ORDER BY (stock > 0) DESC, price_eur ASC
                    LIMIT :limit_val OFFSET :offset_val
                """)
                params = {
                    "w1_p": f"{w1}%",
                    "w2_p": f"{w2}%",
                    "full": full_combined,
                    "limit_val": limit,
                    "offset_val": offset
                }

            # СЦЕНАРІЙ Б: Одне слово (напр. "GDB1330")
            else:
                sql_query = text("""
                    SELECT supplier_id, code, unicode, brand, name, stock, price_eur
                    FROM product_catalog
                    WHERE unicode_norm LIKE :q_p OR code_norm LIKE :q_p OR brand_norm LIKE :q_p
                    ORDER BY
                        (stock > 0) DESC,                                   -- 1. Спочатку те, що є в наявності
                        (unicode_norm = :q_c OR code_norm = :q_c) DESC,     -- 2. Потім точні збіги коду
                        price_eur ASC                                       -- 3. І ТЕПЕР за ціною!
                    LIMIT :limit_val OFFSET :offset_val
                """)
                params = {
                    "q_p": f"{q_clean_full}%",
                    "q_c": q_clean_full,
                    "limit_val": limit,
                    "offset_val": offset
                }

            # --- ⏱️ ВИМІРЮЄМО ЧИСТИЙ ЧАС SQL ---
            t_sql_start = time.perf_counter()
            rows = conn.execute(sql_query, params)
            t_sql_end = time.perf_counter()
            # ----------------------------------

            for row in rows:
                results.append(dict(row._mapping))

        # РОЗРАХУНОК МІЛІСЕКУНД
        sql_ms = (t_sql_end - t_sql_start) * 1000
        total_ms = (time.perf_counter() - t_start_total) * 1000

        # Додаємо в хедери відповіді
        response.headers["X-SQL-Execution-Ms"] = f"{sql_ms:.1f}"
        response.headers["X-Total-Search-Ms"] = f"{total_ms:.1f}"

        # Гарний лог у консоль
        print(f"🔍 [SEARCH] Запит: '{q_raw}' | | Offset: {offset} | Знайдено: {len(results)} | SQL: {sql_ms:.1f}ms | Total: {total_ms:.1f}ms")

        return results

    except Exception as e:
        print(f"[ERROR] Database search failed: {e}")
        raise HTTPException(status_code=500, detail=str(e))