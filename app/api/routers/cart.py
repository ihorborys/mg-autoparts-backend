# from fastapi import APIRouter, HTTPException
# from pydantic import BaseModel
# from sqlalchemy import text
# from app.database import engine  # Перевір, чи шлях до engine правильний
#
# router = APIRouter()  # Префікс /cart ми вже додали в main.py
#
#
# class CartItemIn(BaseModel):
#     user_id: str
#     supplier_id: int
#     code: str
#     brand: str
#     name: str
#     quantity: int
#     price_eur: float
#
#
# @router.post("/")
# async def add_to_cart(item: CartItemIn):
#     try:
#         with engine.connect() as conn:
#             # Магія UPSERT: один запит для додавання або оновлення
#             query = text("""
#                 INSERT INTO cart_items (user_id, supplier_id, code, brand, name, quantity, price_eur)
#                 VALUES (:u_id, :s_id, :code, :brand, :name, :qty, :price)
#                 ON CONFLICT (user_id, supplier_id, code)
#                 DO UPDATE SET
#                     quantity = cart_items.quantity + EXCLUDED.quantity,
#                     price_eur = EXCLUDED.price_eur,
#                     created_at = NOW()
#                 RETURNING quantity;
#             """)
#
#             conn.execute(query, {
#                 "u_id": item.user_id,
#                 "s_id": item.supplier_id,
#                 "code": item.code,
#                 "brand": item.brand,
#                 "name": item.name,
#                 "qty": item.quantity,
#                 "price": item.price_eur
#             })
#             conn.commit()
#
#         return {"status": "success", "message": "Item updated in cart"}
#     except Exception as e:
#         print(f"Cart Error: {e}")
#         raise HTTPException(status_code=500, detail="Internal Server Error")

from fastapi import APIRouter, HTTPException
from pydantic import BaseModel
from sqlalchemy import text
from app.database import engine

router = APIRouter()


# Модель для вхідних даних (додавання в кошик)
class CartItemIn(BaseModel):
    user_id: str
    supplier_id: int
    code: str
    brand: str
    name: str
    quantity: int
    price_eur: float


@router.post("/")
async def add_to_cart(item: CartItemIn):
    try:
        with engine.connect() as conn:
            # ОДИН запит: робить запис/оновлення та повертає сток саме цього бренду
            query = text("""
                WITH upserted AS (
                    INSERT INTO cart_items (user_id, supplier_id, code, brand, name, quantity, price_eur)
                    VALUES (:u_id, :s_id, :code, :brand, :name, :qty, :price)
                    ON CONFLICT (user_id, supplier_id, code, brand) -- ДОДАЛИ brand
                    DO UPDATE SET 
                        quantity = cart_items.quantity + EXCLUDED.quantity,
                        price_eur = EXCLUDED.price_eur,
                        created_at = NOW()
                    RETURNING quantity
                )
                SELECT 
                    (SELECT quantity FROM upserted LIMIT 1) as new_quantity,
                    (SELECT stock FROM product_catalog 
                     WHERE code = :code AND supplier_id = :s_id AND brand = :brand 
                     LIMIT 1) as stock; -- ДОДАЛИ brand та LIMIT 1
            """)

            # Виконуємо запит
            result = conn.execute(query, {
                "u_id": item.user_id,
                "s_id": item.supplier_id,
                "code": item.code,
                "brand": item.brand,
                "name": item.name,
                "qty": item.quantity,
                "price": item.price_eur
            }).mappings().first()

            conn.commit()

        # Безпечно витягуємо дані
        stock_value = result["stock"] if result["stock"] is not None else 0
        new_qty = result["new_quantity"]

        return {
            "status": "success",
            "message": "Кошик оновлено",
            "new_quantity": new_qty,
            "stock": stock_value
        }
    except Exception as e:
        print(f"Cart POST Error: {e}")
        # Повертаємо текст помилки, щоб ти бачив її в Network браузера
        raise HTTPException(status_code=500, detail=str(e))


# --- 2. ОТРИМАННЯ КОШИКА (ДЛЯ СТОРІНКИ КОШИКА) ---
@router.get("/{user_id}")
async def get_cart(user_id: str):
    try:
        with engine.connect() as conn:
            # Робимо LEFT JOIN з таблицею product_catalog за кодом та постачальником
            query = text("""
                            SELECT 
                                c.id, c.supplier_id, c.code, c.brand, c.name, c.quantity, c.price_eur, c.created_at,
                                p.stock as stock
                            FROM cart_items c
                            LEFT JOIN product_catalog p ON c.code = p.code AND c.supplier_id = p.supplier_id
                            WHERE c.user_id = :u_id
                            ORDER BY c.id ASC  -- Сортуємо за ID (від старих до нових)
                        """)

            rows = conn.execute(query, {"u_id": user_id})
            items = [dict(row._mapping) for row in rows]

            # Рахуємо загальну суму в EUR прямо тут (це швидше, ніж на фронтенді)
            total_eur = sum(item['price_eur'] * item['quantity'] for item in items)

        return {
            "user_id": user_id,
            "items": items,
            "total_items": len(items),
            "total_price_eur": round(total_eur, 2)
        }
    except Exception as e:
        print(f"Cart GET Error: {e}")
        raise HTTPException(status_code=500, detail="Не вдалося завантажити кошик")


# --- 3. ОНОВЛЕННЯ КІЛЬКОСТІ (ТОЧНЕ ЗНАЧЕННЯ) ---
# Використовується, коли юзер змінює кількість в самому кошику (input або +/-)
@router.patch("/update")
async def update_quantity(user_id: str, supplier_id: int, code: str, quantity: int):
    if quantity < 1:
        raise HTTPException(status_code=400, detail="Кількість не може бути менше 1")

    try:
        with engine.connect() as conn:
            # 1. Перевіряємо актуальний залишок у каталозі
            stock_query = text("""
                    SELECT stock FROM product_catalog 
                    WHERE code = :code AND supplier_id = :s_id
                """)
            stock_res = conn.execute(stock_query, {"code": code, "s_id": supplier_id}).scalar()

            # 2. Якщо юзер хоче більше, ніж є — видаємо помилку або ставимо ліміт
            final_qty = quantity
            if stock_res is not None and quantity > stock_res:
                # Можна або викинути помилку:
                # raise HTTPException(status_code=400, detail=f"На складі лише {stock_res} шт.")
                # Або просто прирівняти до максимуму:
                final_qty = stock_res

            # 3. Оновлюємо
            query = text("""
                    UPDATE cart_items 
                    SET quantity = :qty
                    WHERE user_id = :u_id AND supplier_id = :s_id AND code = :code
                """)
            conn.execute(query, {"qty": final_qty, "u_id": user_id, "s_id": supplier_id, "code": code})
            conn.commit()
        return {
            "status": "success",
            "message": "Кількість оновлено",
            "final_quantity": final_qty  # <--- ДОДАЙ ЦЕ
        }
    except Exception as e:
        print(f"Cart PATCH Error: {e}")
        raise HTTPException(status_code=500, detail="Не вдалося оновити кількість")


# --- 4. ВИДАЛЕННЯ ОДНОГО ТОВАРУ ---
@router.delete("/{user_id}/{supplier_id}/{code}")
async def remove_item(user_id: str, supplier_id: int, code: str):
    try:
        with engine.connect() as conn:
            query = text("""
                DELETE FROM cart_items 
                WHERE user_id = :u_id AND supplier_id = :s_id AND code = :code
            """)
            conn.execute(query, {"u_id": user_id, "s_id": supplier_id, "code": code})
            conn.commit()
        return {"status": "success", "message": "Товар видалено з кошика"}
    except Exception as e:
        print(f"Cart DELETE Item Error: {e}")
        raise HTTPException(status_code=500, detail="Не вдалося видалити товар")


# --- 5. ОЧИЩЕННЯ ВСЬОГО КОШИКА ---
@router.delete("/{user_id}")
async def clear_cart(user_id: str):
    try:
        with engine.connect() as conn:
            query = text("DELETE FROM cart_items WHERE user_id = :u_id")
            conn.execute(query, {"u_id": user_id})
            conn.commit()
        return {"status": "success", "message": "Кошик очищено"}
    except Exception as e:
        print(f"Cart CLEAR Error: {e}")
        raise HTTPException(status_code=500, detail="Не вдалося очистити кошик")