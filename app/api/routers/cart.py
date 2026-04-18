from fastapi import APIRouter, HTTPException
from sqlalchemy import text
# Імпортуємо наші змінні назв таблиць
from app.database import engine, TABLE_CART, TABLE_CATALOG
from app.services.email_service import EmailService # Імпортуємо наш новий сервіс
from pydantic import BaseModel



router = APIRouter()


# Модель для вхідних даних (додавання в кошик)
class CartItemIn(BaseModel):
    user_id: str
    product_id: int
    supplier_id: int
    code: str
    brand: str
    name: str
    quantity: int
    price_eur: float


# --- 1. ДОДАВАННЯ ТОВАРУ (З ПІДТРИМКОЮ ТОСТІВ) ---
@router.post("/")
async def add_to_cart(item: CartItemIn):
    try:
        with engine.connect() as conn:
            # Використовуємо RETURNING quantity, щоб отримати фінальну кількість після UPSERT
            query = text(f"""
                INSERT INTO {TABLE_CART} (user_id, product_id, supplier_id, code, brand, name, quantity, price_eur)
                VALUES (:u_id, :p_id, :s_id, :code, :brand, :name, :qty, :price)
                ON CONFLICT (user_id, supplier_id, code, brand) 
                DO UPDATE SET 
                    quantity = {TABLE_CART}.quantity + EXCLUDED.quantity,
                    price_eur = EXCLUDED.price_eur,
                    product_id = EXCLUDED.product_id,
                    created_at = NOW()
                RETURNING quantity;
            """)

            result = conn.execute(query, {
                "u_id": item.user_id,
                "p_id": item.product_id,  # <--- ПЕРЕДАЄМО ЧИСЛО
                "s_id": item.supplier_id,
                "code": item.code,
                "brand": item.brand,
                "name": item.name,
                "qty": item.quantity,
                "price": item.price_eur
            })

            # Отримуємо нове значення кількості
            new_quantity = result.scalar()
            conn.commit()

        return {
            "status": "success",
            "message": "Кошик оновлено",
            "new_quantity": new_quantity
        }
    except Exception as e:
        print(f"Cart POST Error: {e}")
        raise HTTPException(status_code=500, detail="Помилка при додаванні в кошик")


# --- 2. ОТРИМАННЯ КОШИКА (ДЛЯ СТОРІНКИ КОШИКА) ---
@router.get("/{user_id}")
async def get_cart(user_id: str):
    try:
        with engine.connect() as conn:
            query = text(f"""
                SELECT 
                    c.id, 
                    c.product_id,     
                    c.supplier_id, 
                    c.code, 
                    c.brand, 
                    c.name, 
                    c.quantity, 
                    c.price_eur, 
                    c.created_at,
                    COALESCE(p.stock, 0) as stock
                FROM {TABLE_CART} c
                LEFT JOIN {TABLE_CATALOG} p ON p.id = c.product_id
                WHERE c.user_id = :u_id
                ORDER BY c.created_at DESC
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
            query = text(f"""
                UPDATE {TABLE_CART} 
                SET quantity = :qty, created_at = NOW()
                WHERE user_id = :u_id AND supplier_id = :s_id AND code = :code
            """)
            conn.execute(query, {"qty": quantity, "u_id": user_id, "s_id": supplier_id, "code": code})
            conn.commit()
        return {"status": "success", "message": "Кількість оновлено"}
    except Exception as e:
        print(f"Cart PATCH Error: {e}")
        raise HTTPException(status_code=500, detail="Не вдалося оновити кількість")


# --- 4. ВИДАЛЕННЯ ОДНОГО ТОВАРУ ---
@router.delete("/{user_id}/{supplier_id}/{code}")
async def remove_item(user_id: str, supplier_id: int, code: str):
    try:
        with engine.connect() as conn:
            query = text(f"""
                DELETE FROM {TABLE_CART} 
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
            query = text(f"DELETE FROM {TABLE_CART} WHERE user_id = :u_id")
            conn.execute(query, {"u_id": user_id})
            conn.commit()
        return {"status": "success", "message": "Кошик очищено"}
    except Exception as e:
        print(f"Cart CLEAR Error: {e}")
        raise HTTPException(status_code=500, detail="Не вдалося очистити кошик")


# Модель для валідації (можна винести в окремий файл models.py або лишити тут)
class OrderSchema(BaseModel):
    order_id: str
    user_name: str
    user_email: str
    user_phone: str
    delivery_info: str
    total_price: float
    total_price_uah: float  # Основна сума для листа
    items: list[dict]  # В середині цих dict тепер буде price_uah

# --- 6. ОФОРМЛЕННЯ ЗАМОВЛЕННЯ ТА ОЧИЩЕННЯ КОШИКА ---
@router.post("/checkout")
async def checkout(order: OrderSchema):  # Використовуємо схему замість dict
    # Тепер FastAPI знає, що всередині 'order' є id, name та ціна.

    # 1. Твоя логіка збереження замовлення в БД (якщо вона тут є)
    # ...

    # 2. Відправка сповіщення
    # Передаємо дані у сервіс як словник через .dict()
    email_sent = EmailService.send_order_confirmation(order.dict())

    return {
        "status": "success",
        "order_id": order.order_id,
        "email_notified": email_sent
    }