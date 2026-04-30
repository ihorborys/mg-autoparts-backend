# from fastapi import APIRouter, HTTPException
# from sqlalchemy import text
# # Імпортуємо наші змінні назв таблиць
# from app.database import engine, TABLE_CART, TABLE_CATALOG
# from app.services.email_service import EmailService # Імпортуємо наш новий сервіс
# from pydantic import BaseModel
# from typing import Optional
#
#
#
# router = APIRouter()
#
#
# # Модель для вхідних даних (додавання в кошик)
# class CartItemIn(BaseModel):
#     user_id: str
#     product_id: int
#     supplier_id: int
#     code: str
#     brand: str
#     name: str
#     quantity: int
#     price_eur: float
#
#
# # --- 1. ДОДАВАННЯ ТОВАРУ (З ПІДТРИМКОЮ ТОСТІВ) ---
# @router.post("/")
# async def add_to_cart(item: CartItemIn):
#     try:
#         with engine.connect() as conn:
#             # Використовуємо RETURNING quantity, щоб отримати фінальну кількість після UPSERT
#             query = text(f"""
#                 INSERT INTO {TABLE_CART} (user_id, product_id, supplier_id, code, brand, name, quantity, price_eur)
#                 VALUES (:u_id, :p_id, :s_id, :code, :brand, :name, :qty, :price)
#                 ON CONFLICT (user_id, supplier_id, code, brand)
#                 DO UPDATE SET
#                     quantity = {TABLE_CART}.quantity + EXCLUDED.quantity,
#                     price_eur = EXCLUDED.price_eur,
#                     product_id = EXCLUDED.product_id,
#                     created_at = NOW()
#                 RETURNING quantity;
#             """)
#
#             result = conn.execute(query, {
#                 "u_id": item.user_id,
#                 "p_id": item.product_id,  # <--- ПЕРЕДАЄМО ЧИСЛО
#                 "s_id": item.supplier_id,
#                 "code": item.code,
#                 "brand": item.brand,
#                 "name": item.name,
#                 "qty": item.quantity,
#                 "price": item.price_eur
#             })
#
#             # Отримуємо нове значення кількості
#             new_quantity = result.scalar()
#             conn.commit()
#
#         return {
#             "status": "success",
#             "message": "Кошик оновлено",
#             "new_quantity": new_quantity
#         }
#     except Exception as e:
#         print(f"Cart POST Error: {e}")
#         raise HTTPException(status_code=500, detail="Помилка при додаванні в кошик")
#
#
# # --- 2. ОТРИМАННЯ КОШИКА (ДЛЯ СТОРІНКИ КОШИКА) ---
# @router.get("/{user_id}")
# async def get_cart(user_id: str):
#     try:
#         with engine.connect() as conn:
#             query = text(f"""
#                 SELECT
#                     c.id,
#                     c.product_id,
#                     c.supplier_id,
#                     c.code,
#                     c.brand,
#                     c.name,
#                     c.quantity,
#                     c.price_eur,
#                     c.created_at,
#                     COALESCE(p.stock, 0) as stock
#                 FROM {TABLE_CART} c
#                 LEFT JOIN {TABLE_CATALOG} p ON p.id = c.product_id
#                 WHERE c.user_id = :u_id
#                 ORDER BY c.created_at DESC
#             """)
#
#             rows = conn.execute(query, {"u_id": user_id})
#             items = [dict(row._mapping) for row in rows]
#
#             # Рахуємо загальну суму в EUR прямо тут (це швидше, ніж на фронтенді)
#             total_eur = sum(item['price_eur'] * item['quantity'] for item in items)
#
#         return {
#             "user_id": user_id,
#             "items": items,
#             "total_items": len(items),
#             "total_price_eur": round(total_eur, 2)
#         }
#     except Exception as e:
#         print(f"Cart GET Error: {e}")
#         raise HTTPException(status_code=500, detail="Не вдалося завантажити кошик")
#
#
# # --- 3. ОНОВЛЕННЯ КІЛЬКОСТІ (ТОЧНЕ ЗНАЧЕННЯ) ---
# # Використовується, коли юзер змінює кількість в самому кошику (input або +/-)
# @router.patch("/update")
# async def update_quantity(user_id: str, supplier_id: int, code: str, quantity: int):
#     if quantity < 1:
#         raise HTTPException(status_code=400, detail="Кількість не може бути менше 1")
#
#     try:
#         with engine.connect() as conn:
#             query = text(f"""
#                 UPDATE {TABLE_CART}
#                 SET quantity = :qty, created_at = NOW()
#                 WHERE user_id = :u_id AND supplier_id = :s_id AND code = :code
#             """)
#             conn.execute(query, {"qty": quantity, "u_id": user_id, "s_id": supplier_id, "code": code})
#             conn.commit()
#         return {"status": "success", "message": "Кількість оновлено"}
#     except Exception as e:
#         print(f"Cart PATCH Error: {e}")
#         raise HTTPException(status_code=500, detail="Не вдалося оновити кількість")
#
#
# # --- 4. ВИДАЛЕННЯ ОДНОГО ТОВАРУ ---
# @router.delete("/{user_id}/{supplier_id}/{code}")
# async def remove_item(user_id: str, supplier_id: int, code: str):
#     try:
#         with engine.connect() as conn:
#             query = text(f"""
#                 DELETE FROM {TABLE_CART}
#                 WHERE user_id = :u_id AND supplier_id = :s_id AND code = :code
#             """)
#             conn.execute(query, {"u_id": user_id, "s_id": supplier_id, "code": code})
#             conn.commit()
#         return {"status": "success", "message": "Товар видалено з кошика"}
#     except Exception as e:
#         print(f"Cart DELETE Item Error: {e}")
#         raise HTTPException(status_code=500, detail="Не вдалося видалити товар")
#
#
# # --- 5. ОЧИЩЕННЯ ВСЬОГО КОШИКА ---
# @router.delete("/{user_id}")
# async def clear_cart(user_id: str):
#     try:
#         with engine.connect() as conn:
#             query = text(f"DELETE FROM {TABLE_CART} WHERE user_id = :u_id")
#             conn.execute(query, {"u_id": user_id})
#             conn.commit()
#         return {"status": "success", "message": "Кошик очищено"}
#     except Exception as e:
#         print(f"Cart CLEAR Error: {e}")
#         raise HTTPException(status_code=500, detail="Не вдалося очистити кошик")
#
#
# # Модель для валідації (можна винести в окремий файл models.py або лишити тут)
# class OrderSchema(BaseModel):
#     order_id: str
#     full_user_name: str
#     first_name: str
#     last_name: str
#     user_email: str
#     user_phone: str
#     delivery_info: str
#     payment_method: str
#     total_price_eur: float
#     total_price_uah: int  # Основна сума для листа
#     notes: Optional[str] = ""  # Якщо прийде null або нічого, буде порожній рядок
#     items: list[dict]  # В середині цих dict тепер буде price_uah
#
# # --- 6. ОФОРМЛЕННЯ ЗАМОВЛЕННЯ ТА ОЧИЩЕННЯ КОШИКА ---
# @router.post("/checkout")
# async def checkout(order: OrderSchema):  # Використовуємо схему замість dict
#     # Тепер FastAPI знає, що всередині 'order' є id, name та ціна.
#
#     # 1. Твоя логіка збереження замовлення в БД (якщо вона тут є)
#     # ...
#
#     # 2. Відправка сповіщення
#     # Передаємо дані у сервіс як словник через .dict()
#     email_sent = EmailService.send_order_confirmation(order.dict())
#
#     return {
#         "status": "success",
#         "order_id": order.order_id,
#         "email_notified": email_sent
#     }



from fastapi import APIRouter, HTTPException, BackgroundTasks
from sqlalchemy import text
from app.database import engine, TABLE_CART, TABLE_CATALOG, TABLE_ORDERS, TABLE_ORDER_ITEMS, TABLE_PROFILES
from app.services.email_service import EmailService
from pydantic import BaseModel
from typing import Optional

router = APIRouter()


# ───────────────────────────────────────────────
# МОДЕЛІ
# ───────────────────────────────────────────────

class CartItemIn(BaseModel):
    user_id: str
    product_id: int
    supplier_id: int
    code: str
    brand: str
    name: str
    quantity: int
    price_eur: float


class OrderItemSchema(BaseModel):
    product_id: int
    supplier_id: int
    code: str
    brand: str
    price_eur: float
    quantity: int


# НОВА МОДЕЛЬ: тепер фронт передає всі дані замовлення сюди
# і бекенд сам пише в orders + order_items + profiles + відправляє email
class CreateOrderSchema(BaseModel):
    user_id: str
    first_name: str
    last_name: str
    user_email: str
    user_phone: str
    ship_city: str
    ship_method: str
    ship_branch: str
    payment_method: str
    total_price_eur: float
    total_price_uah: int
    notes: Optional[str] = ""
    items: list[OrderItemSchema]


# class OrderSchema(BaseModel):
#     order_id: str
#     full_user_name: str
#     first_name: str
#     last_name: str
#     user_email: str
#     user_phone: str
#     delivery_info: str
#     payment_method: str
#     total_price_eur: float
#     total_price_uah: int
#     notes: Optional[str] = ""
#     items: list[dict]


# ───────────────────────────────────────────────
# 1. ДОДАВАННЯ ТОВАРУ
# ───────────────────────────────────────────────

@router.post("/")
async def add_to_cart(item: CartItemIn):
    try:
        with engine.connect() as conn:
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
                "p_id": item.product_id,
                "s_id": item.supplier_id,
                "code": item.code,
                "brand": item.brand,
                "name": item.name,
                "qty": item.quantity,
                "price": item.price_eur
            })
            new_quantity = result.scalar()
            conn.commit()

        return {"status": "success", "message": "Кошик оновлено", "new_quantity": new_quantity}
    except Exception as e:
        print(f"Cart POST Error: {e}")
        raise HTTPException(status_code=500, detail="Помилка при додаванні в кошик")


# ───────────────────────────────────────────────
# 2. ОТРИМАННЯ КОШИКА
# ───────────────────────────────────────────────

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


# ───────────────────────────────────────────────
# 3. ОНОВЛЕННЯ КІЛЬКОСТІ
# ───────────────────────────────────────────────

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


# ───────────────────────────────────────────────
# 4. ВИДАЛЕННЯ ОДНОГО ТОВАРУ
# ───────────────────────────────────────────────

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


# ───────────────────────────────────────────────
# 5. ОЧИЩЕННЯ ВСЬОГО КОШИКА
# ───────────────────────────────────────────────

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


# ───────────────────────────────────────────────
# 6. НОВА ТОЧКА ВХОДУ: СТВОРЕННЯ ЗАМОВЛЕННЯ
#
# Фронт більше НЕ пише в Supabase напряму.
# Один запит сюди — і бекенд робить все:
#   - INSERT в orders
#   - INSERT в order_items
#   - UPDATE profiles
#   - відправка email
#   - очищення кошика
#
# Це повністю обходить Supabase JS клієнт і всі
# його проблеми з внутрішньою чергою та lock.
# ───────────────────────────────────────────────

@router.post("/create-order")
async def create_order(data: CreateOrderSchema, background_tasks: BackgroundTasks):
    try:
        with engine.connect() as conn:

            # КРОК 1: Створюємо замовлення, отримуємо id і order_number
            order_result = conn.execute(text(f"""
                INSERT INTO {TABLE_ORDERS} (
                    user_id, total_price_eur, total_price_uah, status,
                    payment_method, ship_first_name, ship_last_name,
                    ship_phone, ship_city, ship_method, ship_branch, ship_notes
                ) VALUES (
                    :user_id, :total_eur, :total_uah, 'new',
                    :payment, :first_name, :last_name,
                    :phone, :city, :method, :branch, :notes
                )
                RETURNING id, order_number;
            """), {
                "user_id": data.user_id,
                "total_eur": data.total_price_eur,
                "total_uah": data.total_price_uah,
                "payment": data.payment_method,
                "first_name": data.first_name,
                "last_name": data.last_name,
                "phone": data.user_phone,
                "city": data.ship_city,
                "method": data.ship_method,
                "branch": data.ship_branch,
                "notes": data.notes,
            })

            order_row = order_result.fetchone()
            order_id = order_row.id
            order_number = str(order_row.order_number).zfill(6)

            # КРОК 2: Вставляємо всі товари замовлення
            for item in data.items:
                conn.execute(text(f"""
                    INSERT INTO {TABLE_ORDER_ITEMS} (
                        order_id, product_id, supplier_id,
                        code, brand, price_eur, quantity
                    ) VALUES (
                        :order_id, :product_id, :supplier_id,
                        :code, :brand, :price_eur, :quantity
                    );
                """), {
                    "order_id": order_id,
                    "product_id": item.product_id,
                    "supplier_id": item.supplier_id,
                    "code": item.code,
                    "brand": item.brand,
                    "price_eur": item.price_eur,
                    "quantity": item.quantity,
                })

            # КРОК 3: Оновлюємо профіль юзера
            conn.execute(text(f"""
                INSERT INTO {TABLE_PROFILES} (
                    id, first_name, last_name, phone,
                    city, delivery_method, updated_at
                ) VALUES (
                    :id, :first_name, :last_name, :phone,
                    :city, :method, NOW()
                )
                ON CONFLICT (id) DO UPDATE SET
                    first_name = EXCLUDED.first_name,
                    last_name = EXCLUDED.last_name,
                    phone = EXCLUDED.phone,
                    city = EXCLUDED.city,
                    delivery_method = EXCLUDED.delivery_method,
                    updated_at = NOW();
            """), {
                "id": data.user_id,
                "first_name": data.first_name,
                "last_name": data.last_name,
                "phone": data.user_phone,
                "city": data.ship_city,
                "method": data.ship_method,
            })

            # КРОК 4: Очищаємо кошик юзера
            conn.execute(text(f"""
                DELETE FROM {TABLE_CART} WHERE user_id = :user_id
            """), {"user_id": data.user_id})

            conn.commit()

        # КРОК 5: Відправляємо email (після commit — замовлення вже збережено)
        delivery_info = (
            'Самовивіз (Самбір)' if data.ship_method == 'self'
            else f'НП: {data.ship_city}, №{data.ship_branch}'
        )

        email_payload = {
            "order_id": order_number,
            "full_user_name": f"{data.last_name} {data.first_name}".strip(),
            "first_name": data.first_name,
            "last_name": data.last_name,
            "user_email": data.user_email,
            "user_phone": data.user_phone,
            "delivery_info": delivery_info,
            "payment_method": data.payment_method,
            "total_price_eur": data.total_price_eur,
            "total_price_uah": data.total_price_uah,
            "notes": data.notes,
            "items": [item.dict() for item in data.items],
        }
        # Email у фоні через FastAPI BackgroundTasks — надійніше ніж asyncio.create_task
        background_tasks.add_task(EmailService.send_order_confirmation, email_payload)

        return {
            "status": "success",
            "order_number": order_number,
            "order_id": str(order_id),
        }

    except Exception as e:
        print(f"[CREATE ORDER ERROR]: {e}")
        raise HTTPException(status_code=500, detail=f"Помилка створення замовлення: {str(e)}")


# # ───────────────────────────────────────────────
# # 7. СТАРИЙ ENDPOINT ДЛЯ EMAIL (залишаємо для сумісності)
# # ───────────────────────────────────────────────
#
# @router.post("/checkout")
# async def checkout(order: OrderSchema):
#     email_sent = EmailService.send_order_confirmation(order.dict())
#     return {
#         "status": "success",
#         "order_id": order.order_id,
#         "email_notified": email_sent
#     }
