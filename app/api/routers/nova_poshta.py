# import os
# import httpx
# from fastapi import APIRouter, HTTPException, Query
#
# router = APIRouter()
#
# NP_API_URL = "https://api.novaposhta.ua/v2.0/json/"
# NP_API_KEY = os.getenv("NOVA_POSHTA_API_KEY")
#
#
# async def np_request(model: str, method: str, properties: dict) -> dict:
#     """Базовий хелпер для запитів до НП API."""
#
#     # # ТИМЧАСОВИЙ ДЕБАГ:
#     # print(f"DEBUG: Відправляю запит з ключем: {NP_API_KEY}")
#
#     payload = {
#         "apiKey": NP_API_KEY,
#         "modelName": model,
#         "calledMethod": method,
#         "methodProperties": properties,
#     }
#     async with httpx.AsyncClient(timeout=10) as client:
#         response = await client.post(NP_API_URL, json=payload)
#         response.raise_for_status()
#         data = response.json()
#
#     if not data.get("success"):
#         errors = data.get("errors", ["Невідома помилка НП API"])
#         raise HTTPException(status_code=400, detail=errors)
#
#     return data
#
#
# # ───────────────────────────────────────────────
# # 1. ПОШУК МІСТ
# # GET /api/nova-poshta/cities?q=Льв
# # Повертає список міст які підходять під запит
# # ───────────────────────────────────────────────
#
# @router.get("/cities")
# async def search_cities(q: str = Query(..., min_length=2)):
#     try:
#         data = await np_request(
#             model="Address",
#             method="searchSettlements",
#             properties={
#                 "CityName": q,
#                 "Limit": 10,
#                 "Page": 1,
#             }
#         )
#
#         # НП повертає вкладену структуру — розпаковуємо
#         addresses = data.get("data", [])
#         if not addresses:
#             return []
#
#         settlements = addresses[0].get("Addresses", [])
#
#         # Повертаємо тільки те що потрібно фронту
#         return [
#             {
#                 "ref": s.get("DeliveryCity"),       # унікальний ID міста для запиту відділень
#                 "description": s.get("Present"),    # повна назва: "Львів, Львівська область"
#                 "city": s.get("MainDescription"),   # коротка назва: "Львів"
#             }
#             for s in settlements
#         ]
#
#     except HTTPException:
#         raise
#     except Exception as e:
#         print(f"[NP] cities error: {e}")
#         raise HTTPException(status_code=500, detail="Помилка пошуку міст")
#
#
# # ───────────────────────────────────────────────
# # 2. ВІДДІЛЕННЯ МІСТА
# # GET /api/nova-poshta/warehouses?city_ref=xxx
# # Повертає список відділень для вибраного міста
# # ───────────────────────────────────────────────
#
# @router.get("/warehouses")
# async def get_warehouses(city_ref: str = Query(...)):
#     try:
#         data = await np_request(
#             model="AddressGeneral",
#             method="getWarehouses",
#             properties={
#                 "CityRef": city_ref,
#                 "Limit": 10000,
#                 "Page": 1,
#             }
#         )
#
#         warehouses = data.get("data", [])
#
#         return [
#             {
#                 "ref": w.get("Ref"),
#                 "number": w.get("Number"),                          # номер відділення
#                 "description": w.get("Description"),               # повна назва відділення
#                 "short_address": w.get("ShortAddress"),            # коротка адреса
#             }
#             for w in warehouses
#         ]
#
#     except HTTPException:
#         raise
#     except Exception as e:
#         print(f"[NP] warehouses error: {e}")
#         raise HTTPException(status_code=500, detail="Помилка завантаження відділень")






import os
import httpx
from fastapi import APIRouter, HTTPException, Query

router = APIRouter()

NP_API_URL = "https://api.novaposhta.ua/v2.0/json/"
NP_API_KEY = os.getenv("NOVA_POSHTA_API_KEY")


async def np_request(model: str, method: str, properties: dict) -> dict:
    """Базовий хелпер для запитів до НП API."""
    payload = {
        "apiKey": NP_API_KEY,
        "modelName": model,
        "calledMethod": method,
        "methodProperties": properties,
    }
    async with httpx.AsyncClient(timeout=10) as client:
        response = await client.post(NP_API_URL, json=payload)
        response.raise_for_status()
        data = response.json()

    if not data.get("success"):
        errors = data.get("errors", ["Невідома помилка НП API"])
        raise HTTPException(status_code=400, detail=errors)

    return data


# ───────────────────────────────────────────────
# 1. ПОШУК МІСТ
# GET /api/nova-poshta/cities?q=Льв
# ───────────────────────────────────────────────

@router.get("/cities")
async def search_cities(q: str = Query(..., min_length=2)):
    try:
        data = await np_request(
            model="Address",
            method="searchSettlements",
            properties={
                "CityName": q,
                "Limit": 10,
                "Page": 1,
            }
        )

        addresses = data.get("data", [])
        if not addresses:
            return []

        settlements = addresses[0].get("Addresses", [])

        return [
            {
                "ref": s.get("DeliveryCity"),
                "description": s.get("Present"),    # "Львів, Львівська область"
                "city": s.get("MainDescription"),   # "Львів"
            }
            for s in settlements
        ]

    except HTTPException:
        raise
    except Exception as e:
        print(f"[NP] cities error: {e}")
        raise HTTPException(status_code=500, detail="Помилка пошуку міст")


# ───────────────────────────────────────────────
# 2. ПОШУК ВІДДІЛЕНЬ У МІСТІ
# GET /api/nova-poshta/warehouses/search?city_ref=xxx&q=14
#
# Шукає по номеру відділення АБО по назві вулиці.
# Використовуємо замість завантаження всіх відділень —
# це критично для великих міст де відділень до 1000+
# ───────────────────────────────────────────────

@router.get("/warehouses/search")
async def search_warehouses(
    city_ref: str = Query(...),
    q: str = Query(..., min_length=1)
):
    try:
        data = await np_request(
            model="AddressGeneral",
            method="getWarehouses",
            properties={
                "CityRef": city_ref,
                "FindByString": q,   # ← правильний параметр
                "Limit": 20,
                "Page": 1,
            }
        )
        warehouses = data.get("data", [])
        return [
            {
                "ref": w.get("Ref"),
                "number": w.get("Number"),
                "description": w.get("Description"),
                "short_address": w.get("ShortAddress"),
            }
            for w in warehouses
        ]
    except HTTPException:
        raise
    except Exception as e:
        print(f"[NP] warehouse search error: {e}")
        raise HTTPException(status_code=500, detail="Помилка пошуку відділень")
