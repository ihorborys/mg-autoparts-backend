# from fastapi import APIRouter, HTTPException
# from pydantic import BaseModel
#
# # Імпортуємо функцію обробки з сусідньої папки (тому дві крапки ..)
# from app.etl.price_manager import process_all_prices
#
# # Створюємо роутер замість цілого додатку FastAPI
# router = APIRouter()
#
# # Модель для вхідних даних (перенесли сюди)
# class ImportAllRequest(BaseModel):
#     remote_gz_path: str
#     supplier: str  # напр. "AP_GDANSK"
#
# # Визначаємо маршрут.
# # Зверніть увагу: ми пишемо просто "/import-all", а не "/admin/import-all".
# # Префікс "/admin" ми додамо в головному файлі main.py.
# @router.post("/import-all")
# def import_all(req: ImportAllRequest):
#     try:
#         print(f"[INFO] Admin received import request for: {req.supplier}")
#         # Викликаємо функцію, яка запустить обробку
#         results = process_all_prices(req.supplier, req.remote_gz_path)
#         return {"supplier": req.supplier, "results": results}
#     except Exception as e:
#         print(f"[ERROR] Import failed: {e}")
#         raise HTTPException(status_code=500, detail=f"Import failed: {str(e)}")

from fastapi import APIRouter, HTTPException
from pydantic import BaseModel
from typing import Optional, Dict, Any

# Імпортуємо функцію обробки
from app.etl.price_manager import process_all_prices

router = APIRouter()

# --- УНІВЕРСАЛЬНА МОДЕЛЬ ---
class ImportAllRequest(BaseModel):
    supplier: str  # Обов'язково (напр. "AP_GDANSK")
    remote_gz_path: Optional[str] = None  # Для одного архіву (Maxgear)
    files: Optional[Dict[str, str]] = None  # Для кількох файлів (напр. {"prices": "p.csv", "stock": "s.csv"})

@router.post("/import-all")
def import_all(req: ImportAllRequest):
    try:
        print(f"[INFO] Admin received import request for: {req.supplier}")

        # Тепер ми передаємо і шлях до архіву, і словник файлів.
        # Функція process_all_prices сама вирішить, що з цього використовувати.
        results = process_all_prices(
            supplier=req.supplier,
            remote_gz_path=req.remote_gz_path,
            additional_files=req.files
        )

        return {"supplier": req.supplier, "results": results}

    except Exception as e:
        print(f"[ERROR] Import failed: {e}")
        raise HTTPException(status_code=500, detail=f"Import failed: {str(e)}")