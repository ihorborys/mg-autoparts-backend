from fastapi import APIRouter, HTTPException
from pydantic import BaseModel
from typing import Optional, Dict

# Імпортуємо функцію обробки
from app.etl.price_manager import process_all_prices

# Додаємо тег для документації Swagger
router = APIRouter()

class ImportRequest(BaseModel):
    supplier: str
    remote_gz_path: Optional[str] = None
    files: Optional[Dict[str, str]] = None

@router.post("/import")  # Буде доступно за адресою /prices/import
def run_price_import(req: ImportRequest):
    try:
        print(f"[INFO] Starting price import for: {req.supplier}")

        results = process_all_prices(
            supplier=req.supplier,
            remote_gz_path=req.remote_gz_path,
            additional_files=req.files
        )

        return {"status": "success", "supplier": req.supplier, "results": results}

    except Exception as e:
        print(f"[ERROR] Import failed: {e}")
        raise HTTPException(status_code=500, detail=f"Import failed: {str(e)}")