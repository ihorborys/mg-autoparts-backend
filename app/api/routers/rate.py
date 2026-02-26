from fastapi import APIRouter
from app.services.exchange import get_eur_to_uah  # Твій імпорт уже правильний

# Створюємо роутер
router = APIRouter()

@router.get("/get-rate")  # Префікс /api зазвичай додається в main.py
def get_rate():
    rate = get_eur_to_uah()
    return {"rate": rate}