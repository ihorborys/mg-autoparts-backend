from dotenv import load_dotenv
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
import uvicorn

# --- ІМПОРТ РОУТЕРІВ ---
# Ми імпортуємо наші модулі з папки routers
# Якщо ви вже створили search.py на попередньому кроці, розкоментуйте цей рядок:
from app.api.routers import search, admin

# -----------------------

# Завантаження змінних оточення
load_dotenv()

app = FastAPI(title="Maxgear API")

# === ДОДАТИ ЦЕЙ БЛОК ===
origins = [
    "https://mg-autoparts-frontend.vercel.app"
    "http://localhost:5173", # Адреса вашого Vite фронтенду (перевірте порт при запуску)
    "http://127.0.0.1:5173",
    "http://localhost:5174"
]

# Налаштування CORS (щоб фронтенд міг робити запити)
app.add_middleware(
    CORSMiddleware,
    allow_origins=origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# --- ПІДКЛЮЧЕННЯ РОУТЕРІВ ---

# 1. Підключаємо адмінські маршрути.
# Всі маршрути з файлу routers/admin.py будуть починатися з префіксу /admin
# Наприклад: /admin/import-all
app.include_router(admin.router, prefix="/admin", tags=["admin"])

# 2. Підключаємо пошукові маршрути (якщо файл search.py існує).
# Всі маршрути будуть починатися з /api
# Наприклад: /api/search
# Якщо ви вже створили search.py, розкоментуйте цей рядок:
app.include_router(search.router, prefix="/api", tags=["search"])

# ----------------------------

@app.get("/")
def root():
    """Проста перевірка, що сервер працює"""
    return {"message": "Maxgear API is running! Go to /docs to see API"}

if __name__ == "__main__":
    # Запуск сервера для локальної розробки
    # reload=True автоматично перезапускає сервер при зміні коду
    uvicorn.run("app.main:app", host="0.0.0.0", port=8000, reload=True)