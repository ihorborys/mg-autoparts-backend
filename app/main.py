from dotenv import load_dotenv
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from contextlib import asynccontextmanager
import uvicorn

# --- ІМПОРТ РОУТЕРІВ ---
from app.api.routers import search, prices, rate
# Імпортуємо функцію ініціалізації бази даних
from app.database import init_db

# Завантаження змінних оточення
load_dotenv()


# --- LIFESPAN (Автозапуск при старті) ---
@asynccontextmanager
async def lifespan(app: FastAPI):
    # Цей код спрацює ОДИН РАЗ при запуску сервера
    print("[STARTUP] Checking database and tables...")
    init_db()
    yield
    # Тут можна додати код для завершення роботи (якщо треба)


# Створюємо додаток із підключеним lifespan
app = FastAPI(title="Maxgear API", lifespan=lifespan)

# --- НАЛАШТУВАННЯ CORS ---
origins = [
    "https://mg-autoparts-frontend.vercel.app",
    "http://localhost:5173",
    "http://127.0.0.1:5173",
    "http://localhost:5174"
]

app.add_middleware(
    CORSMiddleware,
    allow_origins=origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# --- ПІДКЛЮЧЕННЯ РОУТЕРІВ ---
app.include_router(prices.router, prefix="/prices", tags=["prices"])
app.include_router(search.router, prefix="/api", tags=["search"])
app.include_router(rate.router, prefix="/api", tags=["exchange rate"])


@app.get("/")
def root():
    return {"message": "Maxgear API is running! Go to /docs to see API"}


if __name__ == "__main__":
    # Команда запуску тепер виглядає так: "app.main:app"
    # Це працює, якщо ти запускаєш скрипт з КОРЕНЕВОЇ папки проекту
    uvicorn.run("app.main:app", host="0.0.0.0", port=8000, reload=True)