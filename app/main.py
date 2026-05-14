from dotenv import load_dotenv
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from contextlib import asynccontextmanager
import uvicorn

# --- ІМПОРТ РОУТЕРІВ ---
from app.api.routers import search, prices, rate, cart, nova_poshta

load_dotenv()


@asynccontextmanager
async def lifespan(app: FastAPI):
    print("[STARTUP] Checking database and tables...")
    yield


app = FastAPI(title="Maxgear API", lifespan=lifespan)

# --- НАЛАШТУВАННЯ CORS ---
origins = [
    "https://mg-autoparts-frontend.vercel.app",
    "http://localhost:5173",
    "http://127.0.0.1:5173",
    "http://localhost:5174",
]

app.add_middleware(
    CORSMiddleware,
    allow_origins=origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# --- ПІДКЛЮЧЕННЯ РОУТЕРІВ ---

# 1. Каталог (Пошук)
app.include_router(search.router, prefix="/api/catalog", tags=["Catalog"])

# 2. Курси валют
app.include_router(rate.router, prefix="/api/rates", tags=["Rates"])

# 3. Кошик
app.include_router(cart.router, prefix="/api/cart", tags=["Cart"])

# 4. Адмінка (Імпорт цін)
app.include_router(prices.router, prefix="/api/admin/prices", tags=["Admin"])

# 5. Нова Пошта
app.include_router(nova_poshta.router, prefix="/api/nova-poshta", tags=["Nova Poshta"])


@app.get("/")
def root():
    return {"message": "Maxgear API is running! Go to /docs to see API"}


if __name__ == "__main__":
    uvicorn.run("app.main:app", host="0.0.0.0", port=8000, reload=True)
