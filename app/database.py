import os
from dotenv import load_dotenv
from sqlalchemy import create_engine, text

# 1. Завантажуємо змінні
load_dotenv()

# 2. Логіка формування DATABASE_URL
raw_url = os.getenv("DATABASE_URL")

if raw_url:
    if raw_url.startswith("postgresql://"):
        DATABASE_URL = raw_url.replace("postgresql://", "postgresql+psycopg2://", 1)
    else:
        DATABASE_URL = raw_url
else:
    # Fallback для локальної розробки
    DB_USER = os.getenv("DB_USER", "postgres")
    DB_PASSWORD = os.getenv("DB_PASSWORD", "")
    DB_HOST = os.getenv("DB_HOST", "localhost")
    DB_PORT = os.getenv("DB_PORT", "5432")
    DB_NAME = os.getenv("DB_NAME", "postgres")
    DATABASE_URL = f"postgresql+psycopg2://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"

# 3. Створення ENGINE (один на весь додаток)
engine = create_engine(
    DATABASE_URL,
    pool_pre_ping=True,
    pool_size=10,
    max_overflow=20
)

# 4. Функція створення таблиць (щоб не робити руками в Supabase)
def init_db():
    query = text("""
    CREATE TABLE IF NOT EXISTS translations_dictionary (
        id SERIAL PRIMARY KEY,
        pl_text TEXT NOT NULL UNIQUE, 
        uk_text TEXT NOT NULL,         
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    );
    CREATE INDEX IF NOT EXISTS idx_pl_text ON translations_dictionary (pl_text);
    """)
    try:
        with engine.connect() as conn:
            conn.execute(query)
            conn.commit()
            print("[INFO] Database Check: 'translations_dictionary' is ready.")
    except Exception as e:
        print(f"[ERROR] DB initialization failed: {e}")