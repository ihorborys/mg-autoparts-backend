from pathlib import Path

# 1. Визначаємо, де лежить цей файл (backend/app/services/paths.py)
CURRENT_FILE = Path(__file__).resolve()

# 2. Піднімаємось на 2 рівні вгору, щоб знайти папку 'app'
# services -> app
APP_DIR = CURRENT_FILE.parent.parent

# 3. Визначаємо корінь проєкту 'backend' (для папки data)
# app -> backend
PROJECT_ROOT = APP_DIR.parent

# ===========================================
# CONFIG PATHS (Тепер це працюватиме!)
# ===========================================
# Шукаємо: backend/app/config
CONFIG_DIR = APP_DIR / "config"

# ===========================================
# DATA PATHS
# ===========================================
# Краще зберігати дані в backend/data (поруч з app, а не всередині)
BASE_DATA_DIR = PROJECT_ROOT / "data"

TEMP_DIR = BASE_DATA_DIR / "temp"
STATE_DIR = BASE_DATA_DIR / "state"  # Для файлів стану (якщо треба)

# Гарантуємо, що папки існують
TEMP_DIR.mkdir(parents=True, exist_ok=True)
STATE_DIR.mkdir(parents=True, exist_ok=True)

# ===========================================
# ДІАГНОСТИКА (Щоб ти міг перевірити)
# ===========================================
if __name__ == "__main__":
    print(f"📂 APP_DIR:    {APP_DIR}")
    print(f"⚙️ CONFIG_DIR: {CONFIG_DIR}")
    print(f"💾 DATA_DIR:   {BASE_DATA_DIR}")

    # Перевірка існування
    if CONFIG_DIR.exists():
        print("✅ Папка config знайдена!")
    else:
        print("❌ Папка config НЕ знайдена! Перевір шляхи.")