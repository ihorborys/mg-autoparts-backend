import os
import re
import gzip
import shutil
import yaml
import ftplib
from datetime import datetime
from typing import Tuple, List, Dict, Any, Optional
from pathlib import Path


import pandas as pd
# --- Імпорт text для безпечних SQL-запитів ---
from sqlalchemy import create_engine, text

from app.services.paths import TEMP_DIR
from app.services.storage import StorageClient

from re import sub


# ----------------------- FTP / unzip -----------------------

def download_file_from_ftp(remote_path: str, local_path: Path, supplier: str) -> None:
    """
    Завантажує файл з FTP, використовуючи динамічні секрети з .env
    на основі імені постачальника (напр. AUTOPARTNER_FTP_HOST).
    """
    # 1) Готуємо префікс для пошуку в .env (напр. "AUTOPARTNER")
    prefix = supplier.upper().replace(" ", "_")

    # 2) Витягуємо специфічні налаштування для цього постачальника
    host = os.getenv(f"{prefix}_FTP_HOST")
    user = os.getenv(f"{prefix}_FTP_USER")
    pwd = os.getenv(f"{prefix}_FTP_PASS")

    # Перевірка: якщо в .env забули прописати дані для цього постачальника
    if not all([host, user, pwd]):
        raise RuntimeError(f"Credentials for {prefix} are missing in .env. "
                           f"Please add {prefix}_FTP_HOST, {prefix}_FTP_USER, {prefix}_FTP_PASS.")

    print(f"[INFO] Connecting to FTP for {prefix} ({host})...")

    # Допоміжний виконавець (залишається майже без змін, але використовує локальні host/user/pwd)
    def _retr(ftp):
        ftp.set_pasv(True)  # Режим PASV (як у FileZilla)
        ftp.login(user, pwd)
        local_path.parent.mkdir(parents=True, exist_ok=True)
        with open(local_path, "wb") as f:
            ftp.retrbinary(f"RETR {remote_path}", f.write)
        ftp.quit()

    # 1) Спроба через Explicit TLS (FTPS) - більш безпечно
    try:
        ftps = ftplib.FTP_TLS(host, timeout=20)
        ftps.auth()
        ftps.prot_p()
        _retr(ftps)
        print(f"[SUCCESS] Downloaded via FTPS: {remote_path}")
        return
    except ftplib.all_errors as e_tls:
        # 2) Якщо TLS не доступний — пробуємо звичайний FTP
        try:
            print(f"[WARN] FTPS failed for {prefix}, trying plain FTP...")
            ftp = ftplib.FTP(host, timeout=20)
            _retr(ftp)
            print(f"[SUCCESS] Downloaded via FTP: {remote_path}")
            return
        except ftplib.all_errors as e_plain:
            raise RuntimeError(f"FTP/FTPS failed for {prefix}. TLS Error: {e_tls}; Plain Error: {e_plain}")



def unzip_gz_file(gz_file: Path, output_csv: Path) -> None:
    with gzip.open(gz_file, "rb") as f_in, open(output_csv, "wb") as f_out:
        shutil.copyfileobj(f_in, f_out)


# ----------------------- Config helpers -----------------------

def _config_dir() -> Path:
    # backend/app/price_processor.py -> backend/config/...
    return Path(__file__).resolve().parent.parent / "config"


def _load_supplier_cfg(supplier_name: str) -> dict:
    """Завантажує секцію постачальника з config/suppliers.yaml."""
    cfg_path = _config_dir() / "suppliers.yaml"
    if not cfg_path.exists():
        return {}
    with open(cfg_path, "r", encoding="utf-8") as f:
        all_suppliers = yaml.safe_load(f) or {}
    return (
            all_suppliers.get(supplier_name)
            or all_suppliers.get(supplier_name.upper())
            or all_suppliers.get(supplier_name.lower())
            or {}
    )


# ----------------------- Normalize & parse -----------------------

def _normalize_line_with_cfg(line: str, gt5_to: Optional[int]) -> str:
    """
    Нормалізація рядка для «пробільних» форматів.
    """
    repl = str(gt5_to if gt5_to is not None else 10)
    line = re.sub(r">\s*5", repl, line)
    m = re.search(r"\w\s\w*\s\w", line)
    if m:
        line = re.sub(r"\s", "", line, count=1)
    line = re.sub(r"\s", ";", line)
    return line


def raw_csv_to_rows(
        input_csv: Path,
        *,
        stock_index: Optional[int],
        stock_header_token: str = "STAN",
        gt5_to: Optional[int] = None,
        skip_rows: int = 0,
        normalize_mode: str = "spaces",
) -> List[List[str]]:
    """
    Читає сирий CSV. Якщо stock_index=None, повертає всі рядки без фільтрації залишків.
    """
    rows: List[List[str]] = []

    # Використовуємо cp1250 для польських прайсів, щоб не було помилок декодування
    with open(input_csv, "r", encoding="cp1250", errors="replace") as f:
        for i, raw in enumerate(f):
            if i < skip_rows:
                continue
            raw = raw.strip()
            if not raw:
                continue

            # Розбиваємо рядок на частини
            if normalize_mode == "csv":
                parts = raw.split(";")
            else:
                norm = _normalize_line_with_cfg(raw, gt5_to=gt5_to)
                parts = norm.split(";")

            if not parts:
                continue

            # --- ГОЛОВНА ЗМІНА ТУТ ---
            # Якщо ми не вказали індекс стоку (як для файлу цін),
            # ми просто додаємо рядок і йдемо далі, не перевіряючи числа.
            if stock_index is None:
                rows.append(parts)
                continue

            # --- ЛОГІКА ДЛЯ ФАЙЛУ ЗАЛИШКІВ (де індекс вказано) ---
            idx = stock_index
            if idx < 0 or idx >= len(parts):
                continue

            val = (parts[idx] or "").strip()

            # Пропускаємо заголовки типу "STAN"
            if val.lower() == (stock_header_token or "").lower():
                continue

            # Нормалізуємо '>5'
            if gt5_to is not None and ">" in val:
                val = str(gt5_to)
                parts[idx] = val

            # Перевірка на число (тільки для файлу залишків!)
            try:
                if float(val) <= 0:
                    continue
            except ValueError:
                # Якщо в колонці залишку не число — ігноруємо цей рядок
                continue

            rows.append(parts)

    return rows


def _rows_to_standard_df(rows: List[List[str]], colmap: Dict[str, int]) -> pd.DataFrame:
    """
    Приводимо сирі рядки до стандартної моделі колонок.
    """

    def take(r: List[str], idx: Optional[int]) -> str:
        if idx is None or idx < 0 or idx >= len(r):
            return ""
        return (r[idx] or "").strip()

    data: List[List[Any]] = []
    for r in rows:
        code = take(r, colmap.get("code"))
        unicode_ = take(r, colmap.get("unicode")) or code
        brand = take(r, colmap.get("brand"))
        name = take(r, colmap.get("name")) or brand
        stock_s = take(r, colmap.get("stock"))
        price_s = take(r, colmap.get("price"))

        # stock -> int
        try:
            stock = int(float(stock_s))
        except Exception:
            stock = 0

        # price -> float (коми/зайві символи прибираємо)
        ps = price_s.replace(",", ".")
        ps = re.sub(r"[^0-9.]", "", ps)
        try:
            price = float(ps)
        except Exception:
            price = float("nan")

        data.append([code, unicode_, brand, name, stock, price])

    df = pd.DataFrame(data, columns=["code", "unicode", "brand", "name", "stock", "price"])
    df["price"] = pd.to_numeric(df["price"], errors="coerce")
    df["stock"] = pd.to_numeric(df["stock"], errors="coerce").fillna(0).astype(int)
    return df


# ----------------------- Pricing & build output -----------------------

def _apply_pricing(
        df: pd.DataFrame,
        factor: float,
        currency_out: str,
        rate: float,
        rounding: Dict[str, int],
) -> pd.Series:
    """
    Обчислює фінальну ціну.
    """
    base = pd.to_numeric(df["price"], errors="coerce").fillna(0.0).astype(float)
    if currency_out.upper() == "UAH":
        val = base * float(factor) * float(rate)
        digits = int(rounding.get("UAH", 0))
    else:
        val = base * float(factor)
        digits = int(rounding.get("EUR", 2))
    return val.round(digits).astype(float)


def _build_output_df(
        df_std: pd.DataFrame,
        price_final: pd.Series,
        columns_cfg: List[Dict[str, str]],
        supplier_id: Optional[int],
) -> pd.DataFrame:
    """
    Збирає вихідний DataFrame.
    """
    temp = df_std.copy()
    temp["supplier_id"] = supplier_id if supplier_id is not None else None
    temp["price"] = price_final

    out_cols: Dict[str, pd.Series] = {}
    for col in columns_cfg:
        src = col["from"]
        hdr = col["header"]
        if src not in temp.columns:
            temp[src] = temp.get(src, None)
        out_cols[hdr] = temp[src]

    return pd.DataFrame(out_cols)


# ----------------------- Materialize to CSV -----------------------

def _materialize_to_csv(remote_path: str, tmp_dir: Path, supplier: str) -> tuple[Path, list[Path]]:
    """
    Завантажує файл (з локального диска або FTP) та готує його до читання.
    Тепер враховує назву постачальника та тип файлу (.csv або .gz).
    """
    cleanup: list[Path] = []

    # 1) Робота з локальним файлом (для тестів)
    if os.path.exists(remote_path):
        p = Path(remote_path)
        if p.suffix.lower() == ".csv":
            return p, cleanup
        if p.suffix.lower() == ".gz":
            csv_out = tmp_dir / f"{p.stem}.csv"
            unzip_gz_file(p, csv_out)
            cleanup.append(csv_out)
            return csv_out, cleanup
        raise ValueError(f"Unsupported local file type: {p.suffix}")

    # 2) Робота з FTP
    else:
        stamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        filename = Path(remote_path).name

        # Перевіряємо, чи файл заархівований
        is_gz = remote_path.lower().endswith(".gz")

        # Створюємо шлях для завантаження
        download_path = tmp_dir / f"ftp_{stamp}_{filename}"

        # --- ВИКЛИК ОНОВЛЕНОГО ЗАВАНТАЖУВАЧА ---
        # Передаємо supplier, щоб функція знала, які паролі брати з .env
        download_file_from_ftp(remote_path, download_path, supplier)

        if is_gz:
            # Якщо це архів — розпаковуємо
            csv_tmp = download_path.with_suffix(".csv")
            if csv_tmp == download_path:  # про всяк випадок, щоб не затерти
                csv_tmp = download_path.parent / (download_path.name + "_unzipped.csv")

            unzip_gz_file(download_path, csv_tmp)

            # Додаємо обидва файли в чергу на видалення
            cleanup.extend([download_path, csv_tmp])
            return csv_tmp, cleanup
        else:
            # Якщо це звичайний CSV — просто повертаємо його
            cleanup.append(download_path)
            return download_path, cleanup


# ----------------------- NEW: ПІДГОТОВКА ДАНИХ (ОДИН РАЗ) -----------------------

def prepare_base_df(
    supplier: str,
    additional_files: Optional[Dict[str, str]] = None,
    remote_gz_path: Optional[str] = None
) -> Tuple[pd.DataFrame, List[Path]]:
    """
    УНІВЕРСАЛЬНА ПІДГОТОВКА:
    - Завантажує файли (один або кілька).
    - Сумує залишки по складах (Aggregation).
    - Робить мердж, якщо це Autopartner (ціни + залишки).
    - Повертає готовий DataFrame та список файлів для видалення.
    """
    tmp_dir = TEMP_DIR
    local_files = {}
    cleanup_paths = []

    # 1. Завантаження (Download)
    if additional_files:
        print(f"[INFO] 📥 Завантаження кількох файлів для {supplier}...")
        for key, r_path in additional_files.items():
            l_path, c_paths = _materialize_to_csv(r_path, tmp_dir, supplier)
            local_files[key] = l_path
            cleanup_paths.extend(c_paths)
    elif remote_gz_path:
        print(f"[INFO] 📥 Завантаження одного файлу для {supplier}...")
        l_path, c_paths = _materialize_to_csv(remote_gz_path, tmp_dir, supplier)
        local_files["prices"] = l_path
        cleanup_paths.extend(c_paths)

    # 2. Налаштування (Config)
    sup_cfg = _load_supplier_cfg(supplier)
    layout = sup_cfg.get("raw_layout", {}) or {}
    colmap = layout.get("columns") or {}
    read_params = {
        "stock_index": layout.get("stock_index"),
        "stock_header_token": layout.get("stock_header_token", "STAN"),
        "gt5_to": layout.get("gt5_to"),
        "skip_rows": (sup_cfg.get("preprocess") or {}).get("skip_rows", 0),
        "normalize_mode": (sup_cfg.get("normalize") or {}).get("mode", "spaces"),
    }

    # 3. Обробка даних
    # СЦЕНАРІЙ А: Autopartner (2 окремі файли)
    if "prices" in local_files and "stock" in local_files:
        print(f"[INFO] 🧩 Режим МЕРДЖУ для {supplier}...")
        rows_p = raw_csv_to_rows(local_files["prices"], **{**read_params, "stock_index": None})
        df_p = _rows_to_standard_df(rows_p, colmap)
        df_p["code"] = df_p["code"].astype(str).str.strip().str.upper()

        rows_s = raw_csv_to_rows(local_files["stock"], **read_params)
        df_s = _rows_to_standard_df(rows_s, colmap)
        df_s["code"] = df_s["code"].astype(str).str.strip().str.upper()

        # --- СУМУЄМО СКЛАДИ ---
        print(f"[INFO] 🔄 Агрегація стоку: було {len(df_s)} рядків...")
        df_s = df_s.groupby("code", as_index=False).agg({"stock": "sum"})
        print(f"[INFO] ✅ Після об'єднання складів: {len(df_s)} унікальних кодів.")

        # Мердж цін із сумарними залишками
        df_std = pd.merge(df_p.drop(columns=["stock"]), df_s[["code", "stock"]], on="code", how="inner")

    # СЦЕНАРІЙ Б: Гданськ / Maxgear (1 файл)
    else:
        print(f"[INFO] 📄 Режим одного файлу для {supplier}...")
        main_file = local_files.get("prices") or list(local_files.values())[0]
        rows = raw_csv_to_rows(main_file, **read_params)
        df_std = _rows_to_standard_df(rows, colmap)
        df_std["code"] = df_std["code"].astype(str).str.strip().str.upper()

        # Навіть в одному файлі можуть бути дублі (різні склади)
        cols_to_keep = [c for c in df_std.columns if c != 'stock']
        df_std = df_std.groupby(cols_to_keep, as_index=False).agg({"stock": "sum"})

    # 4. Бренди (якщо є файл brands.csv)
    if "brands" in local_files:
        print(f"[INFO] 🏷️ Додаємо повні назви брендів...")
        df_brands = pd.read_csv(local_files["brands"], sep=";", names=["short_name", "full_name"],
                               encoding="cp1250", quotechar='"', encoding_errors="replace")
        df_brands["short_name"] = df_brands["short_name"].astype(str).str.strip().str.upper()
        df_std["brand"] = df_std["brand"].astype(str).str.strip().str.upper()

        df_std = pd.merge(df_std, df_brands, left_on="brand", right_on="short_name", how="left")
        df_std["brand"] = df_std["full_name"].fillna(df_std["brand"])
        df_std = df_std.drop(columns=["short_name", "full_name"])

    return df_std, cleanup_paths


# ----------------------- Main pipeline -----------------------
def process_one_price(
        df_input: pd.DataFrame,  # ТЕПЕР ПРИЙМАЄ ГОТОВИЙ DATAFRAME
        supplier: str,
        supplier_id: Optional[int],
        factor: float,
        currency_out: str,  # "EUR" | "UAH"
        format_: str,  # "xlsx" | "csv"
        rounding: Dict[str, int],  # {"EUR":2, "UAH":0}
        r2_prefix: str,  # ".../{supplier}/"
        columns: List[Dict[str, str]],
        csv_cfg: Optional[Dict[str, Any]] = None,
        rate: float = 1.0,
) -> Tuple[str, str]:
    """
    ЛЕГКИЙ ЕТАП: Тільки націнка, запис у БД та вивантаження файлу.
    Більше не качає FTP і не робить мердж!
    """
    tmp_dir = TEMP_DIR
    stamp = datetime.now().strftime("%Y%m%d_%H%M")
    supplier_code_str = supplier.lower()

    # 1) Створюємо копію даних для цього конкретного проходу
    # Щоб націнка для одного прайсу не вплинула на інший
    df_std = df_input.copy()

    # 2) КАЛЬКУЛЯЦІЯ ЦІНИ
    price_final = _apply_pricing(
        df_std, factor=factor, currency_out=currency_out, rate=rate, rounding=rounding
    )

    # 3) ЗБІРКА ВИХІДНОГО DATAFRAME
    out_df = _build_output_df(
        df_std, price_final, columns_cfg=columns, supplier_id=supplier_id
    )

        # 4) ЗАПИС У POSTGRESQL (тільки для сайтів)
    if "/site/" in r2_prefix and supplier_id is not None:
        try:
            print(f"[INFO] DB Trigger: Updating site prices for ID {supplier_id}...")

            # Зчитуємо URL (переконайся, що в .env він від Supabase!)
            raw_url = os.getenv("DATABASE_URL")
            if raw_url and raw_url.startswith("postgresql://"):
                db_url = raw_url.replace("postgresql://", "postgresql+psycopg2://", 1)
            else:
                db_url = raw_url or "postgresql+psycopg2://postgres:123456789@localhost:5432/postgres"

            engine = create_engine(db_url, pool_pre_ping=True)

            # Використовуємо .begin() для автоматичного збереження (COMMIT)
            with engine.begin() as conn:
                # КРОК А: Очищення старих даних цього постачальника
                conn.execute(
                    text("DELETE FROM product_catalog WHERE supplier_id = :sid"),
                    {"sid": supplier_id}
                )
                print(f"[INFO] DB: Old records for ID {supplier_id} deleted.")

            out_df_db = out_df.replace('\x00', '', regex=True)

            # Додаємо нормалізовані колонки для швидкого пошуку
            def _norm_val(v: str) -> str:
                if not v or pd.isna(v): return ""  # Захист від порожніх клітинок
                return re.sub(r'[^A-Za-z0-9]', '', str(v)).upper()

            if "code" in out_df_db.columns:
                out_df_db["code_norm"] = out_df_db["code"].apply(_norm_val)
            else:
                out_df_db["code_norm"] = None

            if "unicode" in out_df_db.columns:
                out_df_db["unicode_norm"] = out_df_db["unicode"].apply(_norm_val)
            else:
                out_df_db["unicode_norm"] = None

            if "brand" in out_df_db.columns:
                out_df_db["brand_norm"] = out_df_db["brand"].apply(_norm_val)
            else:
                out_df_db["brand_norm"] = None

            out_df_db.to_sql(
                'product_catalog',
                con=engine,
                if_exists='append',
                index=False,
                chunksize=5000
            )

            print(f"[INFO] PostgreSQL: SUCCESS! {len(out_df_db)} items pushed to Supabase.")

        except Exception as e:
            print(f"[ERROR] Database save failed: {e}")

    # 5) ЕКСПОРТ У ФАЙЛ (Excel або CSV)
    ext = "xlsx" if format_.lower() == "xlsx" else "csv"

    # --- 👇 ЛОГІКА ТЕГІВ ЗГІДНО З ТВОЇМ ЗАПИТОМ 👇 ---
    date_str = datetime.now().strftime("%d.%m.%y")  # 08.02.26
    time_str = datetime.now().strftime("%H%M%S")  # 223005

    # Визначаємо мітку на основі префікса шляху (r2_prefix)
    prefix_lower = r2_prefix.lower()

    if "exist" in prefix_lower:
        tag = "exist"
    elif "1_23" in prefix_lower:
        tag = "m"
    elif "1_27" in prefix_lower:
        tag = "l"
    elif "site" in prefix_lower or "1_33" in prefix_lower:
        tag = "xl"
    else:
        tag = "data"  # Технічний тег, якщо нічого не підійшло

    # Формуємо фінальну назву (тільки малі букви)
    # Приклад: price_autopartner_08.02.26_223005_xl.xlsx
    out_name = f"price_{supplier.lower()}_{date_str}_{time_str}_{tag}.{ext}"
    out_path = tmp_dir / out_name
    # -----------------------------------------------


    if ext == "xlsx":
        out_df.to_excel(out_path, index=False, engine="xlsxwriter")
        content_type = "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
    else:
        delim = (csv_cfg or {}).get("delimiter", ";")
        out_df.to_csv(out_path, index=False, sep=delim, header=True, encoding="utf-8")
        content_type = "text/csv"

    # 6) ВИВАНТАЖЕННЯ В CLOUDFLARE R2
    storage = StorageClient()
    key = f"{r2_prefix}{out_name}"

    url = storage.upload_file(
        local_path=str(out_path),
        key=key,
        content_type=content_type,
        cleanup_prefix=r2_prefix,
        keep_last=5,  # Тримаємо 5 останніх версій
    )

    # Видаляємо готовий Excel/CSV з диска після вивантаження
    out_path.unlink(missing_ok=True)

    return key, url