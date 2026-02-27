from pathlib import Path
from typing import Dict, List, Any, Optional
import yaml
import re

from app.services.paths import CONFIG_DIR
# --- ВАЖЛИВО: Імпортуємо обидві функції ---
from .price_processor import process_one_price, prepare_base_df
from app.services.exchange import get_eur_to_uah


def _load_yaml(path: Path) -> Dict[str, Any]:
    if not path.exists():
        raise FileNotFoundError(f"{path} not found")
    with open(path, "r", encoding="utf-8") as f:
        return yaml.safe_load(f) or {}


def _get_supplier_id(supplier: str) -> Optional[int]:
    cfg = _load_yaml(CONFIG_DIR / "suppliers.yaml")
    node = cfg.get(supplier) or cfg.get(supplier.upper()) or cfg.get(supplier.lower())
    if not node:
        return None
    return int(node["supplier_id"]) if "supplier_id" in node and node["supplier_id"] is not None else None


def process_all_prices(
        supplier: str,
        remote_gz_path: Optional[str],
        *,
        delete_input_after: bool = False,
        supplier_id: Optional[int] = None,
        profile_filter: Optional[str] = None,
        additional_files: Optional[Dict[str, str]] = None,
) -> List[Dict[str, Any]]:
    """
    ПЕРЕРОБЛЕНА ЛОГІКА:
    1. ЕТАП ПІДГОТОВКИ: FTP + Мердж + Агрегація стоку (1 раз).
    2. ЕТАП ЕКСПОРТУ: Націнка + БД + R2 (5 разів, але швидко).
    """
    profiles_cfg = _load_yaml(CONFIG_DIR / "profiles.yaml")
    profiles = profiles_cfg.get("profiles", [])
    common = profiles_cfg.get("common", {})
    rounding = (common.get("rounding") or {"EUR": 2, "UAH": 0})

    if supplier_id is None:
        supplier_id = _get_supplier_id(supplier)

    # --- 🛠 КРОК 1: ВАЖКА ПІДГОТОВКА (ОДИН РАЗ ДЛЯ ВСІХ) ---
    # Тут ми качаємо FTP, робимо GROUPBY для стоку та MERGE
    print(f"\n[MANAGER] 🚀 Початок підготовки базових даних для {supplier}...")

    base_df, cleanup_paths = prepare_base_df(
        supplier=supplier,
        additional_files=additional_files,
        remote_gz_path=remote_gz_path
    )

    print(f"[MANAGER] ✅ База готова! Всього позицій: {len(base_df)}")

    # ============================================================
    # ⬇️ НОВА ЛОГІКА НОРМАЛІЗАЦІЇ UNICODE ⬇️
    # ============================================================
    # 1. Якщо це Постачальник 2 (AP_GDANSK) або якщо колонки unicode випадково немає
    if supplier_id == 2 or 'unicode' not in base_df.columns:
        base_df['unicode'] = base_df['code']

    # 2. Очищаємо unicode для ВСІХ постачальників (і для 1, і для 2, і для 3)
    # Видаляємо пробіли, тире, крапки та робимо UPPERCASE
    base_df['unicode'] = base_df['unicode'].astype(str).str.replace(r'[^a-zA-Z0-9]', '', regex=True).str.upper()

    print(f"[MANAGER] ✨ Unicode нормалізовано для {supplier} (ID: {supplier_id})")
    # ============================================================

    results: List[Dict[str, Any]] = []

    # --- ⚡ КРОК 2: ШВИДКИЙ ЦИКЛ ПО ПРОФІЛЯХ ---
    for profile in profiles:
        name = profile["name"]

        # Фільтрація профілів (наприклад, тільки 'site')
        if profile_filter and profile_filter.lower() not in name.lower():
            print(f"ℹ️  Профіль '{name}' пропущено (фільтр: {profile_filter})")
            continue

        factor = float(profile["factor"])
        currency_out = str(profile["currency_out"]).upper()
        format_ = profile["format"]
        r2_prefix = (profile.get("r2_prefix") or "").format(supplier=supplier.lower())
        if r2_prefix and not r2_prefix.endswith("/"):
            r2_prefix += "/"

        columns = profile.get("columns") or []
        csv_cfg = profile.get("csv") or {}

        # Розрахунок курсу (UAH)
        rate = 1.0
        if currency_out == "UAH":
            rp = profile.get("rate_params") or {}
            fb = rp.get("fallback")
            fallback_value = fb.get("value") if isinstance(fb, dict) else (fb or 50)
            rate = get_eur_to_uah(
                add_uah=rp.get("add_uah", 1),
                min_rate=rp.get("min_rate", 49),
                fallback=fallback_value,
            )

        print(f"➡️  Обробка профілю: {name} (націнка x{factor})")

        # Виклик ПОЛЕГШЕНОЇ функції (передаємо вже готовий base_df)
        key, url = process_one_price(
            df_input=base_df,  # ПЕРЕДАЄМО ГОТОВІ ДАНІ
            supplier=supplier,
            supplier_id=supplier_id,
            factor=factor,
            currency_out=currency_out,
            format_=format_,
            rounding=rounding,
            r2_prefix=r2_prefix,
            columns=columns,
            csv_cfg=csv_cfg,
            rate=rate
        )

        results.append({
            "name": name,
            "factor": factor,
            "currency": currency_out,
            "key": key,
            "url": url,
        })

    # --- 🧹 КРОК 3: ФІНАЛЬНЕ ОЧИЩЕННЯ ТИМЧАСОВИХ ФАЙЛІВ ---
    # Видаляємо сирі CSV тільки ПІСЛЯ того, як всі 5 профілів відпрацювали
    print(f"\n[MANAGER] 🧹 Очищення тимчасових файлів постачальника...")
    for p in cleanup_paths:
        try:
            p.unlink(missing_ok=True)
        except Exception as e:
            print(f"[WARN] Не вдалося видалити {p}: {e}")

    return results