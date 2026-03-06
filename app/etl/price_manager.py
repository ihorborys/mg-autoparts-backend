from pathlib import Path
from typing import Dict, List, Any, Optional
import yaml

# --- ОНОВЛЕНІ ІМПОРТИ ---
from .translation_manager import process_price_translation
# from app.services.local_db import init_local_db, backup_db_to_r2
from app.services.paths import CONFIG_DIR
from .price_processor import process_one_price, prepare_base_df
from app.services.exchange import get_eur_to_uah
from app.services.dictionaries import BRANDS_DICT


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
    # Ініціалізуємо локальну базу (створюємо папку data/db, якщо її немає)
    # init_local_db()

    profiles_cfg = _load_yaml(CONFIG_DIR / "profiles.yaml")
    profiles = profiles_cfg.get("profiles", [])
    common = profiles_cfg.get("common", {})
    rounding = (common.get("rounding") or {"EUR": 2, "UAH": 0})

    if supplier_id is None:
        supplier_id = _get_supplier_id(supplier)

    # --- 🛠 КРОК 1: ВАЖКА ПІДГОТОВКА ---
    print(f"\n[MANAGER] 🚀 Початок підготовки базових даних для {supplier}...")

    base_df, cleanup_paths = prepare_base_df(
        supplier=supplier,
        additional_files=additional_files,
        remote_gz_path=remote_gz_path
    )

    # ============================================================
    # ⬇️ ЛОГІКА UNICODE (ПЕРЕНЕСЕНО ПЕРЕД ПЕРЕКЛАДОМ) ⬇️
    # ============================================================
    if 'unicode' not in base_df.columns or supplier_id == 2:
        base_df['unicode'] = base_df['code']

    # Очищаємо unicode відразу для всіх (це ключ для нашої бази перекладів!)
    base_df['unicode'] = base_df['unicode'].astype(str).str.replace(r'[^a-zA-Z0-9]', '', regex=True).str.upper()
    print(f"[MANAGER] ✨ Unicode нормалізовано")

    # ============================================================
    # 🏷️ НОРМАЛІЗАЦІЯ БРЕНДІВ (НОВИЙ БЛОК) 🏷️
    # ============================================================
    if 'brand' in base_df.columns:
        print(f"[MANAGER] 🏷️ Нормалізація брендів для {len(base_df)} позицій...")

        # Перетворюємо колонку бренд: чистимо пробіли, у верхній регістр і замінюємо за словником
        base_df['brand'] = base_df['brand'].astype(str).str.strip().str.upper().apply(
            lambda x: BRANDS_DICT.get(x, x)
        )
        print(f"[MANAGER] ✅ Бренди приведено до стандарту!")

    # ============================================================
    # 🌍 ОНОВЛЕНИЙ БЛОК ПЕРЕКЛАДУ 🌍
    # ============================================================
    if 'name' in base_df.columns:
        if supplier_id == 2:
            print(f"[MANAGER] ℹ️ Пропускаємо переклад для Гданська.")
        else:
            print(f"[MANAGER] 🌍 Переклад назв для {len(base_df)} позицій...")
            # Викликаємо нову логіку (SQLite -> Google)
            base_df = process_price_translation(base_df, supplier_id, limit=1000)

            print(f"[MANAGER] ✅ Переклад завершено!")

    print(f"[MANAGER] ✅ База готова! Всього позицій: {len(base_df)}")

    results: List[Dict[str, Any]] = []

    # --- ⚡ КРОК 2: ШВИДКИЙ ЦИКЛ ПО ПРОФІЛЯХ ---
    for profile in profiles:
        name = profile["name"]
        if profile_filter and profile_filter.lower() not in name.lower():
            continue

        factor = float(profile["factor"])
        currency_out = str(profile["currency_out"]).upper()
        format_ = profile["format"]

        # r2_prefix = (profile.get("r2_prefix") or "").format(supplier=supplier.lower())
        # if r2_prefix and not r2_prefix.endswith("/"):
        #     r2_prefix += "/"

        # 1. Беремо твій оригінальний префікс (наприклад, "1_23/" або "{supplier}/")
        raw_prefix = (profile.get("r2_prefix") or "").format(supplier=supplier.lower())

        # 2. Примусово додаємо "prices/" на початок, щоб все йшло в одну папку
        r2_prefix = f"prices/{raw_prefix}"

        # 3. Твій стандартний фікс слеша в кінці
        if r2_prefix and not r2_prefix.endswith("/"):
            r2_prefix += "/"

        columns = profile.get("columns") or []
        csv_cfg = profile.get("csv") or {}

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

        key, url = process_one_price(
            df_input=base_df,
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

    # --- 🧹 КРОК 3: ФІНАЛЬНЕ ОЧИЩЕННЯ ТА БЕКАП ---
    print(f"\n[MANAGER] 🧹 Очищення тимчасових файлів...")
    for p in cleanup_paths:
        p.unlink(missing_ok=True)

    # # 📦 РОБИМО БЕКАП БАЗИ ПЕРЕКЛАДІВ У ХМАРУ
    # print(f"[MANAGER] 📦 Відправка бекапу бази перекладів у Cloudflare R2...")
    # backup_db_to_r2(keep_last=3)

    return results