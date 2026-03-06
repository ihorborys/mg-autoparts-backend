import time
import re
from deep_translator import GoogleTranslator
from app.services.cloudflare_d1 import CloudflareD1Manager
from app.services.dictionaries import PARTS_DESCRIPTION_DICT, POSITION_DICT

# 3. Створи екземпляр менеджера:
d1 = CloudflareD1Manager()

def apply_manual_rules(name_pl: str) -> (str, bool):
    """Прикладає правила зі словників. Повертає (текст, чи було змінено)"""
    translated = name_pl
    changed = False

    # А) Заміна основної назви
    for pl_key, ua_val in PARTS_DESCRIPTION_DICT.items():
        if pl_key in translated:
            translated = translated.replace(pl_key, ua_val)
            changed = True
            break  # Беремо тільки одну головну назву

    # Б) Заміна позицій
    for pl_pos, ua_pos in POSITION_DICT.items():
        pattern = rf"\b{pl_pos}\b"
        if re.search(pattern, translated):
            translated = re.sub(pattern, ua_pos, translated)
            changed = True

    return translated, changed


def translate_products(products: list, supplier_id: int) -> dict:
    if not products: return {}

    if supplier_id == 2:
        return {(str(p['code']), str(p['name']).strip().upper()): p['name'] for p in products}

    results = {}
    to_google = []

    for p in products:
        code = str(p['code'])
        u_val = str(p.get('unicode', ''))
        raw_name = str(p['name']).strip().upper()
        name_pl = raw_name.replace("—", " ").replace("-", " ").replace("  ", " ").strip()

        # --- НОВА ЛОГІКА ПРІОРИТЕТІВ ---

        # 1. Пробуємо застосувати ручні правила (словник)
        manual_ua, is_manual = apply_manual_rules(name_pl)

        # 2. Шукаємо, що там у нас у кеші
        cached_ua = d1.get_cached_translation(supplier_id, code, name_pl)

        # 3. Синхронізація: якщо в словнику є правило, а в кеші інше - оновлюємо кеш
        if is_manual:
            if cached_ua != manual_ua:
                print(f"🔄 [SYNC] Оновлення словника для {code}: {cached_ua} -> {manual_ua}")
                d1.save_to_cache(supplier_id, code, u_val, name_pl, manual_ua)
            results[(code, name_pl)] = manual_ua
            continue

        # 4. Якщо ручних правил немає - використовуємо кеш
        if cached_ua:
            results[(code, name_pl)] = cached_ua
            continue

        # 5. Якщо і в кеші порожньо - додаємо в список для Google
        to_google.append(p)

    # 3. БЛОК GOOGLE TRANSLATE
    if to_google:
        unique_names = list(set([str(p['name']).strip().upper() for p in to_google]))
        print(f"🌍 [GOOGLE] Переклад: {len(unique_names)} нових назв")

        chunk_size = 20  # Зменшив для стабільності
        google_map = {}

        for i in range(0, len(unique_names), chunk_size):
            chunk = unique_names[i:i + chunk_size]
            context_batch = [f"część samochodowa: {n}" for n in chunk]

            try:
                translated = GoogleTranslator(source='pl', target='uk').translate_batch(context_batch)
                for orig, trans in zip(chunk, translated):
                    clean_ua = re.sub(r'^.*?:', '', trans).strip() if ":" in trans else trans.strip()
                    google_map[orig] = clean_ua.capitalize()
                time.sleep(0.5)
            except Exception as e:
                print(f"❌ Помилка Google: {e}")
                for n in chunk: google_map[n] = n

        for p in to_google:
            c, u = str(p['code']), str(p.get('unicode', ''))
            n_pl = str(p['name']).strip().upper()
            n_uk = google_map.get(n_pl, n_pl)
            d1.save_to_cache(supplier_id, c, u, n_pl, n_uk)
            results[(c, n_pl)] = n_uk

    return results