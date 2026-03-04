import time
import re
from deep_translator import GoogleTranslator
from app.services.local_db import get_cached_translation, save_to_cache

# Словник запчастин (Основні назви)
AUTO_PARTS_DICT = {
    # --- Підвіска та кермо ---
    "WAHACZ": "Важіль підвіски",
    "ŁĄCZNIK STABILIZATORA": "Тяга стабілізатора",
    "LACZNIK STAB.": "Тяга стабілізатора",
    "AMORTYZATOR": "Амортизатор",
    "SPRZĘŻYNA ZAWIESZENIA": "Пружина підвіски",
    "SWORZEŃ WAHACZA": "Шарова опора",
    "PRZEGUB": "ШРУС (шарнір)",
    "DRĄŻEK KIEROWNICZY": "Рульова тяга",
    "KOŃCÓWKA DRĄŻKA": "Наконечник рульової тяги",
    "PIASTA KOŁA": "Маточина колеса",
    "ŁOŻYSKO KOŁA": "Підшипник колеса",
    "PODUSZKA AMORT.": "Подушка амортизатора",

    # --- Гальма ---
    "TARCZA HAMULCOWA": "Гальмівний диск",
    "TARCZA HAM.": "Гальмівний диск",
    "KLOCKI HAMULCOWE": "Гальмівні колодки",
    "KLOCKI HAM.": "Гальмівні колодки",
    "ZACISK HAMULCOWY": "Гальмівний супорт",
    "PRZEWÓD HAMULCOWY": "Гальмівний шланг",
    "CZUJNIK ABS": "Датчик ABS",

    # --- Фільтри ---
    "FILTR OLEJU": "Фільтр масляний",
    "FILTR POWIETRZA": "Фільтр повітряний",
    "FILTR PALIWA": "Фільтр паливний",
    "FILTR KABINOWY": "Фільтр салону",

    # --- Двигун та ГРМ ---
    "ZESTAW ROZRZĄDU": "Комплект ГРМ",
    "PASEK ROZRZĄDU": "Ремінь ГРМ",
    "PASEK WIELOROWKOWY": "Ремінь поліклиновий",
    "PODUSZKA SILNIKA": "Подушка двигуна",
    "PODUSZKA SIL.": "Подушка двигуна",
    "ŚWIECA ZAPŁONOWA": "Свічка запалювання",
    "CEWKA ZAPŁONOWA": "Котушка запалювання",
    "TERMOSTAT": "Термостат",
    "POMPA WODY": "Насос водяний (помпа)",

    # --- Кузовні деталі ---
    "ZDERZAK": "Бампер",
    "MASKA": "Капот",
    "BŁOTNIK": "Крило",
    "LUSTERKO": "Дзеркало",
    "ATRAPA": "Решітка радіатора",
    "KLAMKA": "Ручка дверей",
    "PODNOŚNIK SZYBY": "Склопідйомник",
    "SIŁOWNIK BAGAŻNIKA": "Амортизатор багажника",

    # --- Охолодження та Кондиціонер ---
    "CHŁODNICA WODY": "Радіатор охолодження",
    "CHŁODNICA KLIMATYZACJI": "Радіатор кондиціонера",
    "SKRAPLACZ": "Конденсер кондиціонера",
    "WENTYLATOR": "Вентилятор",
    "NAGRZEWNICA": "Радіатор пічки",
    "INTERCOOLER": "Інтеркулер",

    # --- Світло ---
    "REFLEKTOR": "Фара передня",
    "LAMPA TYLNA": "Ліхтар задній",
    "LAMPA PRZECIWMGIELNA": "Протитуманна фара",
    "KIERUNKOWSKAZ": "Покажчик повороту",

    # --- Електрика ---
    "ALTERNATOR": "Генератор",
    "ROZRUSZNIK": "Стартер",

    # --- Різне ---
    "KOPUŁKA ROZDZ.": "Кришка розподільника",
    "KOPUŁKA": "Кришка розподільника",
    "PALEC ROZDZ.": "Бігунок розподільника",
    "PALEC ROZDZIELACZA": "Бігунок розподільника",
    "WŁĄCZNIK ŚWIATEŁ": "Вмикач ліхтарів",
    "WŁĄCZNIK —WIATEŁ": "Вмикач ліхтарів",
    "KOREK WLEWU": "Кришка заливної горловини",
    "GUMA STAB.": "Втулка стабілізатора",
    "GUMA STABILIZATORA": "Втулка стабілізатора",
    "CYLINDEREK HAM.": "Гальмівний циліндр",
    "TULEJA BELKI": "Втулка балки",
    "KOŃCÓWKA WTR.": "Розпилювач форсунки",
    "CZUJNIK TEMP.": "Датчик температури",
    "PŁYNU CHŁODZ.": "Охолоджуючої рідини",
    "CZUJNIK CI—N. OLEJU": "Датчик тиску масла",
}

# Словник сторін та позицій (Для SEO та пошуку)
POSITION_DICT = {
    "PRZÓD": "Перед",
    "PRZEDNI": "Передній",
    "PRZEDNIA": "Передня",
    "TYŁ": "Зад",
    "TYLNY": "Задній",
    "TYLNA": "Задня",
    "LEWY": "Лівий",
    "LEWA": "Ліва",
    "PRAWY": "Правий",
    "PRAWA": "Права",
    "KOMPLET": "Комплект",
    "KPL": "кпл.",
    "SZTUKA": "Штука",
    "SZT": "шт.",
    "GÓRA": "Верх",
    "GÓRNY": "Верхній",
    "DÓŁ": "Низ",
    "DOLNY": "Нижній"
}


def translate_products(products: list, supplier_id: int) -> dict:
    if not products: return {}

    # Гданськ (ID 2) не перекладаємо
    if supplier_id == 2:
        return {(str(p['code']), str(p['name']).strip().upper()): p['name'] for p in products}

    results = {}
    to_google = []

    for p in products:
        code = str(p['code'])
        u_val = str(p.get('unicode', ''))

        # 1. Отримуємо назву та ОДРАЗУ чистимо її від сміття
        raw_name = str(p['name']).strip().upper()
        # ВСТАВЛЯЄМО ТУТ:
        name_pl = raw_name.replace("—", " ").replace("-", " ").replace("  ", " ").strip()

        # 2. Перевірка кешу SQLite (використовуємо вже чисту назву)
        cached = get_cached_translation(supplier_id, code, u_val, name_pl)
        if cached:
            results[(code, name_pl)] = cached
            continue

        # 2. ГІБРИДНИЙ ПЕРЕКЛАД (Словники)
        translated_name = name_pl
        changed = False

        # А) Заміна назви запчастини
        for pl_key, ua_val in AUTO_PARTS_DICT.items():
            if pl_key in translated_name:
                translated_name = translated_name.replace(pl_key, ua_val)
                changed = True
                break  # Замінюємо лише першу знайдену основну назву

        # Б) Заміна позицій (Перед/Зад/Ліво/Право)
        # Використовуємо регекс, щоб замінювати тільки окремі слова (не всередині інших)
        for pl_pos, ua_pos in POSITION_DICT.items():
            pattern = rf"\b{pl_pos}\b"
            if re.search(pattern, translated_name):
                translated_name = re.sub(pattern, ua_pos, translated_name)
                changed = True

        if changed:
            # Якщо ми щось замінили словниками - зберігаємо в кеш
            save_to_cache(supplier_id, code, u_val, name_pl, translated_name)
            results[(code, name_pl)] = translated_name
        else:
            # Якщо в словниках нічого не знайшли - віддаємо Google
            to_google.append(p)

    # 3. БЛОК GOOGLE TRANSLATE (Для всього, що не в словниках)
    if to_google:
        unique_names = list(set([str(p['name']).strip().upper() for p in to_google]))
        print(f"🌍 [GOOGLE] Переклад: {len(unique_names)} нових назв")

        chunk_size = 30
        google_map = {}

        for i in range(0, len(unique_names), chunk_size):
            chunk = unique_names[i:i + chunk_size]
            context_batch = [f"część samochodowa: {n}" for n in chunk]

            try:
                translated = GoogleTranslator(source='pl', target='uk').translate_batch(context_batch)
                for orig, trans in zip(chunk, translated):
                    if ":" in trans:
                        # Видаляємо все від початку рядка до першої двокрапки включно
                        clean_ua = re.sub(r'^.*?:', '', trans).strip()
                    else:
                        clean_ua = trans.strip()

                        # Робимо першу літеру великою
                    google_map[orig] = clean_ua.capitalize()
                else:
                    google_map[orig] = orig
                time.sleep(0.3)  # Пауза, щоб Google не банив
            except Exception as e:
                print(f"❌ Помилка Google: {e}")
                for n in chunk: google_map[n] = n

        # Зберігаємо результати Google в кеш та додаємо в результати
        for p in to_google:
            c, u = str(p['code']), str(p.get('unicode', ''))
            n_pl = str(p['name']).strip().upper()
            n_uk = google_map.get(n_pl, n_pl)

            save_to_cache(supplier_id, c, u, n_pl, n_uk)
            results[(n_pl, c)] = n_uk  # Використовуємо стабільний ключ для повернення

    return results