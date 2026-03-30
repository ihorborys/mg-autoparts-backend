# import requests
#
#
# def get_eur_to_uah(add_uah=1, min_rate=49, fallback=50, timeout=5) -> float:
#     """
#     Отримати курс EUR→UAH: курс НБУ + надбавка, з мінімальним порогом і фолбеком.
#     """
#     try:
#         r = requests.get(
#             "https://bank.gov.ua/NBUStatService/v1/statdirectory/exchange?valcode=EUR&json",
#             timeout=timeout,
#         )
#         r.raise_for_status()
#         rate = float(r.json()[0]["rate"])
#         rate += float(add_uah or 0)
#         return max(rate, float(min_rate or 0))
#     except Exception:
#         return float(fallback)


import requests
import time

# Глобальні змінні для зберігання даних у пам'яті сервера
_cached_rate = None
_last_updated = 0
CACHE_DURATION = 3600  # 1 година (в секундах)


def get_eur_to_uah(add_uah=1, min_rate=50, fallback=52, timeout=5) -> float:
    global _cached_rate, _last_updated

    current_time = time.time()

    # 1. Якщо в пам'яті вже є курс і він свіжий — віддаємо його миттєво
    if _cached_rate is not None and (current_time - _last_updated < CACHE_DURATION):
        return _cached_rate

    # 2. Якщо кешу немає або він застарів — ідемо в НБУ
    try:
        r = requests.get(
            "https://bank.gov.ua/NBUStatService/v1/statdirectory/exchange?valcode=EUR&json",
            timeout=timeout,
        )
        r.raise_for_status()

        # Обчислюємо фінальний курс
        raw_rate = float(r.json()[0]["rate"])
        final_rate = max(raw_rate + float(add_uah or 0), float(min_rate or 0))

        # Оновлюємо глобальний кеш
        _cached_rate = final_rate
        _last_updated = current_time

        print(f"--- КУРС ОНОВЛЕНО: {_cached_rate} UAH ---")
        return _cached_rate

    except Exception as e:
        print(f"Помилка НБУ: {e}")

        # 3. Якщо НБУ не відповів, але в нас є ХОЧ ЯКИЙСЬ старий курс — віддаємо його
        if _cached_rate is not None:
            print("Використовуємо попередній успішний курс із пам'яті")
            return _cached_rate

        # 4. Якщо взагалі нічого немає (перший запуск сервера) — віддаємо fallback
        return float(fallback)