import requests


def get_eur_to_uah(add_uah=1, min_rate=49, fallback=50, timeout=5) -> float:
    """
    Отримати курс EUR→UAH: курс НБУ + надбавка, з мінімальним порогом і фолбеком.
    """
    try:
        r = requests.get(
            "https://bank.gov.ua/NBUStatService/v1/statdirectory/exchange?valcode=EUR&json",
            timeout=timeout,
        )
        r.raise_for_status()
        rate = float(r.json()[0]["rate"])
        rate += float(add_uah or 0)
        return max(rate, float(min_rate or 0))
    except Exception:
        return float(fallback)
