from app.services.dictionaries import BRANDS_DICT


def normalize_brand(raw_brand):
    """Перетворює 'ORG HON' на 'HONDA / ACURA'"""
    if not raw_brand:
        return "UNKNOWN"

    # Очищаємо від пробілів та переводимо у верхній регістр для порівняння
    brand_key = str(raw_brand).strip().upper()

    # Шукаємо у словнику. Якщо немає — залишаємо як було
    return BRANDS_DICT.get(brand_key, raw_brand)