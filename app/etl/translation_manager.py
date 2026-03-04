from app.services.translator import translate_products


def process_price_translation(df, supplier_id, limit=1000):
    """Готує дані та запускає переклад."""

    # --- ЗМІНА 1: Справжнє обмеження ---
    # Ми беремо тільки верхівку (head), щоб не мучити Google
    if limit:
        df_to_work = df.head(limit).copy()
    else:
        df_to_work = df.copy()

    # --- ЗМІНА 2: Робота тільки з обмеженим списком ---
    raw_list = df_to_work[['code', 'name', 'unicode']].to_dict('records')

    # 2. Отримуємо переклади (Cache -> Dict -> Google)
    translations = translate_products(raw_list, supplier_id)

    # 3. Функція-помічник для підстановки
    def get_uk_name(row):
        key = (str(row['code']), str(row['name']).strip().upper())
        return translations.get(key, row['name'])

    # --- ЗМІНА 3: Оновлення тільки потрібної частини ---
    if limit:
        # Оновлюємо тільки перші N рядків у колонці 'name'
        df.loc[:limit - 1, 'name'] = df_to_work.apply(get_uk_name, axis=1)
    else:
        # Оновлюємо весь стовпчик
        df['name'] = df.apply(get_uk_name, axis=1)

    return df