import time
from deep_translator import GoogleTranslator

# Кеш у пам'яті (RAM) для миттєвих повторів у межах однієї сесії
translation_cache = {}


def translate_batch(names_list: list) -> list:
    if not names_list:
        return []

    # 1. Відфільтровуємо те, що вже є в кеші, або не потребує перекладу
    to_translate = []
    for name in names_list:
        name_str = str(name).strip()

        # Перевірка: чи варто це перекладати?
        # Пропускаємо, якщо: вже в кеші, порожньо, або немає літер (тільки цифри/символи)
        if name_str and name_str not in translation_cache:
            if any(c.isalpha() for c in name_str) and len(name_str) > 2:
                to_translate.append(name_str)
            else:
                # Якщо це просто цифри (напр. "123.45"), відразу кешуємо як є
                translation_cache[name_str] = name_str

    # 2. Перекладаємо порціями
    if to_translate:
        chunk_size = 100  # Максимум для стабільності Google
        for i in range(0, len(to_translate), chunk_size):
            chunk = to_translate[i: i + chunk_size]

            print(f"[GOOGLE] Перекладаю порцію {i // chunk_size + 1} (фрази {i} - {i + len(chunk)})...")

            try:
                # Виклик Google API
                translated_chunk = GoogleTranslator(source='pl', target='uk').translate_batch(chunk)

                # Записуємо результат у кеш
                for orig, trans in zip(chunk, translated_chunk):
                    translation_cache[orig] = trans

                # --- АНТИ-БАН ПАУЗА ---
                # Робимо невеличку перерву, щоб Google не сприйняв нас за бота
                time.sleep(0.5)

            except Exception as e:
                print(f"[ERROR] Помилка в пакетному перекладі: {e}")
                # У разі помилки (напр. таймаут) — робимо паузу довше і залишаємо оригінали
                time.sleep(2)
                for orig in chunk:
                    translation_cache[orig] = orig

    # 3. Повертаємо фінальний список
    return [translation_cache.get(str(name).strip(), name) for name in names_list]