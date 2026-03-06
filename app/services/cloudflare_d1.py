import os

import requests
from dotenv import load_dotenv


class CloudflareD1Manager:
    def __init__(self):
        # Завантажуємо змінні з .env
        load_dotenv()

        # Отримуємо дані через os.getenv
        self.account_id = os.getenv("CLOUDFLARE_ACCOUNT_ID")
        self.database_id = os.getenv("CLOUDFLARE_DATABASE_ID")
        self.api_token = os.getenv("CLOUDFLARE_API_TOKEN")

        # Перевірка (опціонально), щоб код не впав без ключів
        if not all([self.account_id, self.database_id, self.api_token]):
            raise ValueError("❌ Помилка: У файлі .env відсутні ключі Cloudflare!")

        self.url = f"https://api.cloudflare.com/client/v4/accounts/{self.account_id}/d1/database/{self.database_id}/query"
        self.headers = {
            "Authorization": f"Bearer {self.api_token}",
            "Content-Type": "application/json"
        }

    def execute(self, sql, params=None):
        """Виконує один SQL запит"""
        payload = {"sql": sql, "params": params or []}
        try:
            response = requests.post(self.url, headers=self.headers, json=payload)
            return response.json()
        except Exception as e:
            print(f"❌ Помилка Cloudflare: {e}")
            return None

    def get_cached_translation(self, supplier_id, code, pl_text):
        """Шукає переклад у хмарі"""
        sql = "SELECT uk_text FROM dict WHERE supplier_id = ? AND code = ? AND pl_text = ? LIMIT 1"
        res = self.execute(sql, [supplier_id, code, pl_text.upper()])

        if res and res.get('success') and res['result'][0]['results']:
            return res['result'][0]['results'][0]['uk_text']
        return None

    def save_to_cache(self, supplier_id, code, unicode_val, pl_text, uk_text):
        """Зберігає або оновлює переклад у хмарі"""
        sql = """
        INSERT INTO dict (supplier_id, code, unicode, pl_text, uk_text) 
        VALUES (?, ?, ?, ?, ?)
        ON CONFLICT(supplier_id, code, pl_text) DO UPDATE SET uk_text = excluded.uk_text
        """
        params = [supplier_id, code, str(unicode_val), pl_text.upper(), uk_text]
        return self.execute(sql, params)