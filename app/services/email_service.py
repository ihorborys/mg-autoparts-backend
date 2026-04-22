import smtplib
import os
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from dotenv import load_dotenv

load_dotenv()


class EmailService:
    # Словник постачальників
    SUPPLIERS = {
        1: "AUTOPARTNER",
        2: "AP_GDANSK",
        3: "MOTOROL",
    }

    @staticmethod
    def get_supplier_name(supplier_id):
        """Перетворює ID постачальника в назву."""
        try:
            sid = int(supplier_id)
            return EmailService.SUPPLIERS.get(sid, f"Постачальник #{sid}")
        except:
            return "Невідомий постачальник"

    @staticmethod
    def send_order_confirmation(order_data: dict):

        """Відправка підтвердження замовлення через SMTP Gmail."""
        try:
            SENDER_EMAIL = os.getenv("MAIL_USERNAME")
            APP_PASSWORD = os.getenv("MAIL_PASSWORD")

            # Основні дані з payload
            recipient_email = order_data.get('user_email')
            order_id = order_data.get('order_id')
            full_user_name = order_data.get('full_user_name', 'Не вказано')  # Беремо ПІБ
            first_name = order_data.get('first_name', 'Не вказано')
            last_name = order_data.get('last_name', 'Не вказано')
            user_phone = order_data.get('user_phone', 'Не вказано')
            delivery_info = order_data.get('delivery_info', 'Не вказано')
            notes = order_data.get('notes', 'Не вказано')


            # Чисті ціни (FastAPI вже перевірив їх через Pydantic)
            total_uah = int(order_data.get('total_price_uah', 0))

            notes_html = f'<p style="margin: 5px 0;">Примітка: <strong>{notes}</strong></p>' if notes else ""

            # 1. Створюємо зрозумілу назву для способу оплати
            raw_payment = order_data.get('payment_method', 'cod')
            payment_text = "при отриманні" if raw_payment == 'cod' else "на картку"

            # order_rate = order_data.get('order_rate', 'фіксований')  # Беремо курс з фронта

            if not recipient_email:
                print(f"❌ EmailService: Відсутня адреса отримувача для замовлення {order_id}")
                return False

            items_html = ""

            for item in order_data.get('items', []):
                # --- 2. ОБРОБКА ЦІНИ ТОВАРУ (Гарантуємо інт) ---
                item_price = int(item.get('price_uah', 0))

                qty = int(item.get('quantity', 1))

                supplier_id = item.get('supplier_id')
                supplier_name = EmailService.get_supplier_name(supplier_id)

                items_html += f"""
                <tr style="border-bottom: 1px solid #eeeeee;">
                    <td style="padding: 12px; font-family: verdana, geneva, sans-serif; font-size: 14px;">
                        {item.get('brand')} <strong>{item.get('code')}</strong><br/>
                        <span style="font-size: 10px; color: #666;">{item.get('name')}</span><br/>
                        <span style="font-size: 8px; color: #666;">Склад: <strong>{supplier_name}</strong></span>
                    </td>
                    <td style="padding: 12px; text-align: center; font-family: verdana, geneva, sans-serif;">{qty} шт.</td>
                    <td style="padding: 12px; text-align: right; font-weight: bold; font-family: verdana, geneva, sans-serif;">{item_price} грн.</td>
                </tr>
                """

            html_content = f"""
            <html>
            <body style="font-family: verdana, geneva, sans-serif; color: #333; line-height: 1.6;">
                <div style="max-width: 600px; margin: 0 auto; border: 1px solid #e0e0e0; padding: 25px; border-radius: 10px;">
                    <div style="text-align: center; border-bottom: 2px solid #d32f2f; padding-bottom: 10px; margin-bottom: 20px;">
                        <a href="https://maxgear.com.ua" target="_blank" rel="noopener">
                            <img src="https://pub-fcf51cc33cf647358f319200a346cc52.r2.dev/images/images_maxgear_logo.jpg" width="200" alt="">
                        </a>
                    </div>

                    <p style="font-size: 16px;">Вітаємо, <strong>{first_name}</strong>!</p>
                    <p style="font-size: 16px;">Дякуємо Вам за замовлення № <strong>{order_id}</strong>. Ми вже почали його обробку.</p>

                    <div style="background: #f9f9f9; padding: 15px; border-radius: 5px; margin: 20px 0;">
                        <p style="margin: 5px 0;">Отримувач: <strong>{full_user_name}</strong></p>
                        <p style="margin: 5px 0;">Телефон: <strong>{user_phone}</strong></p>
                        <p style="margin: 5px 0;">Доставка: <strong>{delivery_info}</strong></p>
                        <p style="margin: 5px 0;">Оплата: <strong>{payment_text}</strong></p>
                        {notes_html}
                    </div>

                    <table style="width: 100%; border-collapse: collapse;">
                        <thead>
                            <tr style="background-color: #f2f2f2;">
                                <th style="padding: 10px; text-align: left;">ТОВАР</th>
                                <th style="padding: 10px; min-width:20%">К-СТЬ</th>
                                <th style="padding: 10px; text-align: right; min-width:20%">ЦІНА</th>
                            </tr>
                        </thead>
                        <tbody>{items_html}</tbody>
                    </table>

                    <div style="margin-top: 25px; text-align: right; font-size: 18px; font-weight: bold;">
                        Разом до сплати: <span style="color: #d32f2f;">{total_uah} грн.</span>
                    </div>

                    <div style="margin-top: 40px; border-top: 1px solid #eee; padding-top: 20px;">
                        <p style="font-size: 11px; font-weight: bold; margin-bottom: 10px;">КОНТАКТИ:</p>
                        <ul style="list-style: none; padding: 0; font-size: 11px; color: #555;">
                            <li><strong>Телефон:</strong> +38 (097) 013-43-31</li>
                            <li><strong>Viber / WhatsApp:</strong> +38 (097) 013-43-31</li>
                            <li><strong>Email:</strong> contact@maxgear.com.ua</li>
                        </ul>
                    </div>

                    <p style="text-align: left; margin: 30px 0 0 0;">
                        <a style="text-decoration: none;" href="https://maxgear.com.ua" target="_blank" rel="noopener">
                            <img src="https://pub-fcf51cc33cf647358f319200a346cc52.r2.dev/images/images_maxgear_logo.jpg" alt="MaxGear Logo" width="100">
                        </a>
                    </p>
                    <p style="color: #999999; text-align: center; font-size: 8pt; margin-top: 30px;">
                        Ви отримали даний лист, тому що зробили замовлення на платформі
                        <a style="color: #999999; text-decoration: underline; font-weight: bold;" href="https://mg-autoparts-frontend.vercel.app/">MaxGear</a>.
                    </p>
                </div>
            </body>
            </html>
            """

            # Налаштування листа
            msg = MIMEMultipart()
            msg['Subject'] = f"MaxGear | Замовлення №{order_id}"
            msg['From'] = f"MaxGear <{SENDER_EMAIL}>"
            msg['To'] = recipient_email
            msg['Bcc'] = SENDER_EMAIL
            msg.attach(MIMEText(html_content, 'html'))

            with smtplib.SMTP_SSL('smtp.gmail.com', 465) as server:
                server.login(SENDER_EMAIL, APP_PASSWORD)
                server.send_message(msg)

            print(f"✅ Лист для {order_id} успішно відправлено на {recipient_email}")
            return True

        except Exception as e:
            print(f"❌ Помилка EmailService: {e}")
            return False

