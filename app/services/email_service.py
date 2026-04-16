import smtplib
import os
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from dotenv import load_dotenv

try:
    from services.exchange import get_eur_to_uah
except ImportError:
    from .exchange import get_eur_to_uah

load_dotenv()

class EmailService:
    # Словник постачальників (додай сюди свої реальні назви)
    SUPPLIERS = {
        1: "AUTOPARTNER",
        2: "AP_GDANSK",
        3: "MOTOROL",
    }

    @staticmethod
    def get_supplier_name(supplier_id):
        """Перетворює ID постачальника в назву."""
        try:
            # Перетворюємо в int на випадок, якщо прийшов рядок
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

            # Основні дані
            recipient_email = order_data.get('user_email')
            order_id = order_data.get('order_id')
            user_name = order_data.get('user_name', 'Клієнт')
            user_phone = order_data.get('user_phone', 'Не вказано')
            delivery_info = order_data.get('delivery_info', 'Не вказано')

            if not recipient_email:
                print(f"❌ EmailService: Відсутня адреса отримувача для замовлення {order_id}")
                return False

            # Розрахунок товарів та курсу
            current_rate = get_eur_to_uah(add_uah=1)
            items_html = ""
            total_uah = 0

            for item in order_data.get('items', []):
                price_eur = float(item.get('price_eur', 0))
                qty = int(item.get('quantity', 1))

                # --- ОСЬ ТУТ ДІСТАЄМО ПОСТАЧАЛЬНИКА ---
                supplier_id = item.get('supplier_id')
                supplier_name = EmailService.get_supplier_name(supplier_id)
                # --------------------------------------

                item_price_uah = int(price_eur * current_rate)
                subtotal = item_price_uah * qty
                total_uah += subtotal

                items_html += f"""
                <tr style="border-bottom: 1px solid #eeeeee;">
                    <td style="padding: 12px; font-family: verdana, geneva, sans-serif; font-size: 14px;">
                        {item.get('brand')}<strong>{item.get('code')}</strong><br/>
                        <span style="font-size: 10px; color: #666;">{item.get('name')}</span><br/>
                        <span style="font-size: 8px; color: #666;">Склад: <strong>{supplier_name}</strong></span>
                    </td>
                    <td style="padding: 12px; text-align: center; font-family: verdana, geneva, sans-serif;">{qty} шт.</td>
                    <td style="padding: 12px; text-align: right; font-weight: bold; font-family: verdana, geneva, sans-serif;">{item_price_uah} грн.</td>
                </tr>
                """

            # Чистий HTML шаблон
            html_content = f"""
            <html>
            <body style="font-family: verdana, geneva, sans-serif; color: #333; line-height: 1.6;">
                <div style="max-width: 600px; margin: 0 auto; border: 1px solid #e0e0e0; padding: 25px; border-radius: 10px;">
                    <h2 style="color: #d32f2f; text-align: center; border-bottom: 2px solid #d32f2f; padding-bottom: 10px;"><href="[Website Link]" target="_blank" rel="noopener"><img src="https://pub-fcf51cc33cf647358f319200a346cc52.r2.dev/images/maxgear_logo.png" width="200px"></a></h2>
                    <p style="font-size: 16px;">Вітаємо, <strong>{user_name}</strong>!</p>
                    <p style="font-size: 16px;">Дякуємо Вам за замовлення № <strong>{order_id}</strong>. Ми вже почали його обробку.</p>

                    <div style="background: #f9f9f9; padding: 15px; border-radius: 5px; margin: 20px 0;">
                        <p style="margin: 5px 0;">Телефон: <strong>{user_phone}</strong></p>
                        <p style="margin: 5px 0;">Доставка: <strong>{delivery_info}</strong></p>
                    </div>

                    <table style="width: 100%; border-collapse: collapse;">
                        <thead>
                            <tr style="background-color: #f2f2f2;">
                                <th style="padding: 10px; text-align: left;">ТОВАР</th>
                                <th style="padding: 10px; min-width:15%">К-СТЬ</th>
                                <th style="padding: 10px; text-align: right; min-width:15%">ЦІНА</th>
                            </tr>
                        </thead>
                        <tbody>{items_html}</tbody>
                    </table>

                    <div style="margin-top: 25px; text-align: right; font-size: 18px; font-weight: bold;">
                        Разом до сплати: <span style="color: #d32f2f;">{total_uah} грн.</span>
                    </div>
                    <p style="font-size: 10px; color: #999; text-align: right; margin-top: 10px;">* Курс замовлення: {current_rate} грн/EUR</p>
                    <br>
                    <br>
                    <br>
                    <br>
                    <p>
                        <span style="font-family: verdana, geneva, sans-serif;"><strong><span style="font-size: xx-small;">КОНТАКТИ:</span></strong></span>
                    </p>
                    <ul>
                        <li style="font-family: verdana, geneva, sans-serif;"><span style="font-family: verdana, geneva, sans-serif; font-size: xx-small;"><strong>Телефон:</strong>&nbsp;+38 (097) 013-43-31</span></li>
                        <li style="font-family: verdana, geneva, sans-serif;"><span style="font-family: verdana, geneva, sans-serif; font-size: xx-small;"><strong>Viber / Whatsapp:</strong>&nbsp; +38 (097) 013-43-31</span></li>
                        <li style="font-family: verdana, geneva, sans-serif;"><span style="font-family: verdana, geneva, sans-serif; font-size: xx-small;"><strong>Skype:&nbsp;</strong>s0634949207</span></li>
                        <li style="font-family: verdana, geneva, sans-serif;"><span style="font-family: verdana, geneva, sans-serif; font-size: xx-small;"><strong>Email:</strong>&nbsp;<a rel="noopener">contact@maxgear.com.ua</a></span></li>
                    </ul>
                    <p>
                        <span style="font-family: verdana, geneva, sans-serif;"><em><span style="font-size: xx-small;">З повагою,</span></em></span>
                    </p>
                    <p style="text-align: left; margin: 0; padding: 0;"><span style="font-family: verdana, geneva, sans-serif;"><a style="text-decoration: none;" href="https://maxgear.com.ua" target="_blank" rel="noopener"> <img style="display: block; border: 0;" src="https://pub-fcf51cc33cf647358f319200a346cc52.r2.dev/images/maxgear_logo.png" alt="MaxGear Logo" width="100"> </a></span></p>
                    <br>
                    <br>
                    <p style="color: #999999; text-align: center;"><span style="font-family: verdana, geneva, sans-serif; font-size: 8pt;">Ви отримали даний лист, тому що зробили замовлення на платформі&nbsp;<a style="color: #999999; text-decoration: underline; font-weight: bold;" href="https://mg-autoparts-frontend.vercel.app/">MaxGear</a>.</span></p>
                </div>
            </body>
            </html>
            """

            # Налаштування листа
            msg = MIMEMultipart()
            msg['Subject'] = f"MaxGear | Замовлення №{order_id}"
            msg['From'] = f"MaxGear <{SENDER_EMAIL}>"
            msg['To'] = recipient_email
            msg['Bcc'] = SENDER_EMAIL  # Копія власнику
            msg.attach(MIMEText(html_content, 'html'))

            with smtplib.SMTP_SSL('smtp.gmail.com', 465) as server:
                server.login(SENDER_EMAIL, APP_PASSWORD)
                server.send_message(msg)

            print(f"✅ Лист для {order_id} успішно відправлено на {recipient_email}")
            return True

        except Exception as e:
            print(f"❌ Помилка EmailService: {e}")
            return False