from dotenv import load_dotenv, find_dotenv

load_dotenv(find_dotenv())

from app import process_one_price

if __name__ == "__main__":
    csv_path = "data/temp/09033.cennik.csv"  # поклади сюди свій сформатований CSV

    key, url = process_one_price(
        remote_gz_path=csv_path,  # локальний CSV → піде без FTP
        supplier="MOTOROL",
        supplier_id=3,
        factor=1.27,
        currency_out="EUR",
        format_="xlsx",
        rounding={"EUR": 2, "UAH": 0},
        r2_prefix="1_27/motorol/",
        columns=[
            {"from": "code", "header": "code"},
            {"from": "brand", "header": "brand"},
            {"from": "stock", "header": "stock"},
            {"from": "price", "header": "price_EUR"},
        ],
        delete_input_after=True,  # прибрати вхідний CSV після успішної обробки
    )
    print("OK:", key, url)
