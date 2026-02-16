# python test_ftp_login.py
import os, ftplib
from dotenv import load_dotenv

load_dotenv("../.env")  # шлях до твого .env

host = os.getenv("FTP_HOST")
# user = "u458439667.zatoka"
# pwd = "f3#S]a|dJ"
user = os.getenv("FTP_USER")
pwd = os.getenv("FTP_PASS")

print("HOST:", repr(host))
print("USER:", repr(user))
print("PASS:", repr(pwd))  # тимчасово! переконайся що тут рівно те саме, що у FileZilla


def test_ftps():
    print("\n=== FTPS (explicit TLS) ===")
    try:
        t = ftplib.FTP_TLS(host, timeout=20)
        print("welcome:", t.getwelcome())
        t.auth()  # AUTH TLS
        resp = t.login(user, pwd)  # ЛОГІН ПЕРЕД prot_p
        print("login:", resp)
        t.prot_p()  # захист data channel після login
        print("pwd:", t.pwd())
        t.quit()
    except Exception as e:
        print("FTPS ERROR:", repr(e))


def test_ftp():
    print("\n=== Plain FTP ===")
    try:
        f = ftplib.FTP(host, timeout=20)
        print("welcome:", f.getwelcome())
        print("login:", f.login(user, pwd))
        print("pwd:", f.pwd())
        f.quit()
    except Exception as e:
        print("FTP ERROR:", repr(e))


test_ftps()
test_ftp()
