import os
import pymysql
import requests
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

# Настройки (берём из переменных Railway)
DB_HOST = os.getenv("DB_HOST")
DB_PORT = int(os.getenv("DB_PORT", "3306"))
DB_USER = os.getenv("DB_USER")
DB_PASS = os.getenv("DB_PASS")
DB_NAME = os.getenv("DB_NAME")

BOT_TOKEN = os.getenv("BOT_TOKEN")
CHAT_ID = os.getenv("CHAT_ID")  # -4811468174

# Часовой пояс Баку
TZ = ZoneInfo("Asia/Baku")

def send_message(text: str):
    url = f"https://api.telegram.org/bot{BOT_TOKEN}/sendMessage"
    payload = {"chat_id": CHAT_ID, "text": text}
    try:
        r = requests.post(url, data=payload, timeout=10)
        r.raise_for_status()
    except Exception as e:
        print("Ошибка отправки в Telegram:", e)

def main():
    now_local = datetime.now(TZ).replace(second=0, microsecond=0)
    window_start = now_local
    window_end = now_local + timedelta(minutes=1)

    connection = pymysql.connect(
        host=DB_HOST, port=DB_PORT,
        user=DB_USER, password=DB_PASS,
        database=DB_NAME, cursorclass=pymysql.cursors.DictCursor
    )
    try:
        with connection.cursor() as cur:
            # ⚠️ поправь под реальную структуру БД
            cur.execute("""
                SELECT s.full_name, l.lesson_number, l.lesson_start
                FROM lessons l
                JOIN students s ON s.id = l.student_id
                WHERE l.lesson_number >= 9
                  AND l.lesson_start >= %s
                  AND l.lesson_start < %s
                  AND COALESCE(l.sent_payment_notice, 0) = 0
            """, (window_start, window_end))
            rows = cur.fetchall()

            for r in rows:
                msg = (f"💳 Оплата нужна!\n"
                       f"Ученик: {r['full_name']}\n"
                       f"Урок: {r['lesson_number']}\n"
                       f"Время: {r['lesson_start']}")
                send_message(msg)

            if rows:
                cur.execute("""
                    UPDATE lessons
                    SET sent_payment_notice = 1
                    WHERE lesson_number >= 9
                      AND lesson_start >= %s AND lesson_start < %s
                """, (window_start, window_end))
                connection.commit()
    finally:
        connection.close()

if __name__ == "__main__":
    main()
