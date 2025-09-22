import os
import pymysql
import requests
from datetime import datetime
from zoneinfo import ZoneInfo

DB_HOST = os.getenv("DB_HOST")
DB_PORT = int(os.getenv("DB_PORT", "3306"))
DB_USER = os.getenv("DB_USER")
DB_PASS = os.getenv("DB_PASS")
DB_NAME = os.getenv("DB_NAME")

BOT_TOKEN = os.getenv("BOT_TOKEN")
CHAT_ID = os.getenv("CHAT_ID")

TZ = ZoneInfo("Asia/Baku")

def send_message(text: str):
    url = f"https://api.telegram.org/bot{BOT_TOKEN}/sendMessage"
    requests.post(url, data={"chat_id": CHAT_ID, "text": text})

def main():
    now = datetime.now(TZ)
    today_weekday = now.isoweekday()  # 1=Mon .. 7=Sun
    now_time = now.strftime("%H:%M:%S")

    connection = pymysql.connect(
        host=DB_HOST, port=DB_PORT, user=DB_USER,
        password=DB_PASS, database=DB_NAME,
        cursorclass=pymysql.cursors.DictCursor
    )

    try:
        with connection.cursor() as cur:
            # Найдём всех у кого сейчас по расписанию урок
            cur.execute("""
                SELECT sc.user_id, s.name, sc.time
                FROM schedule sc
                JOIN stud s ON s.user_id = sc.user_id
                WHERE sc.weekday = %s AND sc.time = %s
            """, (today_weekday, now_time))
            lessons_now = cur.fetchall()

            for ln in lessons_now:
                user_id = ln["user_id"]
                student_name = ln["name"]

                # Подсчёт уроков и оплат
                cur.execute("""
                    SELECT 
                        (SELECT COUNT(*) FROM dates WHERE user_id=%s AND visited=1) AS lessons_done,
                        (SELECT COALESCE(SUM(lessons),0) FROM pays WHERE user_id=%s) AS lessons_paid,
                        (SELECT MAX(date) FROM pays WHERE user_id=%s) AS last_pay_date,
                        (SELECT amount FROM pays WHERE user_id=%s ORDER BY date DESC LIMIT 1) AS last_pay_amount
                """, (user_id, user_id, user_id, user_id))
                stats = cur.fetchone()

                lessons_done = stats["lessons_done"]
                lessons_paid = stats["lessons_paid"]
                last_date = stats["last_pay_date"]
                last_amount = stats["last_pay_amount"]

                debt = lessons_done - lessons_paid
                if debt > 0 and lessons_done >= 9:  # долг и 9-й урок или позже
                    msg = (f"⚠️ Ученик: {student_name}\n"
                           f"Прошёл уроков: {lessons_done}\n"
                           f"Оплачено уроков: {lessons_paid}\n"
                           f"Должен: {debt} урок(а)\n"
                           f"Последняя оплата: {last_date} (сумма {last_amount})")
                    send_message(msg)

    finally:
        connection.close()

if __name__ == "__main__":
    main()
