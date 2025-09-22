import os
import pymysql
import requests
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

# --- Настройки Railway ---
DB_HOST = os.getenv("DB_HOST")
DB_PORT = int(os.getenv("DB_PORT", "3306"))
DB_USER = os.getenv("DB_USER")
DB_PASS = os.getenv("DB_PASS")
DB_NAME = os.getenv("DB_NAME")

BOT_TOKEN = os.getenv("BOT_TOKEN")
CHAT_ID = os.getenv("CHAT_ID")

# Часовой пояс
TZ = ZoneInfo("Asia/Baku")


def send_message(text: str):
    """Отправка сообщения в Telegram"""
    url = f"https://api.telegram.org/bot{BOT_TOKEN}/sendMessage"
    payload = {"chat_id": CHAT_ID, "text": text}
    try:
        r = requests.post(url, data=payload, timeout=10)
        r.raise_for_status()
    except Exception as e:
        print("Ошибка отправки в Telegram:", e)


def main():
    now = datetime.now(TZ)
    today_weekday = now.isoweekday()  # 1=Mon ... 7=Sun

    # Окно проверки (-5 до +5 минут от текущего времени)
    window_start = (now - timedelta(minutes=5)).strftime("%H:%M:%S")
    window_end = (now + timedelta(minutes=5)).strftime("%H:%M:%S")

    connection = pymysql.connect(
        host=DB_HOST, port=DB_PORT, user=DB_USER,
        password=DB_PASS, database=DB_NAME,
        cursorclass=pymysql.cursors.DictCursor
    )

    try:
        with connection.cursor() as cur:
            # Уроки, попадающие в интервал
            cur.execute("""
                SELECT sc.user_id, s.name, s.money, sc.time
                FROM schedule sc
                JOIN stud s ON s.user_id = sc.user_id
                WHERE sc.weekday = %s
                  AND TIME(sc.time) BETWEEN %s AND %s
            """, (today_weekday, window_start, window_end))
            lessons_now = cur.fetchall()

            for ln in lessons_now:
                user_id = ln["user_id"]
                student_name = ln["name"]
                lesson_price = ln["money"] or 0
                lesson_time = ln["time"]

                # Проверяем, не было ли уже уведомления
                cur.execute("""
                    SELECT 1 FROM notifications
                    WHERE user_id=%s AND lesson_time=%s
                """, (user_id, lesson_time))
                if cur.fetchone():
                    continue  # уже отправлено

                # Считаем статистику по ученику
                cur.execute("""
                    SELECT 
                        (SELECT COUNT(*) FROM dates WHERE user_id=%s AND visited=1) AS lessons_done,
                        (SELECT COALESCE(SUM(lessons),0) FROM pays WHERE user_id=%s) AS lessons_paid,
                        (SELECT MAX(date) FROM pays WHERE user_id=%s) AS last_pay_date,
                        (SELECT amount FROM pays WHERE user_id=%s ORDER BY date DESC LIMIT 1) AS last_pay_amount
                """, (user_id, user_id, user_id, user_id))
                stats = cur.fetchone()

                lessons_done = stats["lessons_done"] or 0
                lessons_paid = stats["lessons_paid"] or 0
                last_date = stats["last_pay_date"] or "—"
                last_amount = stats["last_pay_amount"] or 0

                debt = lessons_done - lessons_paid
                if debt > 0 and lessons_done >= 9:
                    total_debt = debt * lesson_price
                    msg = (f"⚠️ Ученик: {student_name}\n"
                           f"Сейчас идёт урок №{lessons_done}\n"
                           f"Оплачено уроков: {lessons_paid}\n"
                           f"Должен: {debt} урок(а)\n"
                           f"Общая сумма долга: {total_debt} AZN\n"
                           f"Последняя оплата: {last_date} (сумма {last_amount})")
                    send_message(msg)

                    # Записываем факт уведомления
                    cur.execute("""
                        INSERT INTO notifications (user_id, lesson_time) 
                        VALUES (%s, %s)
                    """, (user_id, lesson_time))
                    connection.commit()

    finally:
        connection.close()


if __name__ == "__main__":
    main()
