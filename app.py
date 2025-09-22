import os
import pymysql
import requests
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

# --- ENV (Railway Variables) ---
DB_HOST = os.getenv("DB_HOST")
DB_PORT = int(os.getenv("DB_PORT", "3306"))
DB_USER = os.getenv("DB_USER")
DB_PASS = os.getenv("DB_PASS")
DB_NAME = os.getenv("DB_NAME")

BOT_TOKEN = os.getenv("BOT_TOKEN")
CHAT_ID = os.getenv("CHAT_ID")  # для группы ID отрицательный, напр. -4811468174

# Часовой пояс Баку
TZ = ZoneInfo("Asia/Baku")


def send_message(text: str):
    """Отправить сообщение в Telegram."""
    if not BOT_TOKEN or not CHAT_ID:
        print("BOT_TOKEN/CHAT_ID не заданы")
        return
    url = f"https://api.telegram.org/bot{BOT_TOKEN}/sendMessage"
    payload = {"chat_id": CHAT_ID, "text": text}
    try:
        r = requests.post(url, data=payload, timeout=10)
        r.raise_for_status()
    except Exception as e:
        print("Ошибка отправки в Telegram:", e)


def fmt_dt(dt):
    """Форматируем дату/время (или '—')."""
    if not dt:
        return "—"
    if isinstance(dt, datetime):
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=TZ)
        else:
            dt = dt.astimezone(TZ)
        return dt.strftime("%d.%m.%Y %H:%M")
    try:
        parsed = datetime.fromisoformat(str(dt))
        parsed = parsed.replace(tzinfo=TZ)
        return parsed.strftime("%d.%m.%Y %H:%M")
    except Exception:
        return str(dt)


def main():
    now = datetime.now(TZ)
    today_weekday = now.isoweekday()  # 1=Mon ... 7=Sun

    # Окно: -5 до +5 минут
    win_start = now - timedelta(minutes=5)
    win_end   = now + timedelta(minutes=5)

    # Переводим окно в "минуты с начала суток"
    s_h, s_m = win_start.hour, win_start.minute
    e_h, e_m = win_end.hour,   win_end.minute
    start_total = s_h * 60 + s_m
    end_total   = e_h * 60 + e_m

    connection = pymysql.connect(
        host=DB_HOST, port=DB_PORT, user=DB_USER,
        password=DB_PASS, database=DB_NAME,
        cursorclass=pymysql.cursors.DictCursor
    )

    try:
        with connection.cursor() as cur:
            # Приводим время урока к "минутам с начала суток"
            lesson_minutes_expr = "HOUR(sc.time)*60 + MINUTE(sc.time)"

            if end_total >= start_total:
                # Обычный случай в пределах суток
                cur.execute(f"""
                    SELECT sc.user_id, s.name, s.money AS lesson_price, sc.time AS lesson_time
                    FROM schedule sc
                    JOIN stud s ON s.user_id = sc.user_id
                    WHERE sc.weekday = %s
                      AND ({lesson_minutes_expr}) BETWEEN %s AND %s
                """, (today_weekday, start_total, end_total))
            else:
                # Окно пересекает полночь (редко, но корректно обработаем):
                # условие: ( >= start_total ИЛИ <= end_total )
                cur.execute(f"""
                    SELECT sc.user_id, s.name, s.money AS lesson_price, sc.time AS lesson_time
                    FROM schedule sc
                    JOIN stud s ON s.user_id = sc.user_id
                    WHERE sc.weekday = %s
                      AND ( ({lesson_minutes_expr}) >= %s OR ({lesson_minutes_expr}) <= %s )
                """, (today_weekday, start_total, end_total))

            lessons_now = cur.fetchall()

            for row in lessons_now:
                user_id      = row["user_id"]
                student_name = row["name"]
                lesson_price = row["lesson_price"] or 0
                lesson_time  = row["lesson_time"]  # DATETIME из schedule

                # Не дублировать уведомление для этого слота
                cur.execute("""
                    SELECT 1 FROM notifications
                    WHERE user_id = %s AND lesson_time = %s
                    LIMIT 1
                """, (user_id, lesson_time))
                if cur.fetchone():
                    continue

                # Счётчики
                cur.execute("""
                    SELECT 
                        (SELECT COUNT(*) 
                           FROM dates 
                          WHERE user_id = %s AND visited = 1) AS lessons_done,
                        (SELECT COALESCE(SUM(lessons), 0) 
                           FROM pays 
                          WHERE user_id = %s) AS lessons_paid,
                        (SELECT MAX(date) 
                           FROM pays 
                          WHERE user_id = %s) AS last_pay_date,
                        (SELECT amount 
                           FROM pays 
                          WHERE user_id = %s 
                          ORDER BY date DESC LIMIT 1) AS last_pay_amount
                """, (user_id, user_id, user_id, user_id))
                stats = cur.fetchone() or {}

                lessons_done  = stats.get("lessons_done", 0) or 0
                lessons_paid  = stats.get("lessons_paid", 0) or 0
                last_pay_date = stats.get("last_pay_date")
                last_pay_amt  = stats.get("last_pay_amount") or 0

                debt_lessons = lessons_done - lessons_paid

                # Условие: долг есть и это >= 9-й урок
                if debt_lessons > 0 and lessons_done >= 9:
                    total_debt_azn = debt_lessons * (lesson_price or 0)
                    msg = (
                        f"⚠️ Напоминание об оплате\n"
                        f"Ученик: {student_name}\n"
                        f"Время урока: {fmt_dt(lesson_time)}\n"
                        f"Сейчас идёт урок №{lessons_done}\n"
                        f"Оплачено уроков: {lessons_paid}\n"
                        f"Долг: {debt_lessons} урок(а)\n"
                        f"Цена урока: {lesson_price} AZN\n"
                        f"Общая сумма долга: {total_debt_azn} AZN\n"
                        f"Последний платёж: {fmt_dt(last_pay_date)} (сумма {last_pay_amt})"
                    )
                    send_message(msg)

                    # Зафиксировать отправку
                    cur.execute("""
                        INSERT INTO notifications (user_id, lesson_time)
                        VALUES (%s, %s)
                    """, (user_id, lesson_time))
                    connection.commit()

    finally:
        connection.close()


if __name__ == "__main__":
    main()
