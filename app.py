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
        # считаем, что из БД приходит наивный локальный dt (без tz)
        dt = dt.replace(tzinfo=TZ)
        return dt.strftime("%d.%m.%Y %H:%M")
    try:
        parsed = datetime.fromisoformat(str(dt)).replace(tzinfo=TZ)
        return parsed.strftime("%d.%m.%Y %H:%M")
    except Exception:
        return str(dt)


def minutes_of_day(d: datetime) -> int:
    """Минуты с начала суток для datetime."""
    return d.hour * 60 + d.minute


def main():
    now = datetime.now(TZ)
    today_weekday = now.isoweekday()  # 1=Mon ... 7=Sun

    # Окно: -5 до +5 минут (в минутах с начала суток)
    win_start_dt = now - timedelta(minutes=5)
    win_end_dt   = now + timedelta(minutes=5)
    start_total = minutes_of_day(win_start_dt)
    end_total   = minutes_of_day(win_end_dt)

    connection = pymysql.connect(
        host=DB_HOST, port=DB_PORT, user=DB_USER,
        password=DB_PASS, database=DB_NAME,
        cursorclass=pymysql.cursors.DictCursor
    )

    try:
        with connection.cursor() as cur:
            # Берём расписание ТОЛЬКО по дню недели — без сравнения времени в SQL
            cur.execute("""
                SELECT sc.user_id, s.name, s.money AS lesson_price, sc.time AS lesson_time
                FROM schedule sc
                JOIN stud s ON s.user_id = sc.user_id
                WHERE sc.weekday = %s
            """, (today_weekday,))
            all_today = cur.fetchall()

            # Фильтруем по окну времени уже в Python
            # (никаких TIME/DATETIME в SQL -> никаких ошибок)
            lessons_now = []
            for row in all_today:
                lt = row["lesson_time"]
                # Если из БД пришло без tz — считаем это локальным временем
                if isinstance(lt, datetime):
                    lesson_minutes = minutes_of_day(lt)
                else:
                    # если вдруг строка — попробуем разобрать "YYYY-MM-DD HH:MM:SS"
                    try:
                        parsed = datetime.fromisoformat(str(lt))
                        lesson_minutes = minutes_of_day(parsed)
                    except Exception:
                        # если совсем нестандартный формат — пропускаем слот
                        continue

                if start_total <= end_total:
                    in_window = (start_total <= lesson_minutes <= end_total)
                else:
                    # редкий случай пересечения полуночи
                    in_window = (lesson_minutes >= start_total or lesson_minutes <= end_total)

                if in_window:
                    lessons_now.append(row)

            for row in lessons_now:
                user_id      = row["user_id"]
                student_name = row["name"]
                lesson_price = row["lesson_price"] or 0
                lesson_time  = row["lesson_time"]  # DATETIME / строка; в БД он как есть

                # Не дублировать уведомление для этого слота
                cur.execute("""
                    SELECT 1 FROM notifications
                    WHERE user_id = %s AND lesson_time = %s
                    LIMIT 1
                """, (user_id, lesson_time))
                if cur.fetchone():
                    continue

                # Счётчики посещённых/оплаченных и последний платёж
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

                # Условие: есть долг и это >= 9-й урок — уведомляем
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

                    # Фиксируем факт отправки
                    cur.execute("""
                        INSERT INTO notifications (user_id, lesson_time)
                        VALUES (%s, %s)
                    """, (user_id, lesson_time))
                    connection.commit()

    finally:
        connection.close()


if __name__ == "__main__":
    main()
