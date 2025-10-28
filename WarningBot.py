import psycopg2
from psycopg2 import Error
import telebot
from telebot import types
import logging
from datetime import datetime
import os
from dotenv import load_dotenv

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def main(sql_file, sql_insert, sql_select):
    load_dotenv()

    BOT_TOKEN = os.getenv('BOT_TOKEN')
    CHAT_ID = os.getenv('CHAT_ID')

    DB_CONFIG = {
        "user": os.getenv('USER'),
        "password": os.getenv('PASSWORD'),
        "host": os.getenv('HOST'),
        "port": os.getenv('PORT'),
        "database": os.getenv('DATABASE'),
    }

    send_telegram_message(BOT_TOKEN, CHAT_ID, DB_CONFIG, sql_file, sql_insert, sql_select)


def get_existing_credit_ids(connection, sql_select):
    """Получение всех существующих ID кредитов из БД"""
    try:
        cursor = connection.cursor()
        query = open(sql_select, 'r', encoding='utf-8').read()
        cursor.execute(query)
        results = cursor.fetchall()

        # Возвращаем множество ID кредитов
        return {result[0] for result in results}
    except Exception as e:
        logger.error(f"Ошибка при получении существующих ID кредитов: {e}")
        return set()
    finally:
        if cursor: cursor.close()


def save_new_records(connection, new_records, sql_insert):
    """Сохранение новых записей в БД"""
    try:
        cursor = connection.cursor()
        query = open(sql_insert, 'r', encoding='utf-8').read()

        for record in new_records:
            if len(record) >= 2:
                cursor.execute(
                    query,
                    (record[1], record[0])  # credit_id, deal_id
                )

        connection.commit()
        logger.info(f"Сохранено {len(new_records)} новых записей в БД")
    except Exception as e:
        logger.error(f"Ошибка при сохранении записей: {e}")
        connection.rollback()
    finally:
        if cursor: cursor.close()


def get_data_from_db(DB_CONFIG, sql_file):
    """Получение данных из базы данных"""
    connection = None
    cursor = None
    try:
        # Подключение к БД
        connection = psycopg2.connect(**DB_CONFIG)
        cursor = connection.cursor()

        # Ваш запрос
        query = open(sql_file, 'r', encoding='utf-8').read()
        logger.info(f"Выполняю sql файл: {sql_file}")

        cursor.execute(query)
        data = cursor.fetchall()
        columns = [desc[0] for desc in cursor.description]

        logger.info(f"Получено {len(data)} записей из БД")
        return data, columns, connection

    except (Exception, psycopg2.Error) as error:
        logger.error(f"Ошибка при подключении к PostgreSQL: {error}")
        return None, None, None
    finally:
        if cursor:
            cursor.close()


def get_new_records_only(data, existing_credit_ids):
    """Получение только новых записей по ID кредита"""
    new_records = []

    for record in data:
        if len(record) >= 2:
            credit_id = record[1]  # Второй столбец - ID кредита
            # Если ID кредита нет в существующих - это новая запись
            if credit_id not in existing_credit_ids:
                new_records.append(record)

    return new_records


def format_data(data, columns):
    """Форматирование данных в читаемый вид"""
    if not data:
        return "📭 Нет новых заявок в статусе 'attention'"

    current_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    message = f"🆕 Новые заявки 'Требуется внимание' на {current_time}\n\n"

    # Заголовки столбцов
    message += " | ".join(columns) + "\n"
    message += "-" * 50 + "\n"

    # Данные
    for row in data:
        formatted_row = []
        for item in row:
            if item is None:
                formatted_row.append("NULL")
            elif isinstance(item, (int, float)):
                formatted_row.append(str(item))
            else:
                formatted_row.append(str(item))

        message += " | ".join(formatted_row) + "\n"

    message += f"\n📈 Новых записей: {len(data)}"

    return message


def send_telegram_message(BOT_TOKEN, CHAT_ID, DB_CONFIG, sql_file, sql_insert, sql_select):
    """Основная функция для отправки сообщения"""
    connection = None
    try:
        if not BOT_TOKEN or not CHAT_ID:
            logger.error("Не указаны BOT_TOKEN или CHAT_ID в .env файле")
            return False

        # Получаем данные из БД
        data, columns, connection = get_data_from_db(DB_CONFIG, sql_file)

        if data is None:
            error_msg = "❌ Ошибка при получении данных из базы данных"
            logger.error(error_msg)
            return False

        if not data:
            logger.info("Нет данных для обработки")
            return False

        # Получаем существующие ID кредитов
        existing_credit_ids = get_existing_credit_ids(connection, sql_select)
        logger.info(f"Найдено {len(existing_credit_ids)} существующих ID кредитов")

        # Получаем только новые записи
        new_records = get_new_records_only(data, existing_credit_ids)
        logger.info(f"Найдено {len(new_records)} новых записей")

        # Если нет новых записей - выходим
        if not new_records:
            logger.info("Нет новых записей, отправка не требуется")
            return True

        # Сохраняем новые записи
        save_new_records(connection, new_records, sql_insert)

        # Инициализация бота
        bot = telebot.TeleBot(BOT_TOKEN)

        # Форматируем только новые данные
        formatted_message = format_data(new_records, columns)

        # Отправляем сообщение
        if len(formatted_message) > 4096:
            for x in range(0, len(formatted_message), 4096):
                bot.send_message(CHAT_ID, formatted_message[x:x + 4096])
        else:
            bot.send_message(CHAT_ID, formatted_message)

        logger.info(f"Сообщение с {len(new_records)} новыми записями отправлено в чат {CHAT_ID}")
        return True

    except Exception as e:
        logger.error(f"Ошибка при отправке сообщения: {e}")
        return False
    finally:
        if connection:
            connection.close()
            logger.info("PostgreSQL connection is closed")


if __name__ == '__main__':
    main('sql_query.sql', 'sql_insert.sql', 'sql_check.sql')