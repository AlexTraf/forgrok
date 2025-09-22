import sqlite3
from config import rootLogger, write_daily_log

# Инициализация базы данных
conn = sqlite3.connect("database.db", check_same_thread=False)
cursor = conn.cursor()

def init_db():
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS channels (
            link TEXT PRIMARY KEY,
            status TEXT,
            last_processed TEXT
        )
    """)
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS processed_users (
            user_id INTEGER PRIMARY KEY,
            username TEXT,
            last_processed TEXT
        )
    """)
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS blacklist (
            username TEXT,
            company TEXT,
            PRIMARY KEY (username, company)
        )
    """)
    conn.commit()
    rootLogger.info("База данных инициализирована")
    write_daily_log("База данных инициализирована")